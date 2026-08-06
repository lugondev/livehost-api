import asyncio
import time

import pytest
from fastapi.testclient import TestClient

from tests.fake_gateway import build_fake_gateway


class FlakyUpstream:
    """Drops once, then behaves."""

    def __init__(self):
        self.connects = 0
        self.session_id = None
        self._dropped = False

    async def connect(self):
        self.connects += 1

    async def events(self):
        if not self._dropped:
            self._dropped = True
            yield {"event": "session_started", "session_id": "sess-1"}
            raise ConnectionError("upstream dropped")
        yield {"event": "session_started", "session_id": "sess-1"}
        await asyncio.sleep(0.05)

    async def close(self):
        pass


async def test_the_upstream_is_redialled_after_a_drop():
    """Exercises `_redial` directly -- the one reconnect loop `_down()` also
    calls in production -- rather than a second, standalone copy of the
    same shape. Two such copies existed at one point and had already
    diverged (one dialled on every attempt, the other gated the first), and
    this test was asserting against the one that never shipped."""
    from livehost.api.ws import _redial

    upstream = FlakyUpstream()
    events = []

    async def run_once():
        async for message in upstream.events():
            events.append(message)

    async def run():
        await _redial(upstream, run_once, max_attempts=2, already_connected=False, label="upstream")

    await asyncio.wait_for(run(), timeout=5)
    assert upstream.connects == 2


async def test_the_session_id_is_carried_across_the_reconnect():
    """History must stay continuous: the gateway resumes the same stored
    session when ?session_id= names one it already owns."""
    from livehost.api.ws import resume_params

    params = resume_params({"profile": "host"}, session_id="sess-1")
    assert params["session_id"] == "sess-1"
    assert params["profile"] == "host"


# --- Fixtures for the full-stack reconnect test below. Duplicated from
# tests/test_ws_social.py rather than factored into a conftest.py, matching
# this repo's existing convention (test_ws_social.py has no shared conftest
# either) of keeping each test module's fixtures local and self-contained.


@pytest.fixture
def gateway(monkeypatch):
    """Point the plugin's upstream at an in-process fake.

    Served on a real loopback port because `websockets.connect` dials a real
    socket; a TestClient-only fake cannot be reached by a real ws client.
    """
    import threading

    import uvicorn

    app, log = build_fake_gateway()
    config = uvicorn.Config(app, host="127.0.0.1", port=8198, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline:
            server.should_exit = True
            thread.join(timeout=5)
            pytest.fail("fake gateway did not start within 10s")
        time.sleep(0.01)

    monkeypatch.setattr("livehost.settings.settings.gateway_url", "http://127.0.0.1:8198")
    yield log
    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture
def authed(monkeypatch):
    async def _introspect(ticket, client=None):
        return "user-1" if ticket == "good" else None

    monkeypatch.setattr("livehost.api.ws.introspect", _introspect)


def test_the_ingestor_is_never_torn_down_by_an_upstream_drop(gateway, authed, monkeypatch):
    """A TikTok connection costs backoff and time to rebuild; an upstream
    hiccup must not spend it.

    This drives the real WS handler end to end (not a bare call to
    `_redial`, which never sees an ingestor at all and so could not fail
    this way) so the ingestor is genuinely reachable from the code under
    test: `TikTokLiveIngestor.stop` is monkeypatched at the class
    level, meaning it is the *live* session's own ingestor -- sitting right
    alongside the reconnect loop in `livehost_stream`'s closure -- and this
    would catch a reconnect path that mistakenly called it. `Upstream.events`
    is wrapped to drop the upstream connection exactly once, forcing the
    handler's real reconnect loop to run; a second `session_started` on the
    browser side proves the reconnect actually happened rather than the test
    passing because nothing dropped at all.
    """
    from livehost.app import app
    from livehost.ingest.tiktok import TikTokLiveIngestor
    from livehost.upstream import Upstream

    stopped = []
    real_stop = TikTokLiveIngestor.stop

    async def _tracking_stop(self):
        stopped.append(True)
        await real_stop(self)

    monkeypatch.setattr(TikTokLiveIngestor, "stop", _tracking_stop)

    real_events = Upstream.events
    dropped = {"done": False}

    async def _events_that_drop_once(self):
        if not dropped["done"]:
            dropped["done"] = True
            async for message in real_events(self):
                yield message
                raise ConnectionError("simulated upstream drop")
            return
        async for message in real_events(self):
            yield message

    monkeypatch.setattr(Upstream, "events", _events_that_drop_once)

    with TestClient(app) as client:
        with client.websocket_connect("/v1/livehost/stream?ticket=good") as ws:
            first = ws.receive_json()
            assert first["event"] == "session_started"

            # Proves the reconnect loop actually redialled the gateway,
            # rather than this test passing vacuously because the drop
            # never happened.
            second = ws.receive_json()
            assert second["event"] == "session_started"

            # Checked here, with the WS session still open: teardown at
            # normal disconnect legitimately calls ingestor.stop(), so
            # asserting after the `with` blocks exit would pass even if the
            # reconnect path itself never touched the ingestor.
            assert stopped == []


def test_a_mid_turn_drop_does_not_leave_the_cohost_permanently_silent(gateway, authed, monkeypatch):
    """A drop between speech_start and turn_done -- the streamer mid
    utterance, a completely ordinary moment -- must not leave voice_active
    stuck True forever.

    The resumed gateway session has a fresh endpointer and will never emit
    a turn_done for an utterance it never saw, so without `_down()`
    resetting `voice_active`/`social_turn_in_flight` on every redial,
    `poll_social_turn` would be starved permanently and the co-host would
    go silent for the rest of the session -- the same permanently-silent
    failure Task 8 built a guard for, arriving through the reconnect path
    instead. `Upstream.events` is wrapped to drop the connection exactly
    once, right after the real fake-gateway `speech_start` (not merely
    after `session_started`, which would never enter the mid-turn state at
    all), so this exercises the actual failure window. Confirmed to bite:
    removing the flag reset in `_down()`'s `_run_once` makes this test fail
    (verified by hand; see task-10-report.md).
    """
    from livehost.app import app
    from livehost.registry import livehost_registry
    from livehost.schemas import SocialEvent
    from livehost.upstream import Upstream

    # Zero, not just small: matches test_ws_social.py's own arbitration
    # test -- the fake's round trip can complete well under a real timer's
    # resolution, so a real interval could miss the window entirely.
    monkeypatch.setattr("livehost.api.ws._SOCIAL_POLL_SECONDS", 0.0)

    real_events = Upstream.events
    dropped = {"done": False}

    async def _events_that_drop_after_speech_start(self):
        async for message in real_events(self):
            yield message
            if (
                not dropped["done"]
                and isinstance(message, dict)
                and message.get("event") == "speech_start"
            ):
                dropped["done"] = True
                raise ConnectionError("simulated mid-turn drop")

    monkeypatch.setattr(Upstream, "events", _events_that_drop_after_speech_start)

    session_id = "midturn-1"
    with TestClient(app) as client:
        with client.websocket_connect(
            f"/v1/livehost/stream?ticket=good&session_id={session_id}"
        ) as ws:
            started = ws.receive_json()
            assert started["event"] == "session_started"

            ws.send_bytes(b"MIC")
            speech_start = ws.receive_json()
            assert speech_start["event"] == "speech_start"

            # The drop is injected right after the speech_start above; the
            # handler must redial -- a second session_started proves it
            # actually did, rather than this test passing because nothing
            # dropped at all.
            resumed = ws.receive_json()
            assert resumed["event"] == "session_started"

            # If voice_active were still stuck True from the aborted turn,
            # this queued turn would never fire.
            session = livehost_registry.get(session_id)
            session.scheduler.enqueue(
                SocialEvent(
                    id="e1",
                    kind="comment",
                    user_id="u1",
                    user_name="ann",
                    text="hi",
                    timestamp=0.0,
                )
            )
            deadline = time.monotonic() + 5
            while session.scheduler.pending_count() != 0 and time.monotonic() < deadline:
                time.sleep(0.005)

    control_texts = [c for c in gateway["control"] if c.get("type") == "text"]
    assert len(control_texts) == 1
    assert "ann" in control_texts[0]["text"]
