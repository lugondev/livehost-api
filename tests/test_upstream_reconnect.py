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
    from livehost.api.ws import relay_with_reconnect

    upstream = FlakyUpstream()
    events = []

    async def run():
        await relay_with_reconnect(upstream, events.append, lambda b: None, max_attempts=2)

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
    `relay_with_reconnect`, which never sees an ingestor at all and so could
    not fail this way) so the ingestor is genuinely reachable from the code
    under test: `TikTokLiveIngestor.stop` is monkeypatched at the class
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
