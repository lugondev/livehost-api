import time

import pytest
from fastapi.testclient import TestClient

from livehost.auth import IntrospectResult
from tests.fake_gateway import build_fake_gateway


@pytest.fixture
def gateway(monkeypatch):
    """Point the plugin's upstream at an in-process fake.

    Served on a real loopback port because `websockets.connect` dials a real
    socket; a TestClient-only fake cannot be reached by a real ws client.
    """
    import threading

    import uvicorn

    app, log = build_fake_gateway()
    config = uvicorn.Config(app, host="127.0.0.1", port=8199, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Bounded wait, not a spin: a server that fails to bind never sets
    # `started`, and a bare `while not server.started: pass` would burn a core
    # until the suite's 120s timeout killed the whole run.
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline:
            server.should_exit = True
            thread.join(timeout=5)
            pytest.fail("fake gateway did not start within 10s")
        time.sleep(0.01)

    monkeypatch.setattr("livehost.settings.settings.gateway_url", "http://127.0.0.1:8199")
    yield log
    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture
def authed(monkeypatch):
    async def _introspect(ticket, client=None):
        if ticket != "good":
            return IntrospectResult(user_id=None, session_token=None)
        return IntrospectResult(user_id="user-1", session_token="sess-tok-user-1")

    monkeypatch.setattr("livehost.api.ws.introspect", _introspect)


def test_a_bad_ticket_is_refused(gateway, authed):
    from livehost.app import app

    with TestClient(app) as client:
        with pytest.raises(Exception):
            with client.websocket_connect("/v1/livehost/stream?ticket=bad"):
                pass


def test_session_started_is_relayed_from_the_gateway(gateway, authed):
    """The plugin does not synthesize this event -- the gateway's version is a
    superset of what livehost used to send, so it passes straight through."""
    from livehost.app import app

    with TestClient(app) as client:
        with client.websocket_connect("/v1/livehost/stream?ticket=good") as ws:
            event = ws.receive_json()
            assert event["event"] == "session_started"
            assert event["stt_engine"] == "fake-stt"
            assert event["stt_ready"] is True


def test_browser_audio_reaches_the_gateway(gateway, authed):
    from livehost.app import app

    with TestClient(app) as client:
        with client.websocket_connect("/v1/livehost/stream?ticket=good") as ws:
            ws.receive_json()
            ws.send_bytes(b"MIC")
            # Poll to a deadline rather than sleeping a guessed interval: a
            # fixed sleep is either flaky on a loaded machine or slow on an
            # idle one.
            deadline = time.monotonic() + 5
            while b"MIC" not in gateway["audio"] and time.monotonic() < deadline:
                time.sleep(0.01)
    assert b"MIC" in gateway["audio"]


def test_skip_social_event_drops_like_and_share_when_enabled():
    from livehost.api.ws import _skip_social_event
    from livehost.schemas import SocialEvent

    def _event(kind):
        return SocialEvent(id="e1", kind=kind, user_id="u1", user_name="ann", timestamp=1.0)

    for kind in ("like", "share"):
        assert _skip_social_event(_event(kind), skip_like_share=True) is True


def test_skip_social_event_leaves_comments_and_gifts_alone():
    from livehost.api.ws import _skip_social_event
    from livehost.schemas import SocialEvent

    def _event(kind):
        return SocialEvent(id="e1", kind=kind, user_id="u1", user_name="ann", timestamp=1.0)

    for kind in ("comment", "gift", "follow"):
        assert _skip_social_event(_event(kind), skip_like_share=True) is False


def test_skip_social_event_is_a_noop_when_disabled():
    from livehost.api.ws import _skip_social_event
    from livehost.schemas import SocialEvent

    like = SocialEvent(id="e1", kind="like", user_id="u1", user_name="ann", timestamp=1.0)
    assert _skip_social_event(like, skip_like_share=False) is False


def test_a_persona_override_is_forwarded_to_the_gateway(gateway, authed):
    """?system_prompt= (the livehost UI's own persona field) must reach the
    gateway's WS query string unchanged, since that is what
    SessionRuntimeConfig.persona_override reads to replace profile.system_prompt
    for this session."""
    from livehost.app import app

    persona = "You are Lan, the channel owner."
    with TestClient(app) as client:
        with client.websocket_connect(
            f"/v1/livehost/stream?ticket=good&system_prompt={persona.replace(' ', '+')}"
        ) as ws:
            ws.receive_json()

    assert gateway["query"]["system_prompt"] == persona


def test_no_persona_override_means_no_system_prompt_param_at_all(gateway, authed):
    """A blank persona field must not send `system_prompt=` at all -- an empty
    string would still be forwarded as a real (empty) query value, and the
    gateway only falls back to the profile/server default when the param is
    absent, not merely empty."""
    from livehost.app import app

    with TestClient(app) as client:
        with client.websocket_connect("/v1/livehost/stream?ticket=good") as ws:
            ws.receive_json()

    assert "system_prompt" not in gateway["query"]


def test_skip_like_share_end_to_end_never_reaches_the_scheduler(gateway, authed):
    """Drives the real _drain_social loop (not just the _skip_social_event
    unit above) by injecting straight onto the live ingestor's own queue --
    the same object _drain_social reads from -- so this exercises the actual
    wiring: ?skip_like_share=1 on the URL reaching the closure's
    skip_like_share variable, not just the predicate function in isolation.
    """
    from livehost.app import app
    from livehost.registry import livehost_registry
    from livehost.schemas import SocialEvent

    session_id = "skip-like-share-1"
    with TestClient(app) as client:
        with client.websocket_connect(
            f"/v1/livehost/stream?ticket=good&session_id={session_id}&skip_like_share=1"
        ) as ws:
            ws.receive_json()

            session = livehost_registry.get(session_id)
            session.ingestor.queue.put_nowait(
                SocialEvent(id="e1", kind="like", user_id="u1", user_name="ann", timestamp=1.0)
            )
            session.ingestor.queue.put_nowait(
                SocialEvent(
                    id="e2",
                    kind="comment",
                    user_id="u2",
                    user_name="bob",
                    text="hi",
                    timestamp=1.0,
                )
            )

            deadline = time.monotonic() + 5
            while session.scheduler.pending_count() == 0 and time.monotonic() < deadline:
                time.sleep(0.01)

    # Only the comment made it through -- the like never touched the
    # scheduler at all, not merely "arrived but was never spoken."
    assert session.scheduler.pending_count() == 1
    turn = session.scheduler.next_turn()
    assert turn.events[0].kind == "comment"


def test_a_different_identity_cannot_hijack_an_existing_session_id(gateway, monkeypatch):
    """H5, ported: livehost_registry.register() overwrites unconditionally on
    a session_id collision, and session_id is caller-supplied. Without a
    guard, a second connection presenting someone else's ?session_id= would
    silently replace the live entry -- the original owner's control calls
    would start resolving to the intruder's session, and when the intruder
    disconnects, unregister() would orphan the original owner's ingestor.

    This would fail if the guard in livehost_stream (the `existing is not
    None and existing.user_id != (user_id or None)` check) were removed:
    the second connect would then succeed, `receive_json()` on it would get
    `session_started` instead of the expected error, and the registry
    identity check below would find the entry had been overwritten to
    user-2.
    """

    async def _introspect(ticket, client=None):
        user_id = {"good": "user-1", "intruder": "user-2"}.get(ticket)
        if user_id is None:
            return IntrospectResult(user_id=None, session_token=None)
        return IntrospectResult(user_id=user_id, session_token=f"sess-tok-{user_id}")

    monkeypatch.setattr("livehost.api.ws.introspect", _introspect)

    from livehost.app import app
    from livehost.registry import livehost_registry

    session_id = "hijack-1"
    with TestClient(app) as client:
        with client.websocket_connect(
            f"/v1/livehost/stream?ticket=good&session_id={session_id}"
        ) as ws1:
            started = ws1.receive_json()
            assert started["event"] == "session_started"

            original = livehost_registry.get(session_id)
            assert original is not None
            assert original.user_id == "user-1"

            # A different, validly-authenticated identity tries to take over
            # the same session_id.
            with client.websocket_connect(
                f"/v1/livehost/stream?ticket=intruder&session_id={session_id}"
            ) as ws2:
                refusal = ws2.receive_json()
                assert refusal == {
                    "event": "error",
                    "message": f"Session '{session_id}' not found",
                }
                with pytest.raises(Exception):
                    ws2.receive_json()

            # The original owner's registry entry must be untouched -- same
            # object, same owner, not silently replaced by the intruder's.
            still_there = livehost_registry.get(session_id)
            assert still_there is original
            assert still_there.user_id == "user-1"


def test_social_turn_does_not_fire_while_the_streamer_is_mid_turn(gateway, authed, monkeypatch):
    """Arbitration under barge-in, driven end to end through the real WS
    route (not the FakeUpstream unit tests in test_relay.py -- this is about
    whether the WS handler wires the poll task to the *same* Relay instance
    pump_down updates, since that sharing is what test_relay.py cannot see).

    Naive wall-clock correlation doesn't work here: the fake's audio reply
    (speech_start..turn_done) completes in low milliseconds with no
    artificial delay, so comparing a background sampler thread's timestamps
    against the main thread's blocking `ws.receive_json()` calls is really
    comparing two independently OS-scheduled clocks with no ordering
    guarantee between them, even when the server-side sequence is correct --
    an earlier version of this test did exactly that and was flaky. Worse,
    the *enqueue* itself, if triggered from `ws.receive_json()` returning on
    the test's own thread, can lag the server by the time it takes the OS to
    reschedule that thread -- easily enough for the whole 4-event burst to
    have already completed server-side, which would make even a broken
    arbitration look correct (nothing left pending to race against).

    Both problems have the same fix: do the observation and the injection on
    the plugin's own event loop, not from a separate thread.
      - `Upstream.events` is wrapped so the pending social event is enqueued
        in the same, un-awaited step that produces the speech_start message
        Relay.pump_down is about to consume -- there is no yield point
        between that and pump_down setting voice_active=True, so the event
        is guaranteed pending before, never after, that flip (no legitimate
        "not active yet" window to race against).
      - `Relay.poll_social` is wrapped (not replaced -- the real
        implementation still runs and still owns arbitration) to record
        `voice_active` and the pending-count transition on every tick. There
        is no await between our read of `voice_active` and poll_social's own
        read of the same attribute, so the two observe the identical value.
    Both rely on the same invariant Task 8's review flagged (no await
    between reading voice_active and mutating turn state) -- reused here for
    race-free observation instead of race-free mutation. If poll_social
    ignored voice_active (arbitration removed) or were hard-wired to always
    fire, the very first tick after the event goes pending -- which must
    land while voice_active is still True, per the above -- shows up as a
    (True, 1, 0) observation, and the assertion on `premature` catches it.
    """
    from livehost.app import app
    from livehost.registry import livehost_registry
    from livehost.relay import Relay
    from livehost.schemas import SocialEvent
    from livehost.upstream import Upstream

    session_id = "arb-1"

    observations: list[tuple[bool, int, int]] = []  # (voice_active, pending_before, pending_after)
    real_poll_social = Relay.poll_social

    async def _observed_poll_social(self, *args, **kwargs):
        voice_active = self.voice_active
        pending_before = self.scheduler.pending_count()
        await real_poll_social(self, *args, **kwargs)
        observations.append((voice_active, pending_before, self.scheduler.pending_count()))

    monkeypatch.setattr(Relay, "poll_social", _observed_poll_social)
    # Zero, not just "small": the fake's whole 4-event round trip can complete
    # in under a millisecond on a fast machine, faster than most platforms'
    # real timer resolution -- `asyncio.sleep(0)` is a plain cooperative
    # yield with no timer involved, so the poll loop gets a turn on every
    # single event-loop iteration instead of missing the window between
    # timer ticks.
    monkeypatch.setattr("livehost.api.ws._SOCIAL_POLL_SECONDS", 0.0)

    real_events = Upstream.events
    enqueued = {"done": False}

    async def _events_that_enqueue_on_speech_start(self):
        async for message in real_events(self):
            if (
                not enqueued["done"]
                and isinstance(message, dict)
                and message.get("event") == "speech_start"
            ):
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
                enqueued["done"] = True
            yield message

    monkeypatch.setattr(Upstream, "events", _events_that_enqueue_on_speech_start)

    with TestClient(app) as client:
        with client.websocket_connect(
            f"/v1/livehost/stream?ticket=good&session_id={session_id}"
        ) as ws:
            started = ws.receive_json()
            assert started["event"] == "session_started"

            ws.send_bytes(b"MIC")

            downlink = [ws.receive_json()["event"] for _ in range(4)]
            assert downlink == ["speech_start", "speech_end", "user_transcript", "turn_done"]

            # voice_active is now clear; give the (very fast) poll loop a
            # bounded number of ticks to notice and fire.
            session = livehost_registry.get(session_id)
            deadline = time.monotonic() + 5
            while session.scheduler.pending_count() != 0 and time.monotonic() < deadline:
                time.sleep(0.005)

    assert enqueued["done"], "never observed speech_start, so never enqueued"
    fired = [o for o in observations if o[1] == 1 and o[2] == 0]
    assert fired, "the pending social turn never fired at all"
    premature = [o for o in fired if o[0] is True]
    assert not premature, (
        "poll_social consumed the pending social turn while voice_active was "
        f"still True: {premature!r}"
    )

    control_texts = [c for c in gateway["control"] if c.get("type") == "text"]
    assert len(control_texts) == 1
    assert "ann" in control_texts[0]["text"]


def test_returning_commenter_gets_a_viewer_note_in_the_second_turn(
    gateway, authed, monkeypatch, tmp_path
):
    """A second comment from the same TikTok user_id, under the same
    memory_id, carries a note referencing the earlier comment -- the
    co-host 'remembers' having talked to them already this browser."""
    from livehost.app import app
    from livehost.registry import livehost_registry
    from livehost.schemas import SocialEvent

    monkeypatch.setattr("livehost.settings.settings.memory_db_path", str(tmp_path / "memory.db"))
    monkeypatch.setattr("livehost.api.ws._SOCIAL_POLL_SECONDS", 0.0)

    def _texts():
        return [c["text"] for c in gateway["control"] if c.get("type") == "text"]

    session_id = "memory-1"
    with TestClient(app) as client:
        with client.websocket_connect(
            f"/v1/livehost/stream?ticket=good&session_id={session_id}&memory_id=browser-abc"
        ) as ws:
            ws.receive_json()
            session = livehost_registry.get(session_id)

            session.ingestor.queue.put_nowait(
                SocialEvent(
                    id="e1", kind="comment", user_id="u1", user_name="ann",
                    text="gia bao nhieu vay shop", timestamp=1.0,
                )
            )
            deadline = time.monotonic() + 5
            while len(_texts()) < 1 and time.monotonic() < deadline:
                time.sleep(0.01)

            session.ingestor.queue.put_nowait(
                SocialEvent(
                    id="e2", kind="comment", user_id="u1", user_name="ann",
                    text="minh hoi lai nha", timestamp=2.0,
                )
            )
            deadline = time.monotonic() + 5
            while len(_texts()) < 2 and time.monotonic() < deadline:
                time.sleep(0.01)

    texts = _texts()
    assert len(texts) == 2
    assert "đã bình luận 1 lần" in texts[1]
    assert "gia bao nhieu vay shop" in texts[1]
