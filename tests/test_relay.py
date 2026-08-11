from livehost.relay import Relay
from livehost.scheduler import EventScheduler
from livehost.schemas import SocialEvent


def _event(**kwargs) -> SocialEvent:
    """`SocialEvent` requires id/user_id/timestamp; the brief's tests only
    care about kind/user_name/text, so fill the rest with the same fixture
    convention tests/test_scheduler.py already uses."""
    defaults = dict(id="e1", kind="comment", user_id="u1", user_name="user", timestamp=1.0)
    defaults.update(kwargs)
    return SocialEvent(**defaults)


class FakeUpstream:
    def __init__(self, events=()):
        self.sent_text = []
        self.sent_audio = []
        self.sent_raw = []
        self.aborted = 0
        self._events = list(events)

    async def send_text(self, text):
        self.sent_text.append(text)

    async def send_text_raw(self, raw):
        self.sent_raw.append(raw)

    async def send_audio(self, data):
        self.sent_audio.append(data)

    async def abort(self):
        self.aborted += 1

    async def events(self):
        for e in self._events:
            yield e


def _relay(upstream, scheduler=None):
    return Relay(upstream=upstream, scheduler=scheduler or EventScheduler())


async def test_speech_start_marks_the_streamer_as_talking():
    upstream = FakeUpstream([{"event": "speech_start"}])
    relay = _relay(upstream)
    sent = []
    await relay.pump_down(sent.append, lambda b: None)
    assert relay.voice_active is True


async def test_turn_done_clears_it():
    upstream = FakeUpstream([{"event": "speech_start"}, {"event": "turn_done", "turn": 1}])
    relay = _relay(upstream)
    await relay.pump_down(lambda e: None, lambda b: None)
    assert relay.voice_active is False


async def test_aborted_clears_it_too():
    """A browser-initiated abort must clear the flags the same way turn_done
    does. If it does not, voice_active stays True forever, poll_social never
    fires again, and the co-host goes silent for the rest of the session --
    with nothing raised and nothing logged."""
    upstream = FakeUpstream([{"event": "speech_start"}, {"event": "aborted", "reason": "user"}])
    relay = _relay(upstream)
    await relay.pump_down(lambda e: None, lambda b: None)
    assert relay.voice_active is False
    assert relay.social_turn_in_flight is False


async def test_events_and_audio_are_relayed_verbatim():
    """The browser protocol is unchanged by the port: the gateway's
    session_started is a superset of what livehost used to send, so it passes
    straight through."""
    upstream = FakeUpstream(
        [
            {"event": "session_started", "session_id": "s1", "stt_ready": True},
            b"AUDIO",
            {"event": "turn_done", "turn": 1},
        ]
    )
    relay = _relay(upstream)
    events, audio = [], []
    await relay.pump_down(events.append, audio.append)
    assert events[0]["event"] == "session_started"
    assert events[0]["session_id"] == "s1"
    assert audio == [b"AUDIO"]


async def test_browser_audio_goes_upstream_untouched():
    upstream = FakeUpstream()
    relay = _relay(upstream)
    await relay.pump_up({"bytes": b"MIC"})
    assert upstream.sent_audio == [b"MIC"]


async def test_a_social_turn_is_injected_as_text_when_the_streamer_is_quiet():
    scheduler = EventScheduler()
    scheduler.enqueue(_event(kind="comment", user_name="ann", text="hi"))
    upstream = FakeUpstream()
    relay = _relay(upstream, scheduler)
    relay.voice_active = False
    await relay.poll_social()
    assert len(upstream.sent_text) == 1
    assert "[TikTok @ann]: hi" in upstream.sent_text[0]


async def test_no_social_turn_while_the_streamer_is_talking():
    scheduler = EventScheduler()
    scheduler.enqueue(_event(kind="comment", user_name="ann", text="hi"))
    upstream = FakeUpstream()
    relay = _relay(upstream, scheduler)
    relay.voice_active = True
    await relay.poll_social()
    assert upstream.sent_text == []


async def test_a_second_social_turn_does_not_fire_while_the_first_is_in_flight():
    """Found live: on a busy room, comments keep queuing continuously, so
    has_pending() never goes false between polls. Without this guard, the
    poll loop fired a fresh social turn on every tick even though the
    previous one's turn_done had not arrived yet, flooding the gateway with
    overlapping turns and spamming the LLM."""
    scheduler = EventScheduler()
    scheduler.enqueue(_event(kind="comment", user_name="ann", text="first"))
    upstream = FakeUpstream()
    relay = _relay(upstream, scheduler)
    relay.voice_active = False

    await relay.poll_social()
    assert len(upstream.sent_text) == 1
    assert relay.social_turn_in_flight is True

    # A second comment arrives while the first turn is still in flight.
    scheduler.enqueue(_event(kind="comment", user_name="bob", text="second"))
    await relay.poll_social()
    assert len(upstream.sent_text) == 1, "must not fire a second turn before turn_done"

    # Once the first turn completes, the queued second comment fires normally.
    relay.social_turn_in_flight = False
    await relay.poll_social()
    assert len(upstream.sent_text) == 2
    assert "[TikTok @bob]: second" in upstream.sent_text[1]


async def test_barge_in_aborts_the_social_turn():
    """The streamer starting to talk mid-social-turn must cut the co-host off,
    which is what {"type":"abort"} is for."""
    upstream = FakeUpstream([{"event": "speech_start"}])
    relay = _relay(upstream)
    relay.social_turn_in_flight = True
    await relay.pump_down(lambda e: None, lambda b: None)
    assert upstream.aborted == 1
    assert relay.social_turn_in_flight is False


async def test_poll_idle_fires_a_spontaneous_topic_after_the_threshold():
    from livehost.relay import IDLE_TOPIC_PROMPT

    upstream = FakeUpstream()
    relay = _relay(upstream)
    relay.last_activity -= 10  # 10s of silence

    await relay.poll_idle(idle_seconds=5)

    assert upstream.sent_text == [IDLE_TOPIC_PROMPT]
    assert relay.social_turn_in_flight is True


async def test_poll_idle_does_not_fire_before_the_threshold():
    upstream = FakeUpstream()
    relay = _relay(upstream)
    relay.last_activity -= 2  # only 2s of silence

    await relay.poll_idle(idle_seconds=5)

    assert upstream.sent_text == []


async def test_poll_idle_does_not_fire_while_the_streamer_is_talking():
    upstream = FakeUpstream()
    relay = _relay(upstream)
    relay.last_activity -= 10
    relay.voice_active = True

    await relay.poll_idle(idle_seconds=5)

    assert upstream.sent_text == []


async def test_poll_idle_does_not_fire_while_a_social_turn_is_already_in_flight():
    """Same overlap-prevention poll_social's own guard exists for: both
    ultimately call upstream.send_text, and firing a second one before the
    first's turn_done would overlap turns on the gateway."""
    upstream = FakeUpstream()
    relay = _relay(upstream)
    relay.last_activity -= 10
    relay.social_turn_in_flight = True

    await relay.poll_idle(idle_seconds=5)

    assert upstream.sent_text == []


async def test_a_fired_social_turn_resets_the_idle_clock():
    from livehost.relay import IDLE_TOPIC_PROMPT

    scheduler = EventScheduler()
    scheduler.enqueue(_event(kind="comment", user_name="ann", text="hi"))
    upstream = FakeUpstream()
    relay = _relay(upstream, scheduler)
    relay.last_activity -= 100

    await relay.poll_social()
    relay.social_turn_in_flight = False  # clear the guard poll_idle also checks

    # If last_activity were still 100s stale, this would fire the idle topic
    # too -- it must not, proving poll_social reset the clock to "now"
    # rather than leaving it at its pre-turn value.
    await relay.poll_idle(idle_seconds=5)
    assert IDLE_TOPIC_PROMPT not in upstream.sent_text


async def test_note_activity_resets_the_idle_clock():
    """ws.py's social-event drain loop calls this directly -- a comment that
    arrives but hasn't fired a turn yet (still batching, or the streamer is
    mid-turn) is still evidence the room is live."""
    upstream = FakeUpstream()
    relay = _relay(upstream)
    relay.last_activity -= 100

    relay.note_activity()

    await relay.poll_idle(idle_seconds=5)
    assert upstream.sent_text == []
