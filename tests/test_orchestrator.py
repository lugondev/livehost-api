from livehost.orchestrator import LiveHostOrchestrator, format_social_turn
from livehost.scheduler import EventScheduler, SocialTurn
from livehost.schemas import SocialEvent


def _event(kind="comment", **kwargs) -> SocialEvent:
    defaults = dict(id="e", user_id="u1", user_name="Bao", timestamp=1.0)
    defaults.update(kwargs)
    return SocialEvent(kind=kind, **defaults)


def test_format_comment_turn():
    turn = SocialTurn(events=[_event(text="xin chao")])
    text = format_social_turn(turn)
    assert "@Bao" in text
    assert "xin chao" in text


def test_format_gift_turn():
    turn = SocialTurn(events=[_event(kind="gift", gift_name="Rose", gift_value=50)])
    text = format_social_turn(turn)
    assert "Rose" in text
    assert "50" in text


def test_format_batch_turn_mentions_overflow():
    turn = SocialTurn(events=[_event(text="a"), _event(text="b")], overflow_count=12)
    text = format_social_turn(turn)
    assert "a" in text and "b" in text
    assert "12" in text


def test_poll_returns_none_while_voice_active():
    scheduler = EventScheduler()
    scheduler.enqueue(_event(text="hi"))
    orchestrator = LiveHostOrchestrator(scheduler)

    assert orchestrator.poll_social_turn(voice_active=True) is None
    assert scheduler.pending_count() == 1  # nothing was dequeued


def test_poll_returns_none_when_nothing_pending():
    orchestrator = LiveHostOrchestrator(EventScheduler())
    assert orchestrator.poll_social_turn(voice_active=False) is None


def test_poll_dequeues_and_formats_when_voice_idle():
    scheduler = EventScheduler(individual_threshold=5)
    scheduler.enqueue(_event(text="hi"))
    orchestrator = LiveHostOrchestrator(scheduler)

    result = orchestrator.poll_social_turn(voice_active=False)

    assert result is not None
    turn, text = result
    assert len(turn.events) == 1
    assert "hi" in text
    assert scheduler.pending_count() == 0
