from livehost.schemas import SocialEvent
from livehost.scheduler import EventScheduler, score_event


def _event(kind="comment", **kwargs) -> SocialEvent:
    defaults = dict(id="e", user_id="u", user_name="user", timestamp=1.0)
    defaults.update(kwargs)
    return SocialEvent(kind=kind, **defaults)


def test_gift_scores_higher_than_plain_comment():
    gift = _event(kind="gift", gift_name="Rose", gift_value=50)
    comment = _event(kind="comment", text="hello")
    assert score_event(gift, []) > score_event(comment, [])


def test_mention_keyword_boosts_comment_above_gift():
    mention = _event(kind="comment", text="hey CoHostBot answer me")
    gift = _event(kind="gift", gift_name="Rose", gift_value=5000)
    assert score_event(mention, ["CoHostBot"]) > score_event(gift, ["CoHostBot"])


def test_like_scores_lowest_by_default():
    like = _event(kind="like", like_count=20)
    comment = _event(kind="comment", text="hi")
    assert score_event(like, []) < score_event(comment, [])


def test_small_backlog_returns_single_event_turn():
    scheduler = EventScheduler(individual_threshold=3, batch_top_k=3)
    scheduler.enqueue(_event(id="e1", text="first"))
    scheduler.enqueue(_event(id="e2", text="second"))

    turn = scheduler.next_turn()

    assert turn is not None
    assert len(turn.events) == 1
    assert turn.overflow_count == 0
    # The remaining event is still pending for the next call.
    assert scheduler.pending_count() == 1


def test_large_backlog_batches_top_k_and_reports_overflow():
    scheduler = EventScheduler(individual_threshold=2, batch_top_k=2)
    for i in range(5):
        scheduler.enqueue(_event(id=f"e{i}", text=f"msg {i}"))

    turn = scheduler.next_turn()

    assert turn is not None
    assert len(turn.events) == 2
    assert turn.overflow_count == 3
    assert scheduler.pending_count() == 0  # batch clears the whole backlog


def test_next_turn_on_empty_queue_returns_none():
    scheduler = EventScheduler()
    assert scheduler.next_turn() is None
    assert scheduler.has_pending() is False


def test_queue_cap_drops_lowest_priority_before_gifts():
    scheduler = EventScheduler(max_queue_size=3, individual_threshold=0, batch_top_k=0)
    scheduler.enqueue(_event(id="gift1", kind="gift", gift_name="Rose", gift_value=10))
    scheduler.enqueue(_event(id="like1", kind="like", like_count=1))
    scheduler.enqueue(_event(id="like2", kind="like", like_count=1))
    # Exceeds cap of 3 -> must drop a "like", never the gift.
    scheduler.enqueue(_event(id="like3", kind="like", like_count=1))

    remaining_ids = {s.event.id for s in scheduler._queue}  # noqa: SLF001 - white-box test
    assert "gift1" in remaining_ids
    assert len(remaining_ids) == 3


def test_queue_never_drops_gift_even_when_full_of_gifts():
    """Verify that gifts are never dropped, even when queue is entirely full of protected entries."""
    scheduler = EventScheduler(max_queue_size=3, individual_threshold=0, batch_top_k=0)
    # Fill the queue entirely with gifts (all are protected).
    scheduler.enqueue(_event(id="gift1", kind="gift", gift_name="Rose", gift_value=10))
    scheduler.enqueue(_event(id="gift2", kind="gift", gift_name="Crown", gift_value=20))
    scheduler.enqueue(_event(id="gift3", kind="gift", gift_name="Heart", gift_value=15))
    # Exceeds cap of 3 -> queue is full of only gifts (all protected),
    # so no eviction should happen. Queue should temporarily exceed max_queue_size.
    scheduler.enqueue(_event(id="gift4", kind="gift", gift_name="Rose", gift_value=5))

    remaining_ids = {s.event.id for s in scheduler._queue}  # noqa: SLF001 - white-box test
    assert remaining_ids == {"gift1", "gift2", "gift3", "gift4"}
    # Queue exceeds cap because gifts were never dropped.
    assert len(scheduler._queue) == 4
