"""Priority queue for social-live events (comments/gifts/likes/follows/shares).

Dequeue decisions (single event vs. batch) are made at consumption time
(`next_turn`), not at enqueue time, since the backlog size the decision should
react to keeps changing between calls. See
docs/superpowers/specs/2026-07-05-livehost-tiktok-cohost-design.md section 3.
"""

from __future__ import annotations

from dataclasses import dataclass

from livehost.schemas import SocialEvent

_KIND_BASE_SCORE = {
    "gift": 500.0,
    "follow": 200.0,
    "share": 100.0,
    "comment": 100.0,
    "like": 0.0,
}
# Large enough that a keyword-mention comment always outranks even a big gift
# (real TikTok gifts top out in the tens of thousands of diamonds per send) —
# mention is priority tier 1, gift is tier 2, per the design doc's ordered list.
_MENTION_BONUS = 100_000.0
# Entries scoring at/above this are never evicted by the queue cap (covers all
# gifts and any mention-boosted comment).
PROTECTED_SCORE_FLOOR = 500.0


def score_event(event: SocialEvent, mention_keywords: list[str]) -> float:
    score = _KIND_BASE_SCORE.get(event.kind, 0.0)
    if event.kind == "gift" and event.gift_value:
        score += float(event.gift_value)
    if event.text and mention_keywords:
        lowered = event.text.lower()
        if any(kw.lower() in lowered for kw in mention_keywords if kw):
            score += _MENTION_BONUS
    return score


@dataclass
class _ScoredEvent:
    event: SocialEvent
    score: float


@dataclass
class SocialTurn:
    events: list[SocialEvent]
    overflow_count: int = 0


class EventScheduler:
    """Not thread-safe beyond a single asyncio event loop; drive it from one
    orchestrator/session only."""

    def __init__(
        self,
        mention_keywords: list[str] | None = None,
        individual_threshold: int = 3,
        batch_top_k: int = 3,
        max_queue_size: int = 200,
    ) -> None:
        self.mention_keywords = mention_keywords or []
        self.individual_threshold = individual_threshold
        self.batch_top_k = batch_top_k
        self.max_queue_size = max_queue_size
        self._queue: list[_ScoredEvent] = []

    def enqueue(self, event: SocialEvent) -> None:
        scored = _ScoredEvent(event=event, score=score_event(event, self.mention_keywords))
        self._queue.append(scored)
        self._queue.sort(key=lambda s: s.score, reverse=True)
        if len(self._queue) > self.max_queue_size:
            self._evict_one()

    def _evict_one(self) -> None:
        for i in range(len(self._queue) - 1, -1, -1):
            candidate = self._queue[i]
            if candidate.event.kind != "gift" and candidate.score < PROTECTED_SCORE_FLOOR:
                del self._queue[i]
                return
        # Every pending entry is protected (gift/mention) — do nothing rather than
        # drop one. The queue temporarily exceeds max_queue_size in this rare case
        # (queue saturated entirely with gifts/mentions); "never drop a gift" wins
        # over the size cap.

    def has_pending(self) -> bool:
        return bool(self._queue)

    def pending_count(self) -> int:
        return len(self._queue)

    def next_turn(self) -> SocialTurn | None:
        if not self._queue:
            return None
        if len(self._queue) <= self.individual_threshold:
            top = self._queue.pop(0)
            return SocialTurn(events=[top.event], overflow_count=0)

        top_k = self._queue[: self.batch_top_k]
        overflow = len(self._queue) - len(top_k)
        events = [s.event for s in top_k]
        self._queue.clear()
        return SocialTurn(events=events, overflow_count=overflow)
