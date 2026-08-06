"""Turn arbitration between the streamer's voice (always wins) and pending
social events. Purely decision + text-formatting logic — actually running a
turn (LLM + TTS + sending WS events) is the WS route's job (it already knows
how to do that for voice turns; social turns reuse the same machinery), so
this stays a small, independently testable arbiter.
"""

from __future__ import annotations

from livehost.scheduler import EventScheduler, SocialTurn


def format_social_turn(turn: SocialTurn) -> str:
    lines: list[str] = []
    for event in turn.events:
        if event.kind == "comment":
            lines.append(f"[TikTok @{event.user_name}]: {event.text}")
        elif event.kind == "gift":
            lines.append(f"[TikTok @{event.user_name}] sent a gift: {event.gift_name} (value {event.gift_value})")
        elif event.kind == "follow":
            lines.append(f"[TikTok @{event.user_name}] just followed the stream")
        elif event.kind == "share":
            lines.append(f"[TikTok @{event.user_name}] shared the stream")
        elif event.kind == "like":
            lines.append(f"[TikTok] {event.like_count or 0} new likes")
    if turn.overflow_count:
        lines.append(f"(and {turn.overflow_count} more from other viewers)")
    return "\n".join(lines)


class LiveHostOrchestrator:
    def __init__(self, scheduler: EventScheduler) -> None:
        self.scheduler = scheduler

    def poll_social_turn(self, voice_active: bool) -> tuple[SocialTurn, str] | None:
        if voice_active or not self.scheduler.has_pending():
            return None
        turn = self.scheduler.next_turn()
        if turn is None:
            return None
        return turn, format_social_turn(turn)
