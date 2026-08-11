"""Two pumps and an arbiter, with no socket on either side so it can be tested.

Arbitration keeps its old meaning: a social turn only fires when the streamer
is not talking. What changed is where that fact comes from. The endpointer used
to live here; it now lives in the gateway, so `voice_active` is derived from
the upstream event stream instead -- speech_start sets it, turn_done clears it.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable

from livehost.orchestrator import LiveHostOrchestrator
from livehost.scheduler import EventScheduler

# Sent as a "text" turn, same mechanism poll_social uses for a comment --
# the LLM sees it as the thing to react to, not a system instruction it
# should acknowledge or explain, hence "don't mention no one commented"
# rather than leaving that implicit.
IDLE_TOPIC_PROMPT = (
    "(Không có bình luận hay sự kiện gì mới trong một lúc. Hãy tự nhiên chuyển "
    "sang một chủ đề, câu chuyện, hoặc câu hỏi thú vị để giữ không khí live sôi "
    "động -- đừng nhắc đến việc không có ai bình luận, cứ nói như một host thật "
    "đang tự dẫn dắt buổi live.)"
)


class Relay:
    def __init__(self, upstream, scheduler: EventScheduler) -> None:
        self.upstream = upstream
        self.scheduler = scheduler
        self.orchestrator = LiveHostOrchestrator(scheduler)
        self.voice_active = False
        self.social_turn_in_flight = False
        # monotonic, not wall clock: only ever compared to itself, and must
        # never jump backward/forward with a system clock adjustment.
        self.last_activity = time.monotonic()
        # When the current batch window started waiting, or None if nothing
        # is accumulating one (queue empty, or the last turn already
        # consumed everything). See note_pending()/poll_social.
        self.pending_since: float | None = None

    def note_pending(self) -> None:
        """Mark the start of a fresh batch window, if one isn't already
        running. Called from ws.py's drain loop on every social event that
        actually made it into the scheduler (post skip_like_share filter),
        so poll_social's batch_wait_seconds counts from the FIRST event of
        the batch, not the most recent one -- otherwise a steady trickle of
        comments could keep pushing the wait out and never fire at all."""
        if self.pending_since is None:
            self.pending_since = time.monotonic()

    def note_activity(self) -> None:
        """Reset the idle clock. Called on anything that means the room is
        NOT quiet: a new social event queuing (ws.py's drain loop), on top
        of the turn-boundary events pump_down/poll_social already reset it
        on -- a comment that arrives but hasn't fired a turn yet (still
        batching, or the streamer is mid-turn) is still evidence the room
        is live, and must not let poll_idle fire underneath it."""
        self.last_activity = time.monotonic()

    async def pump_down(
        self,
        send_json: Callable[[dict], Awaitable[None] | None],
        send_bytes: Callable[[bytes], Awaitable[None] | None],
    ) -> None:
        """Upstream to browser. Everything is relayed verbatim; the only local
        work is reading two events for the arbitration state."""
        async for message in self.upstream.events():
            if isinstance(message, bytes):
                await _maybe_await(send_bytes(message))
                continue
            event = message.get("event")
            if event == "speech_start":
                self.voice_active = True
                self.note_activity()
                # The streamer talking over the co-host wins, always.
                if self.social_turn_in_flight:
                    await self.upstream.abort()
                    self.social_turn_in_flight = False
            elif event in ("turn_done", "aborted"):
                self.voice_active = False
                self.social_turn_in_flight = False
                self.note_activity()
            await _maybe_await(send_json(message))

    async def pump_up(self, message: dict) -> None:
        """Browser to upstream. Audio frames and control messages both pass
        through unchanged -- the plugin adds nothing to the voice path, and
        the browser's own abort/flush/end must reach the gateway verbatim."""
        if message.get("bytes") is not None:
            await self.upstream.send_audio(message["bytes"])
            return
        text = message.get("text")
        if text is not None:
            await self.upstream.send_text_raw(text)

    async def poll_social(self, batch_min_events: int = 1, batch_wait_seconds: float = 0.0) -> None:
        """Fire one social turn if the streamer is quiet, nothing is already
        in flight, something waits, AND the batch window (if any) is ready.

        Found live: on a busy room, comments keep queuing continuously, so
        `scheduler.has_pending()` stays true for the whole stream. Without
        the in-flight guard, the poll loop (every _SOCIAL_POLL_SECONDS)
        popped and sent a fresh turn on every tick regardless of whether the
        previous one had ever received its turn_done -- flooding the
        gateway with overlapping "text" turns, spamming the LLM, and
        leaving the browser UI stuck re-entering "processing" before the
        prior turn's response ever settled.

        `batch_min_events`/`batch_wait_seconds` (both from the browser's own
        UI controls, ws.py) throttle the OTHER complaint: replying to every
        single comment the instant it lands reads as frantic, not like a
        host who lets a few comments pile up before addressing them. The
        gate is an OR, matching "2-3 comment or 5/10/20/30s, whichever
        first": fire once there are enough events pending, OR once the
        oldest one in the current batch has waited long enough, so a lone
        comment in a quiet room still gets answered eventually instead of
        waiting forever for two more that never come.

        `batch_wait_seconds <= 0` means "no time constraint," not "time
        constraint already met" -- it must contribute nothing to the OR, or
        setting only batch_min_events (leaving the time dropdown at "Tắt")
        would fire on the very first event regardless, since a disabled
        time gate would otherwise vacuously satisfy the OR on its own.
        Defaults (1, 0) still reproduce the old fire-immediately behavior:
        has_pending() alone already implies pending_count() >= 1, so
        enough_events carries it without the time gate's help.
        """
        if self.social_turn_in_flight:
            return
        if not self.scheduler.has_pending():
            return
        if self.voice_active:
            return
        enough_events = self.scheduler.pending_count() >= batch_min_events
        waited_enough = batch_wait_seconds > 0 and (
            self.pending_since is None
            or time.monotonic() - self.pending_since >= batch_wait_seconds
        )
        if not (enough_events or waited_enough):
            return
        result = self.orchestrator.poll_social_turn(self.voice_active)
        if result is None:
            return
        _turn, formatted = result
        self.social_turn_in_flight = True
        self.pending_since = None
        self.note_activity()
        await self.upstream.send_text(formatted)

    async def poll_idle(self, idle_seconds: float) -> None:
        """Fire a spontaneous-topic turn if nothing (voice or social) has
        happened for `idle_seconds` -- a real host fills dead air instead of
        sitting in silence. Same in-flight guard as poll_social, and for the
        same reason: this and a social turn both ultimately call
        upstream.send_text, and firing a second one before the first's
        turn_done would overlap turns on the gateway exactly like the bug
        poll_social's own guard exists to prevent."""
        if self.voice_active or self.social_turn_in_flight:
            return
        if time.monotonic() - self.last_activity < idle_seconds:
            return
        self.social_turn_in_flight = True
        self.note_activity()
        await self.upstream.send_text(IDLE_TOPIC_PROMPT)


async def _maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value
