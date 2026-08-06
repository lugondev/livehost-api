"""Two pumps and an arbiter, with no socket on either side so it can be tested.

Arbitration keeps its old meaning: a social turn only fires when the streamer
is not talking. What changed is where that fact comes from. The endpointer used
to live here; it now lives in the gateway, so `voice_active` is derived from
the upstream event stream instead -- speech_start sets it, turn_done clears it.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from livehost.orchestrator import LiveHostOrchestrator
from livehost.scheduler import EventScheduler

logger = logging.getLogger(__name__)


class Relay:
    def __init__(self, upstream, scheduler: EventScheduler) -> None:
        self.upstream = upstream
        self.scheduler = scheduler
        self.orchestrator = LiveHostOrchestrator(scheduler)
        self.voice_active = False
        self.social_turn_in_flight = False

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
                # The streamer talking over the co-host wins, always.
                if self.social_turn_in_flight:
                    await self.upstream.abort()
                    self.social_turn_in_flight = False
            elif event in ("turn_done", "aborted"):
                self.voice_active = False
                self.social_turn_in_flight = False
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

    async def poll_social(self) -> None:
        """Fire one social turn if the streamer is quiet and something waits."""
        result = self.orchestrator.poll_social_turn(self.voice_active)
        if result is None:
            return
        _turn, formatted = result
        self.social_turn_in_flight = True
        await self.upstream.send_text(formatted)


async def _maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value
