"""The browser-facing socket.

Four jobs, and none of them is running a conversation turn:
  1. accept, trade the ticket for a user id
  2. open one upstream conversation socket with that user's ticket
  3. relay both directions
  4. poll the social scheduler and inject turns as text

Wire shape to the browser is unchanged from the in-process version --
{"event": ..., ...} -- because livehost.js reads it.
"""

from __future__ import annotations

import asyncio
import logging
import uuid

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from livehost.auth import introspect
from livehost.ingest.tiktok import TikTokLiveIngestor
from livehost.registry import LivehostSession, livehost_registry
from livehost.relay import Relay
from livehost.scheduler import EventScheduler
from livehost.settings import settings
from livehost.upstream import Upstream

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/livehost", tags=["livehost"])

_SOCIAL_POLL_SECONDS = 0.25


def _mention_keywords() -> list[str]:
    return [k.strip() for k in settings.mention_keywords.split(",") if k.strip()]


@router.websocket("/stream")
async def livehost_stream(websocket: WebSocket) -> None:
    ticket = websocket.query_params.get("ticket") or ""
    user_id = await introspect(ticket)
    if user_id is None:
        await websocket.close(code=4401, reason="unauthorized")
        return
    await websocket.accept()

    q = websocket.query_params
    session_id = q.get("session_id") or str(uuid.uuid4())

    upstream = Upstream(
        settings.gateway_url,
        ticket,
        {
            "session_id": q.get("session_id"),
            "profile": q.get("profile"),
            "tts_profile": q.get("tts_profile"),
            "language": q.get("language"),
            "stt_model": q.get("stt_model"),
            "voice": q.get("voice"),
            "sample_rate": q.get("sample_rate"),
            "audio_codec": q.get("audio_codec"),
            "audio_out": q.get("audio_out"),
            "output_sample_rate": q.get("output_sample_rate"),
            "output": q.get("output") or "audio,text",
        },
    )
    try:
        await upstream.connect()
    except Exception as exc:  # noqa: BLE001 - report upstream failure on the wire
        logger.warning("upstream connect failed for %s: %s", session_id, exc)
        await websocket.send_json({"event": "error", "message": "gateway unavailable"})
        await websocket.close()
        return

    scheduler = EventScheduler(
        mention_keywords=_mention_keywords(),
        individual_threshold=settings.individual_threshold,
        batch_top_k=settings.batch_top_k,
        max_queue_size=settings.queue_max_size,
    )
    raw_social_queue: asyncio.Queue = asyncio.Queue()
    ingestor = TikTokLiveIngestor(
        client_factory=_default_tiktok_client_factory,
        queue=raw_social_queue,
        backoff_initial=settings.backoff_initial_seconds,
        backoff_max=settings.backoff_max_seconds,
        offline_poll_interval=settings.offline_poll_interval_seconds,
        watchdog_idle_seconds=settings.watchdog_idle_seconds,
    )
    livehost_registry.register(
        session_id,
        LivehostSession(scheduler=scheduler, ingestor=ingestor, user_id=user_id or None),
    )
    relay = Relay(upstream=upstream, scheduler=scheduler)

    async def _down() -> None:
        await relay.pump_down(websocket.send_json, websocket.send_bytes)

    async def _drain_social() -> None:
        while True:
            event = await raw_social_queue.get()
            scheduler.enqueue(event)

    async def _poll_social() -> None:
        while True:
            await asyncio.sleep(_SOCIAL_POLL_SECONDS)
            await relay.poll_social()

    tasks = [
        asyncio.create_task(_down()),
        asyncio.create_task(_drain_social()),
        asyncio.create_task(_poll_social()),
    ]
    try:
        while True:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                break
            await relay.pump_up(message)
    except WebSocketDisconnect:
        pass
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001 - teardown must not raise
                pass
        await ingestor.stop()
        await upstream.close()
        livehost_registry.unregister(session_id)


def _default_tiktok_client_factory(unique_id: str):
    """Ported verbatim (module path aside -- TikTokLiveClientAdapter moved to
    livehost.ingest.tiktok in this repo) from the gateway's
    api/routes/livehost.py:660-663:

        def _default_tiktok_client_factory(unique_id: str):
            from app.services.livehost.tiktok_adapter import TikTokLiveClientAdapter
            return TikTokLiveClientAdapter(unique_id)
    """
    from livehost.ingest.tiktok import TikTokLiveClientAdapter

    return TikTokLiveClientAdapter(unique_id)
