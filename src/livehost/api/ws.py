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
from livehost.relay import Relay, _maybe_await
from livehost.scheduler import EventScheduler
from livehost.settings import settings
from livehost.upstream import Upstream

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/livehost", tags=["livehost"])

_SOCIAL_POLL_SECONDS = 0.25

# How many times the *upstream* conversation socket is redialled after a
# drop, before the down direction gives up. Deliberately unrelated to the
# ingestor's own TikTokLiveIngestor backoff/offline-poll settings -- these
# two connections are on separate reconnect budgets by design (see _down()).
_UPSTREAM_RECONNECT_MAX_ATTEMPTS = 5


def _mention_keywords() -> list[str]:
    return [k.strip() for k in settings.mention_keywords.split(",") if k.strip()]


def resume_params(params: dict, session_id: str | None) -> dict:
    """Upstream query params for a reconnect. session_id is what keeps the
    gateway writing to the same stored session, so history does not fork."""
    resumed = dict(params)
    if session_id:
        resumed["session_id"] = session_id
    return resumed


async def relay_with_reconnect(upstream, send_json, send_bytes, max_attempts: int = 5) -> None:
    """Relay downstream, redialling the gateway when it drops.

    Standalone and Relay-agnostic on purpose: it knows nothing about
    voice_active arbitration, the ingestor, or the registry -- it only
    reconnects `upstream` and forwards whatever it yields. The handler
    below does not call this directly (see `_down()`'s docstring for why);
    it exists as the plain, directly-testable shape of the reconnect loop
    that `_down()` follows.

    The ingestor is deliberately NOT in scope here: a TikTok connection costs
    backoff and time to rebuild, and an upstream hiccup must not spend it.
    Social events keep accumulating in the scheduler's bounded queue during the
    gap and are subject to its existing overflow behaviour.
    """
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        try:
            await upstream.connect()
            async for message in upstream.events():
                if isinstance(message, bytes):
                    await _maybe_await(send_bytes(message))
                else:
                    await _maybe_await(send_json(message))
            return
        except (ConnectionError, OSError) as exc:
            logger.warning("upstream dropped (attempt %d/%d): %s", attempt, max_attempts, exc)
            await asyncio.sleep(min(2 ** (attempt - 1), 10))
    logger.error("giving up on the upstream after %d attempts", max_attempts)


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

    # H5, ported: a caller-supplied session_id must not silently overwrite
    # another user's live session. Claimed here -- before the only `await` in
    # this function that runs before registration (`upstream.connect()`
    # below) -- rather than checked-then-registered around that await: two
    # connections racing the same session_id must not both observe "not
    # held" and both pass. `claim()` is atomic (no await in it) precisely so
    # this ordering closes that window instead of just narrowing it.
    session = LivehostSession(scheduler=scheduler, ingestor=ingestor, user_id=user_id or None)
    if not livehost_registry.claim(session_id, session):
        await websocket.send_json(
            {"event": "error", "message": f"Session '{session_id}' not found"}
        )
        await websocket.close(code=4401, reason="unauthorized")
        return

    # session_id here is the *local* session_id (browser-supplied, or the
    # uuid generated above when it wasn't) -- not `q.get("session_id")`
    # directly, so it is always present. That is what lets a reconnect (see
    # _down()) redial this same Upstream's URL unchanged and still land on
    # the same gateway-side stored session: resume_params bakes it into the
    # query once, here, and every subsequent upstream.connect() reuses that
    # same URL verbatim.
    upstream = Upstream(
        settings.gateway_url,
        ticket,
        resume_params(
            {
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
            session_id,
        ),
    )
    try:
        await upstream.connect()
    except Exception as exc:  # noqa: BLE001 - report upstream failure on the wire
        logger.warning("upstream connect failed for %s: %s", session_id, exc)
        # The claim above already holds the slot -- a connect failure here
        # must release it, or it leaks exactly the entry Finding 2 (teardown
        # leak) was about, just via a different path. release(), not a
        # delete-by-id: a racing reconnect could have already replaced this
        # entry with its own (claim() allows a same-owner reclaim), and
        # deleting by id alone would evict that newer, still-live session
        # instead of the failed one this connection actually owns.
        livehost_registry.release(session_id, session)
        await websocket.send_json({"event": "error", "message": "gateway unavailable"})
        await websocket.close()
        return

    relay = Relay(upstream=upstream, scheduler=scheduler)

    async def _down() -> None:
        """Relay upstream to browser, redialling the gateway if the
        conversation socket drops -- without disturbing the ingestor.

        Wiring choice (Task 10): the reconnect loop wraps `relay.pump_down`
        from the outside, rather than `pump_down` growing a `reconnect`
        parameter. That keeps `Relay.pump_down` itself -- and
        tests/test_relay.py, which calls it directly and unpatched -- exactly
        as Task 8 left it. Every attempt below re-enters `pump_down`, so its
        per-message arbitration (`voice_active`, barge-in abort) keeps
        running across a reconnect the same as it does within one unbroken
        connection.

        `upstream` is the only thing redialled, and only `upstream.connect()`
        is called -- the session_id already baked into its URL (see
        `resume_params` above) is unchanged by a reconnect, so the gateway
        resumes the same stored session instead of forking history. The
        ingestor never appears in this function, on purpose: it is a
        separate, far more expensive connection (its own backoff, its own
        offline polling), and an upstream hiccup here must not cost it --
        nothing below stops, restarts, or otherwise reaches `ingestor`.
        """
        attempt = 0
        while attempt < _UPSTREAM_RECONNECT_MAX_ATTEMPTS:
            attempt += 1
            try:
                if attempt > 1:
                    await upstream.connect()
                await relay.pump_down(websocket.send_json, websocket.send_bytes)
                return
            except (ConnectionError, OSError) as exc:
                logger.warning(
                    "upstream dropped for %s (attempt %d/%d): %s",
                    session_id,
                    attempt,
                    _UPSTREAM_RECONNECT_MAX_ATTEMPTS,
                    exc,
                )
                await asyncio.sleep(min(2 ** (attempt - 1), 10))
        logger.error(
            "giving up on the upstream for %s after %d attempts",
            session_id,
            _UPSTREAM_RECONNECT_MAX_ATTEMPTS,
        )

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
        # Each step below is independently guarded, and the release is
        # unconditional: a raise from ingestor.stop() or upstream.close()
        # (e.g. Upstream.close()'s unguarded `await self._ws.close()` on a
        # broken transport) must not skip the others, and must never leave
        # this session_id claimed forever -- a leaked entry there is a
        # session whose ingestor/upstream are gone but whose control routes
        # (/status, /connect, /disconnect) keep resolving to it.
        try:
            await ingestor.stop()
        except Exception as exc:  # noqa: BLE001 - teardown must not raise
            logger.warning("ingestor.stop() failed for %s: %s", session_id, exc)
        try:
            await upstream.close()
        except Exception as exc:  # noqa: BLE001 - teardown must not raise
            logger.warning("upstream.close() failed for %s: %s", session_id, exc)
        # release(), not delete-by-id: if the same owner reconnected under
        # this session_id while this connection was winding down, claim()
        # already replaced the registry entry with the new connection's
        # session. Deleting by id here would evict that newer, live entry
        # instead of this (superseded) connection's own -- compare-and-
        # delete only removes it if this is still the entry on file.
        livehost_registry.release(session_id, session)


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
