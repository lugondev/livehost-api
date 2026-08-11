"""The browser-facing socket.

Four jobs, and none of them is running a conversation turn:
  1. accept, trade the ticket for a user id AND a plugin session token
  2. open one upstream conversation socket authenticated with that session
     token -- never the browser's own ticket, which resolve_ws_identity does
     not (and must not) accept directly
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

# How many times the *upstream* conversation socket is redialled after a
# drop, before the down direction gives up. Deliberately unrelated to the
# ingestor's own TikTokLiveIngestor backoff/offline-poll settings -- these
# two connections are on separate reconnect budgets by design (see _down()).
_UPSTREAM_RECONNECT_MAX_ATTEMPTS = 5


def _mention_keywords() -> list[str]:
    return [k.strip() for k in settings.mention_keywords.split(",") if k.strip()]


def resume_params(params: dict, session_id: str | None) -> dict:
    """Query params for the Upstream connection that will later be resumed.

    Called exactly once, when the Upstream's URL is first built (see
    `livehost_stream` below) -- NOT once per reconnect attempt. Continuity
    across a later drop comes from `_redial()` simply redialling that same
    fixed URL, not from calling this function again; a maintainer reading
    `_down()`/`_redial()` looking for a per-attempt call to this should not
    expect to find one. Baking `session_id` in here, up front, is what makes
    that later redial land on the same gateway-side stored session instead
    of forking history.
    """
    resumed = dict(params)
    if session_id:
        resumed["session_id"] = session_id
    return resumed


async def _redial(
    upstream,
    run_once,
    *,
    max_attempts: int,
    already_connected: bool,
    label: str,
) -> None:
    """Retry `run_once()` against `upstream`, redialling on a drop.

    The one and only reconnect loop: attempt counting, backoff, the
    max-attempts bound, and the exception clause all live here exactly
    once, so production (`_down()`, below) and anything that tests the
    retry mechanics exercise the identical code path -- two independent
    copies of this shape previously existed and had already silently
    diverged (one connected on every attempt, the other gated it), and nothing
    was testing the one that shipped.

    `already_connected=True` skips `upstream.connect()` on the very first
    attempt, for a caller (like `_down()`) that already holds a live
    connection before this loop starts; later attempts always reconnect
    first. `run_once` is whatever should happen for the duration of one
    connection attempt -- this function knows nothing about what that is
    (Relay, arbitration, plain event forwarding, ...), which is what keeps
    it reusable without coupling it to any of them.
    """
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        try:
            if not (already_connected and attempt == 1):
                await upstream.connect()
            await run_once()
            return
        except (ConnectionError, OSError) as exc:
            logger.warning("%s dropped (attempt %d/%d): %s", label, attempt, max_attempts, exc)
            await asyncio.sleep(min(2 ** (attempt - 1), 10))
    logger.error("giving up on %s after %d attempts", label, max_attempts)


@router.websocket("/stream")
async def livehost_stream(websocket: WebSocket) -> None:
    ticket = websocket.query_params.get("ticket") or ""
    identity = await introspect(ticket)
    # session_token can only be None here alongside user_id (see
    # IntrospectResult) -- checking both explicitly rather than trusting that
    # invariant is what stops a malformed-but-200 gateway response from
    # reaching Upstream.connect() with a non-string subprotocol value later.
    if identity.user_id is None or identity.session_token is None:
        await websocket.close(code=4401, reason="unauthorized")
        return
    user_id = identity.user_id
    session_token = identity.session_token
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
    #
    # Sending it even when the browser omitted one (so it names an id the
    # gateway has never seen) is safe: per the gateway's own
    # ws_session_owner_denied contract, that function is "True if session_id
    # already exists and is not owned by the caller" -- an id it has never
    # seen is not denied, and ConversationSession.start() creates it fresh.
    # Checked against the gateway source during Task 10 review, not assumed.
    # session_token, not `ticket`: the browser's ticket cannot authenticate
    # this connection at all (see Upstream's own docstring). Fixed at
    # construction and reused verbatim by every reconnect attempt in _down()
    # below, same as the URL is -- both carry PLUGIN_SESSION_TTL_SECONDS'
    # 60s TTL, so a reconnect sequence whose cumulative backoff exceeds that
    # would present an expired token on a later attempt. Not solved here:
    # doing so well means re-introspecting the browser's own ticket, which is
    # equally short-lived and not refreshed by the browser either -- a
    # broader reconnect-identity design than fixing the first connection
    # calls for.
    upstream = Upstream(
        settings.gateway_url,
        session_token,
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
        from the outside (via the shared `_redial()` above), rather than
        `pump_down` growing a `reconnect` parameter. That keeps
        `Relay.pump_down` itself -- and tests/test_relay.py, which calls it
        directly and unpatched -- exactly as Task 8 left it. Every attempt
        re-enters `pump_down`, so its per-message arbitration (`voice_active`,
        barge-in abort) keeps running across a reconnect the same as it does
        within one unbroken connection.

        Both arbitration flags are reset immediately before every attempt
        (including redials). This matters: after a drop the plugin knows
        nothing about the gateway's runtime state, and resuming by
        session_id restores *history*, not endpointer state -- the resumed
        connection has a fresh endpointer that will never emit a turn_done
        for an utterance it never saw. Without this reset, a drop between
        speech_start and turn_done would leave voice_active stuck True
        forever, starving poll_social_turn and going silent for the rest of
        the session -- the same permanently-silent failure Task 8 guarded
        against, arriving through the reconnect path instead. A drop
        mid-social-turn would symmetrically leave social_turn_in_flight
        stuck True, firing a spurious abort on the next real speech_start.
        Resetting says: the new connection starts with nobody talking and no
        social turn in flight, which is the truth.

        `upstream` is the only thing redialled -- the session_id already
        baked into its URL (see `resume_params` above) is unchanged by a
        reconnect, so the gateway resumes the same stored session instead of
        forking history. The ingestor never appears in this function, on
        purpose: it is a separate, far more expensive connection (its own
        backoff, its own offline polling), and an upstream hiccup here must
        not cost it -- nothing below stops, restarts, or otherwise reaches
        `ingestor`.
        """

        async def _run_once() -> None:
            relay.voice_active = False
            relay.social_turn_in_flight = False
            await relay.pump_down(websocket.send_json, websocket.send_bytes)

        await _redial(
            upstream,
            _run_once,
            max_attempts=_UPSTREAM_RECONNECT_MAX_ATTEMPTS,
            already_connected=True,
            label=f"upstream for {session_id}",
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
