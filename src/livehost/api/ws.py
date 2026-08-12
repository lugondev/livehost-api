"""The browser-facing socket.

Four jobs, and none of them is running a conversation turn:
  1. accept, trade the ticket for a user id AND a plugin session token
  2. open one upstream conversation socket authenticated with that session
     token -- never the browser's own ticket, which resolve_ws_identity does
     not (and must not) accept directly
  3. relay both directions
  4. poll the social scheduler and inject turns as text (and, optionally,
     an idle-topic turn when nothing has happened for a while)

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
from livehost.memory import ViewerMemoryStore
from livehost.registry import LivehostSession, livehost_registry
from livehost.relay import Relay
from livehost.scheduler import EventScheduler
from livehost.settings import settings
from livehost.upstream import Upstream

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/livehost", tags=["livehost"])

_SOCIAL_POLL_SECONDS = 0.25
# Coarser than the social poll: an idle-topic threshold is on the order of
# tens of seconds to minutes, not milliseconds, so sub-second precision on
# when it fires is not worth polling this many times more often.
_IDLE_POLL_SECONDS = 5.0

# How many times the *upstream* conversation socket is redialled after a
# drop, before the down direction gives up. Deliberately unrelated to the
# ingestor's own TikTokLiveIngestor backoff/offline-poll settings -- these
# two connections are on separate reconnect budgets by design (see _down()).
_UPSTREAM_RECONNECT_MAX_ATTEMPTS = 5

_memory_store: ViewerMemoryStore | None = None
_memory_store_path: str | None = None
# Path for which construction was already attempted and failed -- distinct
# from _memory_store_path (which only tracks the last *successful* build).
# Without this, a persistently broken path (e.g. unwritable) would retry
# ViewerMemoryStore(...) -- opening a real sqlite3 connection -- on every
# single call, including from the hot _drain_social loop (one call per
# social event), instead of degrading once and staying degraded until the
# path actually changes.
_memory_store_failed_path: str | None = None


def _get_memory_store() -> ViewerMemoryStore | None:
    """Lazily (re)build the module-level ViewerMemoryStore, rebuilding it
    whenever settings.memory_db_path has changed since the last call. Reads
    the path fresh on every call, the same pattern this module already uses
    for settings.gateway_url (see Upstream(...) below) -- lets tests
    monkeypatch the path before opening a WS connection and get an isolated
    store, instead of every test sharing one real on-disk database.

    Returns None (instead of raising) when construction itself fails -- e.g.
    sqlite3.OperationalError on an unwritable path. This call happens right
    after livehost_registry.claim(...) succeeds, before the try/finally that
    releases it, so letting a construction error propagate here would leak
    the claimed session_id forever. Callers must treat None as "memory
    unavailable, degrade to no notes for this session," not as an error to
    surface. Logs once per broken path (not on every call) to avoid both
    log spam and repeatedly retrying a connection that already failed."""
    global _memory_store, _memory_store_path, _memory_store_failed_path
    current_path = settings.memory_db_path
    if current_path == _memory_store_failed_path:
        return None
    if _memory_store is None or _memory_store_path != current_path:
        old_store = _memory_store
        try:
            _memory_store = ViewerMemoryStore(current_path, settings.memory_recent_comments)
        except Exception as exc:  # noqa: BLE001 - must never leak the caller's claimed session
            logger.warning("viewer memory store unavailable (%s): %s", current_path, exc)
            _memory_store = None
            _memory_store_path = None
            _memory_store_failed_path = current_path
            return None
        _memory_store_path = current_path
        _memory_store_failed_path = None
        if old_store is not None:
            old_store.close()
    return _memory_store


def _mention_keywords() -> list[str]:
    return [k.strip() for k in settings.mention_keywords.split(",") if k.strip()]


def _skip_social_event(event, skip_like_share: bool) -> bool:
    """True if `event` must never reach the scheduler -- "removed from
    context," not merely "not spoken aloud." A popular room fires dozens of
    likes a second; queuing either kind is pure noise that would either spam
    "X liked/shared" turns or, batched, drown out real comments/gifts for
    scheduler.batch_top_k slots. Opt-in (?skip_like_share=1), not the
    default: some streamers do want the co-host to react to those."""
    return skip_like_share and event.kind in ("like", "share")


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
    # 0/absent/garbage all mean "disabled" -- the browser's dropdown only
    # ever sends a positive integer or omits the param entirely, but this is
    # attacker/typo-controlled input same as every other query param here.
    try:
        idle_topic_seconds = max(0, int(q.get("idle_topic_seconds") or 0))
    except ValueError:
        idle_topic_seconds = 0
    skip_like_share = q.get("skip_like_share") == "1"
    try:
        batch_min_events = max(1, int(q.get("batch_min_events") or 1))
    except ValueError:
        batch_min_events = 1
    try:
        batch_wait_seconds = max(0.0, float(q.get("batch_wait_seconds") or 0))
    except ValueError:
        batch_wait_seconds = 0.0

    memory_id = q.get("memory_id") or ""
    # "" (never None) is the anonymous-mode collapse point -- same
    # convention as owner_key below and LivehostSession.user_id elsewhere:
    # every unauthenticated caller shares one owner scope.
    owner_key = user_id or ""

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

    memory_store = _get_memory_store()
    if memory_store is not None:
        memory_store.cleanup(settings.memory_retention_days)

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
                # Per-session persona override (gateway's
                # SessionRuntimeConfig.persona_override) -- lets this page's
                # own persona field replace profile.system_prompt without
                # needing edit access to a gateway profile. Forwarded
                # unchanged here so a reconnect (_redial, via this same
                # resume_params call) keeps the same persona for the rest of
                # the session instead of silently reverting to the profile's
                # default mid-stream.
                "system_prompt": q.get("system_prompt"),
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
            # Recorded unconditionally -- memory is a viewer profile,
            # independent of whether skip_like_share keeps this particular
            # event out of the spoken queue below. memory_store is None when
            # the store is unavailable (see _get_memory_store) -- that
            # degrades to "no note for this session," same as any other
            # storage failure, never a dropped social event.
            memory_store = _get_memory_store()
            event.viewer_note = (
                memory_store.note_and_record(owner_key, memory_id, event)
                if memory_store is not None
                else None
            )
            # A room's like/share volume is not treated as "activity"
            # either (no note_activity() call on this branch) -- silent
            # likes must not keep poll_idle from firing when nothing else
            # is happening.
            if _skip_social_event(event, skip_like_share):
                continue
            scheduler.enqueue(event)
            # A comment that arrives but hasn't fired a turn yet (still
            # batching, or the streamer is mid-turn) is still evidence the
            # room is live -- must not let poll_idle fire underneath it.
            relay.note_activity()
            # Starts the batch-wait clock on the FIRST event of a fresh
            # window -- see Relay.note_pending's own docstring for why it
            # must not reset on every subsequent event in the same batch.
            relay.note_pending()

    async def _poll_social() -> None:
        while True:
            await asyncio.sleep(_SOCIAL_POLL_SECONDS)
            await relay.poll_social(
                batch_min_events=batch_min_events, batch_wait_seconds=batch_wait_seconds
            )

    async def _poll_idle() -> None:
        while True:
            await asyncio.sleep(_IDLE_POLL_SECONDS)
            await relay.poll_idle(idle_topic_seconds)

    tasks = [
        asyncio.create_task(_down()),
        asyncio.create_task(_drain_social()),
        asyncio.create_task(_poll_social()),
    ]
    if idle_topic_seconds > 0:
        tasks.append(asyncio.create_task(_poll_idle()))
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
