"""In-memory registry of active livehost WS sessions, keyed by session_id, so
the REST connect/disconnect/status endpoints (Task 7) can reach the right
session's TikTokLiveIngestor. Entries are created when a WS session starts and
removed when it ends."""

from __future__ import annotations

from dataclasses import dataclass

from livehost.ingest.tiktok import TikTokLiveIngestor
from livehost.scheduler import EventScheduler


@dataclass
class LivehostSession:
    scheduler: EventScheduler
    ingestor: TikTokLiveIngestor
    # H5: the WS caller that registered this session (identity.user_id from
    # resolve_ws_identity -- None for an unauthenticated/dev-mode/fleet-token
    # caller, matching session_store's ownership convention elsewhere). The
    # 3 HTTP control routes in api/routes/livehost.py use this to deny any
    # OTHER user's connect/disconnect/status calls.
    user_id: str | None = None


class LivehostSessionRegistry:
    def __init__(self) -> None:
        self._sessions: dict[str, LivehostSession] = {}

    def register(self, session_id: str, session: LivehostSession) -> None:
        self._sessions[session_id] = session

    def claim(self, session_id: str, session: LivehostSession) -> bool:
        """Register `session` unless the id is already held by someone else.

        Atomic by construction: no `await` anywhere in this method, so on a
        single event loop no other coroutine can observe the gap between the
        lookup and the write. That is the whole point -- a caller that
        instead does `get()` then, after an `await` (e.g. connecting
        upstream), `register()`, leaves a window in which two connections
        racing the same session_id both see "not held" and both pass; this
        method closes that window by keeping the check and the write in the
        same, un-interruptible step. Prefer this over `register()` directly
        for any caller-suppliable session_id.

        Returns False if the id is held by a different owner (the caller
        must refuse); True on a fresh claim or a re-claim by the same owner
        (register()'s old overwrite-on-collision behavior, preserved for a
        legitimate reconnect under the same identity).

        Caveat, not a bug: when auth is disabled, every caller's user_id
        normalizes to the same value (None -- see ws.py's `user_id or
        None`), so this guard cannot distinguish two different anonymous
        callers and protects nothing in that mode. That mirrors the
        gateway's own WsIdentity.unauthenticated: with no ownership model to
        enforce, there is nothing for a guard like this to enforce either.
        """
        existing = self._sessions.get(session_id)
        if existing is not None and existing.user_id != session.user_id:
            return False
        self._sessions[session_id] = session
        return True

    def get(self, session_id: str) -> LivehostSession | None:
        return self._sessions.get(session_id)

    def release(self, session_id: str, session: LivehostSession) -> bool:
        """Remove `session_id` only if it still holds exactly `session`.

        Compare-and-delete, not delete-by-id. A same-owner reclaim (see
        claim()) replaces the stored session while the superseded
        connection is still winding down: that older connection's own
        teardown must not evict the newer, live entry, which a delete-by-id
        (the old `unregister()`, now removed) would do -- it removes
        whatever is stored under the id *now*, not what *this caller* put
        there. That is an ABA bug, and the exact same failure shape as the
        teardown leak `release()`'s callers exist to avoid, re-entering
        through the one path `claim()` deliberately still allows to
        overwrite: a legitimate same-owner reconnect.

        Uses `is`, not `==`: identity of the session object, not equality of
        its fields. Two sessions for the same owner compare equal on
        user_id, and that is exactly the case this must still tell apart.

        Like claim(), atomic by containing no await.

        Returns True if this caller's entry was the one removed.
        """
        if self._sessions.get(session_id) is session:
            del self._sessions[session_id]
            return True
        return False


livehost_registry = LivehostSessionRegistry()
