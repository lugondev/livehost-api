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

    def get(self, session_id: str) -> LivehostSession | None:
        return self._sessions.get(session_id)

    def unregister(self, session_id: str) -> None:
        self._sessions.pop(session_id, None)


livehost_registry = LivehostSessionRegistry()
