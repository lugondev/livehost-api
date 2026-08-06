"""connect / disconnect / status for a live session.

Ported from the gateway's api/routes/livehost.py:86-124. The only change is
where identity comes from: `scope_user_id(request)` became a ticket the caller
presents as a bearer, introspected against the gateway.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from livehost.auth import introspect
from livehost.registry import LivehostSession, livehost_registry

router = APIRouter(prefix="/v1/livehost", tags=["livehost"])


class TikTokConnectRequest(BaseModel):
    unique_id: str


async def _caller_user_id(request: Request) -> str | None:
    """None means "no real identity" -- either no ticket was presented, or the
    gateway said the ticket resolves to no owner (introspect's "" case,
    normalized here the same way ws.py normalizes it when it stores
    LivehostSession.user_id: `user_id or None`). Ownership comparison below
    treats both the same way an ownerless session's own user_id (also None)
    would be treated.
    """
    scheme, _, ticket = request.headers.get("Authorization", "").partition(" ")
    if scheme.lower() != "bearer" or not ticket.strip():
        return None
    return (await introspect(ticket.strip())) or None


async def _get_owned_session(session_id: str, request: Request) -> LivehostSession:
    """404s uniformly for "doesn't exist" and "exists but isn't yours", so this
    is not an existence oracle -- same contract the gateway version kept.

    Unlike the gateway's `_scope_user_id` (which returns None for an admin or
    for a caller when auth is disabled server-wide -- a deliberate "no
    restriction" bypass), a plugin caller who presents no ticket at all is not
    such a bypass: it must be denied against any session that actually has an
    owner. So this compares directly (`session.user_id != scope`) rather than
    only when `scope is not None` -- otherwise a request with no
    Authorization header at all would slip through as if it were exempt from
    ownership checks entirely.
    """
    session = livehost_registry.get(session_id)
    scope = await _caller_user_id(request)
    if session is None or session.user_id != scope:
        raise HTTPException(status_code=404, detail=f"livehost session '{session_id}' not found")
    return session


@router.post("/{session_id}/connect")
async def connect_tiktok(session_id: str, payload: TikTokConnectRequest, request: Request) -> dict:
    session = await _get_owned_session(session_id, request)
    await session.ingestor.start(payload.unique_id)
    return {
        "success": True,
        "data": {"state": session.ingestor.state.value, "unique_id": payload.unique_id},
    }


@router.post("/{session_id}/disconnect")
async def disconnect_tiktok(session_id: str, request: Request) -> dict:
    session = await _get_owned_session(session_id, request)
    await session.ingestor.stop()
    return {"success": True, "data": {"state": session.ingestor.state.value}}


@router.get("/{session_id}/status")
async def livehost_status(session_id: str, request: Request) -> dict:
    session = await _get_owned_session(session_id, request)
    return {
        "success": True,
        "data": {
            "state": session.ingestor.state.value,
            "unique_id": session.ingestor.unique_id,
            "pending_social_events": session.scheduler.pending_count(),
        },
    }
