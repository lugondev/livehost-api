"""Trade a browser's ticket for a user id and a plugin session token.

One round trip per connection, never on the audio path. The plugin cannot
verify the signature itself: that would mean holding the gateway's session
secret, and anything holding that secret can mint tokens for any user.
"""

from __future__ import annotations

import logging
from typing import NamedTuple

import httpx

from livehost.settings import settings

logger = logging.getLogger(__name__)


class IntrospectResult(NamedTuple):
    user_id: str | None
    # Found live: the browser's plugin ticket cannot itself authenticate the
    # WS upstream connection to /v1/conversation/stream -- it verifies only
    # at this endpoint, by design (audience-bound, meant to survive a
    # browser/URL). This is the SEPARATE, purpose-built credential the
    # gateway mints in the same response, meant to travel exactly one more
    # hop: from here into Upstream.connect()'s subprotocol, in-process,
    # never back out to a browser. None whenever `user_id` is None.
    session_token: str | None


_INACTIVE = IntrospectResult(user_id=None, session_token=None)


async def introspect(ticket: str, client: httpx.AsyncClient | None = None) -> IntrospectResult:
    """Resolve `ticket` against the gateway. `user_id` is None if it is not
    active; `session_token` is None under the exact same condition.

    An empty string `user_id` is a real, successful answer -- it is what the
    gateway sends when auth is disabled -- so callers must test `user_id is
    None`, never falsiness.

    Every failure mode collapses to `_INACTIVE` on purpose. A gateway outage
    or a rejected plugin secret must close one browser socket, not raise
    through the handler and take a hard-won TikTok connection down with it.
    """
    owned = client is None
    client = client or httpx.AsyncClient()
    try:
        response = await client.post(
            f"{settings.gateway_url}/api/auth/introspect",
            json={"token": ticket, "plugin": settings.plugin_name},
            headers={"Authorization": f"Bearer {settings.plugin_secret}"},
            timeout=5.0,
        )
        if response.status_code != 200:
            logger.warning("introspect rejected: HTTP %s", response.status_code)
            return _INACTIVE
        body = response.json()
        data = body.get("data") if isinstance(body, dict) else None
        if not isinstance(data, dict) or not data.get("active"):
            return _INACTIVE
        user_id = data.get("user_id")
        if not isinstance(user_id, str):
            return _INACTIVE
        session_token = data.get("session_token")
        return IntrospectResult(
            user_id=user_id,
            session_token=session_token if isinstance(session_token, str) else None,
        )
    except (httpx.HTTPError, ValueError) as exc:
        # ValueError covers json.JSONDecodeError. A gateway answering 200 with
        # a body we cannot parse is a hiccup like any other, and the contract
        # here is that a hiccup costs one browser socket -- never an exception
        # raised into the handler that owns the TikTok connection.
        logger.warning("introspect failed: %s", exc)
        return _INACTIVE
    finally:
        if owned:
            await client.aclose()
