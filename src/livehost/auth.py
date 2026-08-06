"""Trade a browser's ticket for a user id.

One round trip per connection, never on the audio path. The plugin cannot
verify the signature itself: that would mean holding the gateway's session
secret, and anything holding that secret can mint tokens for any user.
"""

from __future__ import annotations

import logging

import httpx

from livehost.settings import settings

logger = logging.getLogger(__name__)


async def introspect(ticket: str, client: httpx.AsyncClient | None = None) -> str | None:
    """Return the user id behind `ticket`, or None if it is not active.

    An empty string is a real, successful answer -- it is what the gateway
    sends when auth is disabled -- so callers must test for None, never for
    falsiness.

    Every failure mode collapses to None on purpose. A gateway outage or a
    rejected plugin secret must close one browser socket, not raise through
    the handler and take a hard-won TikTok connection down with it.
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
            return None
        body = response.json()
        data = body.get("data") if isinstance(body, dict) else None
        if not isinstance(data, dict) or not data.get("active"):
            return None
        user_id = data.get("user_id")
        return user_id if isinstance(user_id, str) else None
    except (httpx.HTTPError, ValueError) as exc:
        # ValueError covers json.JSONDecodeError. A gateway answering 200 with
        # a body we cannot parse is a hiccup like any other, and the contract
        # here is that a hiccup costs one browser socket -- never an exception
        # raised into the handler that owns the TikTok connection.
        logger.warning("introspect failed: %s", exc)
        return None
    finally:
        if owned:
            await client.aclose()
