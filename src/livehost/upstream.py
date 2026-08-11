"""The plugin's client for WS /v1/conversation/stream.

This is the entire host API for voice. Everything the old in-process handler
did with 21 gateway imports -- STT, endpointing, LLM, TTS with prefetch and
pacing, quota, usage, history, memory injection -- happens on the far side of
this socket.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from urllib.parse import urlencode

import websockets


def build_upstream_url(base: str, params: dict[str, str | None]) -> str:
    """ws(s):// URL for the conversation socket.

    Blank and None params are dropped rather than sent empty: `?profile=` is
    not the same as no profile -- the gateway would try to resolve a profile
    named "" and emit a warning the browser would then see.

    No token here, and none was ever meant to be. Found live: the gateway's
    own resolve_ws_identity never reads a `?token=` query param at all -- it
    only accepts a bearer riding the `Sec-WebSocket-Protocol: bearer, <token>`
    subprotocol slot, same as every other WS bearer in this codebase. A
    query-param token silently authenticated nothing; every connection was
    rejected with an HTTP 403 at the handshake, before this file's own
    `connect()` ever got a WS to hand back.
    """
    scheme = "wss" if base.startswith("https://") else "ws"
    host = base.split("://", 1)[-1].rstrip("/")
    query = {k: v for k, v in params.items() if v}
    return f"{scheme}://{host}/v1/conversation/stream?{urlencode(query)}"


class Upstream:
    """One conversation socket, for one browser session.

    Authenticates as a plugin SESSION token (see tokens.py's
    PLUGIN_SESSION_TTL_SECONDS on the gateway) -- never the browser's own
    plugin ticket. The ticket is audience-bound to one plugin and designed to
    survive a browser/URL; resolve_ws_identity does not (and must not)
    accept it directly. The session token is what auth.introspect() trades
    it for, server-to-server, specifically so this connection can open.
    """

    def __init__(self, base: str, session_token: str, params: dict[str, str | None]) -> None:
        self._url = build_upstream_url(base, params)
        self._session_token = session_token
        self._ws: websockets.ClientConnection | None = None

    async def connect(self) -> None:
        self._ws = await websockets.connect(
            self._url, subprotocols=["bearer", self._session_token], max_size=None
        )

    async def send_audio(self, data: bytes) -> None:
        if self._ws is not None:
            await self._ws.send(data)

    async def _control(self, **payload) -> None:
        if self._ws is not None:
            await self._ws.send(json.dumps(payload))

    async def send_text(self, text: str) -> None:
        await self._control(type="text", text=text)

    async def send_text_raw(self, raw: str) -> None:
        """Forward a text frame upstream byte-for-byte, exactly as received.

        Used by the relay to pass the browser's own control frames --
        abort/flush/end and whatever else it sends -- straight through
        without re-serializing them, so a different key order or float
        formatting on the way through never turns into a protocol mismatch
        the gateway has to interpret.
        """
        if self._ws is not None:
            await self._ws.send(raw)

    async def abort(self) -> None:
        await self._control(type="abort")

    async def flush(self) -> None:
        await self._control(type="flush")

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()
            self._ws = None

    async def events(self) -> AsyncIterator[dict | bytes]:
        """Yield upstream traffic: parsed JSON events, raw audio as bytes."""
        if self._ws is None:
            return
        async for message in self._ws:
            if isinstance(message, bytes):
                yield message
            else:
                yield json.loads(message)
