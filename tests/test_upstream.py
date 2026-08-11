from livehost.upstream import Upstream, build_upstream_url


class _FakeWebSocket:
    """Records every frame sent to it, without opening a real socket."""

    def __init__(self) -> None:
        self.sent: list[str | bytes] = []

    async def send(self, message: str | bytes) -> None:
        self.sent.append(message)


def test_the_url_carries_no_token_at_all():
    """Found live: build_upstream_url used to embed the token as `?token=`,
    but the real gateway's resolve_ws_identity never reads a query param for
    bearer auth -- only the Sec-WebSocket-Protocol subprotocol slot. Every
    connection carrying a `?token=` was silently unauthenticated and
    rejected with an HTTP 403 at the handshake. The URL now carries only the
    session shape; the credential travels in connect()'s subprotocols."""
    url = build_upstream_url("http://gw:8000", {"profile": "host", "output": "audio,text"})
    assert url.startswith("ws://gw:8000/v1/conversation/stream?")
    assert "token=" not in url
    assert "profile=host" in url
    assert "output=audio%2Ctext" in url


def test_https_becomes_wss():
    url = build_upstream_url("https://gw", {})
    assert url.startswith("wss://gw/v1/conversation/stream?")


def test_empty_params_are_dropped_rather_than_sent_blank():
    """A blank ?profile= is not the same as no profile: the gateway would try
    to resolve a profile named "" and warn about it."""
    url = build_upstream_url("http://gw", {"profile": "", "voice": None})
    assert "profile=" not in url
    assert "voice=" not in url


async def test_connect_sends_the_session_token_as_a_bearer_subprotocol(monkeypatch):
    """The credential the gateway's resolve_ws_identity actually reads:
    Sec-WebSocket-Protocol: bearer, <session_token>. If this regresses back
    to a query param (or drops the subprotocol entirely), every connection
    would 403 at the handshake exactly as it did live before this fix."""
    seen = {}

    async def fake_connect(url, *, subprotocols=None, max_size=None):
        seen["url"] = url
        seen["subprotocols"] = subprotocols
        return _FakeWebSocket()

    monkeypatch.setattr("livehost.upstream.websockets.connect", fake_connect)

    upstream = Upstream("http://gw", "sess-tok-123", {})
    await upstream.connect()

    assert seen["subprotocols"] == ["bearer", "sess-tok-123"]
    assert "token=" not in seen["url"]


async def test_send_text_raw_forwards_the_browsers_frame_byte_for_byte():
    """The relay uses this so the browser's own abort/flush/end frames reach
    the gateway unchanged. If this re-serialized through the control-message
    envelope (like send_text/abort/flush do), key order or spacing could
    differ from what the browser sent, and the frame recorded upstream would
    not equal the frame that came in."""
    upstream = Upstream("http://gw", "sess-tok", {})
    fake_ws = _FakeWebSocket()
    upstream._ws = fake_ws

    raw = '{"type":"abort","extra":1}'
    await upstream.send_text_raw(raw)

    assert fake_ws.sent == [raw]


async def test_send_text_raw_before_connecting_is_a_noop_not_a_crash():
    """A relay message that arrives before (or after) the upstream socket is
    open must not take the browser connection down with an AttributeError on
    a None socket -- every other send method on Upstream guards the same way."""
    upstream = Upstream("http://gw", "sess-tok", {})
    await upstream.send_text_raw('{"type":"abort"}')
