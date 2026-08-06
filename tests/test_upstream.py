from livehost.upstream import Upstream, build_upstream_url


class _FakeWebSocket:
    """Records every frame sent to it, without opening a real socket."""

    def __init__(self) -> None:
        self.sent: list[str | bytes] = []

    async def send(self, message: str | bytes) -> None:
        self.sent.append(message)


def test_the_url_carries_the_ticket_and_the_requested_modalities():
    url = build_upstream_url("http://gw:8000", "tkt", {"profile": "host", "output": "audio,text"})
    assert url.startswith("ws://gw:8000/v1/conversation/stream?")
    assert "token=tkt" in url
    assert "profile=host" in url
    assert "output=audio%2Ctext" in url


def test_https_becomes_wss():
    url = build_upstream_url("https://gw", "tkt", {})
    assert url.startswith("wss://gw/v1/conversation/stream?")


def test_empty_params_are_dropped_rather_than_sent_blank():
    """A blank ?profile= is not the same as no profile: the gateway would try
    to resolve a profile named "" and warn about it."""
    url = build_upstream_url("http://gw", "tkt", {"profile": "", "voice": None})
    assert "profile=" not in url
    assert "voice=" not in url


async def test_send_text_raw_forwards_the_browsers_frame_byte_for_byte():
    """The relay uses this so the browser's own abort/flush/end frames reach
    the gateway unchanged. If this re-serialized through the control-message
    envelope (like send_text/abort/flush do), key order or spacing could
    differ from what the browser sent, and the frame recorded upstream would
    not equal the frame that came in."""
    upstream = Upstream("http://gw", "tkt", {})
    fake_ws = _FakeWebSocket()
    upstream._ws = fake_ws

    raw = '{"type":"abort","extra":1}'
    await upstream.send_text_raw(raw)

    assert fake_ws.sent == [raw]


async def test_send_text_raw_before_connecting_is_a_noop_not_a_crash():
    """A relay message that arrives before (or after) the upstream socket is
    open must not take the browser connection down with an AttributeError on
    a None socket -- every other send method on Upstream guards the same way."""
    upstream = Upstream("http://gw", "tkt", {})
    await upstream.send_text_raw('{"type":"abort"}')
