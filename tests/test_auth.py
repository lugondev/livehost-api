import httpx

from livehost.auth import introspect


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_an_active_ticket_returns_its_user_id_and_session_token():
    def handler(request):
        assert request.url.path == "/api/auth/introspect"
        assert request.headers["Authorization"].startswith("Bearer ")
        return httpx.Response(
            200,
            json={
                "success": True,
                "data": {"active": True, "user_id": "user-1", "session_token": "sess-tok"},
            },
        )

    async with _client(handler) as c:
        result = await introspect("tkt", client=c)
        assert result.user_id == "user-1"
        assert result.session_token == "sess-tok"


async def test_an_inactive_ticket_returns_none_for_both():
    def handler(request):
        return httpx.Response(
            200,
            json={
                "success": True,
                "data": {"active": False, "user_id": None, "session_token": None},
            },
        )

    async with _client(handler) as c:
        result = await introspect("tkt", client=c)
        assert result.user_id is None
        assert result.session_token is None


async def test_the_plugin_name_is_sent_so_the_gateway_can_check_the_audience():
    seen = {}

    def handler(request):
        import json

        seen.update(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "success": True,
                "data": {"active": True, "user_id": "u", "session_token": "s"},
            },
        )

    async with _client(handler) as c:
        await introspect("tkt", client=c)
    assert seen == {"token": "tkt", "plugin": "livehost"}


async def test_a_401_returns_inactive_rather_than_raising():
    """A rejected plugin secret must close the browser socket, not crash the
    handler and take the TikTok connection down with it."""

    def handler(request):
        return httpx.Response(401, json={"success": False, "error": "invalid"})

    async with _client(handler) as c:
        result = await introspect("tkt", client=c)
        assert result.user_id is None
        assert result.session_token is None


async def test_a_gateway_outage_returns_inactive_rather_than_raising():
    def handler(request):
        raise httpx.ConnectError("gateway down")

    async with _client(handler) as c:
        result = await introspect("tkt", client=c)
        assert result.user_id is None
        assert result.session_token is None


async def test_an_unauthenticated_gateway_caller_yields_an_empty_user_id():
    """Dev mode: the gateway has auth disabled, so the ticket carries "". That
    is a valid identity meaning 'no owner', not a failure -- and it still
    comes with a real session_token, since resolve_ws_identity's fallback
    doesn't special-case an empty user_id either."""

    def handler(request):
        return httpx.Response(
            200,
            json={
                "success": True,
                "data": {"active": True, "user_id": "", "session_token": "s"},
            },
        )

    async with _client(handler) as c:
        result = await introspect("tkt", client=c)
        assert result.user_id == ""
        assert result.session_token == "s"


async def test_a_200_with_non_json_body_returns_inactive_rather_than_raising():
    """The gateway might hiccup and return a 200 with garbage. That must close
    one browser socket, not crash the handler and take the TikTok connection down."""

    def handler(request):
        return httpx.Response(200, content=b"not json")

    async with _client(handler) as c:
        result = await introspect("tkt", client=c)
        assert result.user_id is None
        assert result.session_token is None


async def test_a_200_with_data_not_a_dict_returns_inactive_rather_than_raising():
    """If the gateway returns data as a string instead of a dict, that is a
    protocol violation, not a ticket issue. Must return inactive, never raise."""

    def handler(request):
        return httpx.Response(200, json={"success": True, "data": "oops"})

    async with _client(handler) as c:
        result = await introspect("tkt", client=c)
        assert result.user_id is None
        assert result.session_token is None


async def test_a_200_with_missing_data_returns_inactive_rather_than_raising():
    """If the gateway returns no data key at all, we cannot determine if the
    ticket is active. Must return inactive, never raise."""

    def handler(request):
        return httpx.Response(200, json={"success": True})

    async with _client(handler) as c:
        result = await introspect("tkt", client=c)
        assert result.user_id is None
        assert result.session_token is None


async def test_a_non_string_session_token_is_dropped_defensively():
    """A malformed-but-200 gateway response (session_token the wrong type)
    must not hand ws.py something it would pass straight into a WS
    subprotocol list expecting a string."""

    def handler(request):
        return httpx.Response(
            200,
            json={
                "success": True,
                "data": {"active": True, "user_id": "user-1", "session_token": 12345},
            },
        )

    async with _client(handler) as c:
        result = await introspect("tkt", client=c)
        assert result.user_id == "user-1"
        assert result.session_token is None


async def test_active_with_a_missing_session_token_key_is_none_not_a_crash():
    """An older/mismatched gateway that answers active=true without ever
    adding session_token must not raise a KeyError -- .get() already covers
    this, but the case is worth pinning down explicitly."""

    def handler(request):
        return httpx.Response(
            200, json={"success": True, "data": {"active": True, "user_id": "user-1"}}
        )

    async with _client(handler) as c:
        result = await introspect("tkt", client=c)
        assert result.user_id == "user-1"
        assert result.session_token is None
