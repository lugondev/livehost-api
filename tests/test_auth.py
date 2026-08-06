import httpx

from livehost.auth import introspect


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_an_active_ticket_returns_its_user_id():
    def handler(request):
        assert request.url.path == "/api/auth/introspect"
        assert request.headers["Authorization"].startswith("Bearer ")
        return httpx.Response(
            200, json={"success": True, "data": {"active": True, "user_id": "user-1"}}
        )

    async with _client(handler) as c:
        assert await introspect("tkt", client=c) == "user-1"


async def test_an_inactive_ticket_returns_none():
    def handler(request):
        return httpx.Response(
            200, json={"success": True, "data": {"active": False, "user_id": None}}
        )

    async with _client(handler) as c:
        assert await introspect("tkt", client=c) is None


async def test_the_plugin_name_is_sent_so_the_gateway_can_check_the_audience():
    seen = {}

    def handler(request):
        import json

        seen.update(json.loads(request.content))
        return httpx.Response(200, json={"success": True, "data": {"active": True, "user_id": "u"}})

    async with _client(handler) as c:
        await introspect("tkt", client=c)
    assert seen == {"token": "tkt", "plugin": "livehost"}


async def test_a_401_returns_none_rather_than_raising():
    """A rejected plugin secret must close the browser socket, not crash the
    handler and take the TikTok connection down with it."""

    def handler(request):
        return httpx.Response(401, json={"success": False, "error": "invalid"})

    async with _client(handler) as c:
        assert await introspect("tkt", client=c) is None


async def test_a_gateway_outage_returns_none_rather_than_raising():
    def handler(request):
        raise httpx.ConnectError("gateway down")

    async with _client(handler) as c:
        assert await introspect("tkt", client=c) is None


async def test_an_unauthenticated_gateway_caller_yields_an_empty_user_id():
    """Dev mode: the gateway has auth disabled, so the ticket carries "". That
    is a valid identity meaning 'no owner', not a failure."""

    def handler(request):
        return httpx.Response(200, json={"success": True, "data": {"active": True, "user_id": ""}})

    async with _client(handler) as c:
        assert await introspect("tkt", client=c) == ""
