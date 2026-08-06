import pytest
from fastapi.testclient import TestClient

from livehost.app import app
from livehost.registry import LivehostSession, livehost_registry


class _Ingestor:
    state = type("S", (), {"value": "idle"})()
    unique_id = None

    async def start(self, unique_id):
        self.unique_id = unique_id

    async def stop(self):
        pass


class _Scheduler:
    def pending_count(self):
        return 0


@pytest.fixture
def owned(monkeypatch):
    session = LivehostSession(scheduler=_Scheduler(), ingestor=_Ingestor(), user_id="user-1")
    livehost_registry.register("sess-1", session)

    async def _introspect(ticket, client=None):
        return {"tkt-owner": "user-1", "tkt-other": "user-2"}.get(ticket)

    monkeypatch.setattr("livehost.api.control.introspect", _introspect)
    yield
    livehost_registry.release("sess-1", session)


def _get(client, ticket):
    return client.get("/v1/livehost/sess-1/status", headers={"Authorization": f"Bearer {ticket}"})


def test_the_owner_can_read_status(owned):
    with TestClient(app) as client:
        assert _get(client, "tkt-owner").status_code == 200


def test_another_users_session_is_404_not_403(owned):
    """Not 403: an unowned session must be indistinguishable from a missing
    one, or the id space becomes an existence oracle."""
    with TestClient(app) as client:
        assert _get(client, "tkt-other").status_code == 404


def test_a_missing_ticket_cannot_drive_another_users_session(owned):
    with TestClient(app) as client:
        r = client.post("/v1/livehost/sess-1/connect", json={"unique_id": "@x"})
        assert r.status_code == 404


def test_status_for_an_unknown_session_is_404():
    with TestClient(app) as client:
        assert client.get("/v1/livehost/nope/status").status_code == 404


def test_connect_for_an_unknown_session_is_404():
    with TestClient(app) as client:
        r = client.post("/v1/livehost/nope/connect", json={"unique_id": "@someone"})
        assert r.status_code == 404
