import pytest


@pytest.fixture(autouse=True)
def _isolate_memory_db_path(monkeypatch, tmp_path):
    """Point ViewerMemoryStore's default DB path at a per-test tmp_path for
    every test, not just the ones that explicitly monkeypatch it (see
    tests/test_ws_social.py's memory test for an example of a test that
    does). Without this, any test that opens a real WS connection --
    livehost_stream claims a session and calls _get_memory_store(), which
    falls back to settings.memory_db_path's real default
    ("livehost_memory.db") -- writes a real, growing SQLite file into the
    repo root on every such test run.

    monkeypatch (not a manual save/restore) so the value is undone
    automatically after each test, same as any other monkeypatch use in
    this suite."""
    monkeypatch.setattr(
        "livehost.settings.settings.memory_db_path", str(tmp_path / "livehost_memory.db")
    )
