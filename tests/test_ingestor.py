import asyncio

import pytest

from livehost.ingest.tiktok import IngestorState, RoomOfflineError, TikTokLiveIngestor
from livehost.schemas import SocialEvent


def _event(i: int) -> SocialEvent:
    return SocialEvent(id=f"e{i}", kind="comment", user_id="u", user_name="user", text=str(i), timestamp=float(i))


class FakeClient:
    """Test double for the connect()/events()/close() protocol.

    ``script`` is a list of behaviors consumed one per connect() attempt:
    - a list[SocialEvent | None] -> connect succeeds, events() yields them then hangs
      (simulating a still-open connection) unless the list ends with None (clean
      disconnect) or the test cancels it.
    - an Exception instance -> connect() raises it.
    """

    instances: list["FakeClient"] = []

    def __init__(self, unique_id: str, script: list) -> None:
        self.unique_id = unique_id
        self._script = script
        self.closed = False
        FakeClient.instances.append(self)

    async def connect(self) -> None:
        behavior = self._script.pop(0)
        if isinstance(behavior, Exception):
            raise behavior
        self._events = behavior

    async def _event_gen(self):
        for item in self._events:
            if isinstance(item, Exception):
                raise item
            yield item
        # Simulate a connection that just sits open with no more events, so the
        # watchdog (not StopAsyncIteration) is what ends it.
        await asyncio.sleep(3600)

    def events(self):
        return self._event_gen()

    async def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _reset_fake_client():
    FakeClient.instances.clear()
    yield
    FakeClient.instances.clear()


async def test_start_connects_and_forwards_events_to_queue():
    scripts = [[_event(1), _event(2)]]
    factory = lambda uid: FakeClient(uid, [scripts.pop(0)])
    queue: asyncio.Queue = asyncio.Queue()
    ingestor = TikTokLiveIngestor(client_factory=factory, queue=queue, watchdog_idle_seconds=3600)

    await ingestor.start("alice")
    first = await asyncio.wait_for(queue.get(), timeout=1)
    second = await asyncio.wait_for(queue.get(), timeout=1)

    assert ingestor.state == IngestorState.LIVE
    assert [first.text, second.text] == ["1", "2"]

    await ingestor.stop()
    assert ingestor.state == IngestorState.IDLE
    assert FakeClient.instances[0].closed is True


async def test_room_offline_polls_slowly_then_recovers():
    scripts = [RoomOfflineError("offline"), [_event(1)]]
    factory = lambda uid: FakeClient(uid, [scripts.pop(0)])
    queue: asyncio.Queue = asyncio.Queue()
    ingestor = TikTokLiveIngestor(
        client_factory=factory, queue=queue, offline_poll_interval=0.01, watchdog_idle_seconds=3600,
    )

    await ingestor.start("alice")
    await asyncio.sleep(0.02)
    assert ingestor.state in (IngestorState.OFFLINE_WAITING, IngestorState.LIVE)

    event = await asyncio.wait_for(queue.get(), timeout=1)
    assert event.text == "1"
    assert ingestor.state == IngestorState.LIVE
    await ingestor.stop()


async def test_transient_error_backs_off_then_reconnects():
    scripts = [RuntimeError("network blip"), [_event(1)]]
    factory = lambda uid: FakeClient(uid, [scripts.pop(0)])
    queue: asyncio.Queue = asyncio.Queue()
    ingestor = TikTokLiveIngestor(
        client_factory=factory, queue=queue,
        backoff_initial=0.01, backoff_max=0.02, watchdog_idle_seconds=3600,
    )

    await ingestor.start("alice")
    event = await asyncio.wait_for(queue.get(), timeout=1)
    assert event.text == "1"
    await ingestor.stop()


async def test_mid_stream_error_backs_off_then_reconnects():
    # Distinct from test_transient_error_backs_off_then_reconnects: there the
    # exception is raised by connect() itself. Here connect() succeeds and an
    # event is delivered first, then events() raises mid-stream -- exercising
    # the `except Exception` branch inside _drain rather than the one in _run.
    scripts = [[_event(1), RuntimeError("mid-stream blip")], [_event(2)]]
    factory = lambda uid: FakeClient(uid, [scripts.pop(0)])
    queue: asyncio.Queue = asyncio.Queue()
    ingestor = TikTokLiveIngestor(
        client_factory=factory, queue=queue,
        backoff_initial=0.01, backoff_max=0.02, watchdog_idle_seconds=3600,
    )

    await ingestor.start("alice")
    first = await asyncio.wait_for(queue.get(), timeout=1)
    second = await asyncio.wait_for(queue.get(), timeout=1)

    assert [first.text, second.text] == ["1", "2"]
    assert FakeClient.instances[0].closed is True
    assert len(FakeClient.instances) == 2
    await ingestor.stop()


async def test_clean_disconnect_signal_reconnects_immediately():
    scripts = [[_event(1), None], [_event(2)]]
    factory = lambda uid: FakeClient(uid, [scripts.pop(0)])
    queue: asyncio.Queue = asyncio.Queue()
    ingestor = TikTokLiveIngestor(
        client_factory=factory, queue=queue, backoff_initial=0.01, watchdog_idle_seconds=3600,
    )

    await ingestor.start("alice")
    first = await asyncio.wait_for(queue.get(), timeout=1)
    second = await asyncio.wait_for(queue.get(), timeout=1)
    assert [first.text, second.text] == ["1", "2"]
    await ingestor.stop()


async def test_stale_connection_forces_reconnect_via_watchdog():
    scripts = [[_event(1)], [_event(2)]]
    factory = lambda uid: FakeClient(uid, [scripts.pop(0)])
    queue: asyncio.Queue = asyncio.Queue()
    ingestor = TikTokLiveIngestor(
        client_factory=factory, queue=queue, backoff_initial=0.01, watchdog_idle_seconds=0.05,
    )

    await ingestor.start("alice")
    first = await asyncio.wait_for(queue.get(), timeout=1)
    second = await asyncio.wait_for(queue.get(), timeout=2)
    assert [first.text, second.text] == ["1", "2"]
    assert FakeClient.instances[0].closed is True
    await ingestor.stop()


async def test_client_factory_construction_failure_sets_error_state_and_recovers():
    # Distinct from test_transient_error_backs_off_then_reconnects: there the
    # factory succeeds and connect() raises. Here the factory call itself
    # (client = self._client_factory(...)) raises before any FakeClient is
    # even constructed -- exercising the exact failure point described in the
    # "connect immediately dies" review finding: a constructor failure (e.g.
    # the optional tiktok extra isn't installed) must be caught, surfaced via
    # IngestorState.ERROR, and retried -- not escape _run as an orphaned task
    # exception that leaves the ingestor stuck in CONNECTING forever.
    scripts = [[_event(1)]]
    attempts = {"n": 0}

    def factory(uid):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("client construction failed")
        return FakeClient(uid, [scripts.pop(0)])

    queue: asyncio.Queue = asyncio.Queue()
    ingestor = TikTokLiveIngestor(
        client_factory=factory, queue=queue,
        backoff_initial=0.05, backoff_max=0.1, watchdog_idle_seconds=3600,
    )

    await ingestor.start("alice")
    await asyncio.sleep(0.01)
    assert ingestor.state == IngestorState.ERROR

    event = await asyncio.wait_for(queue.get(), timeout=1)
    assert event.text == "1"
    assert ingestor.state == IngestorState.LIVE
    assert attempts["n"] == 2
    await ingestor.stop()


async def test_starting_twice_stops_the_previous_connection():
    scripts = [[_event(1)], [_event(2)]]
    factory = lambda uid: FakeClient(uid, [scripts.pop(0)])
    queue: asyncio.Queue = asyncio.Queue()
    ingestor = TikTokLiveIngestor(client_factory=factory, queue=queue, watchdog_idle_seconds=3600)

    await ingestor.start("alice")
    await asyncio.wait_for(queue.get(), timeout=1)
    await ingestor.start("bob")
    event = await asyncio.wait_for(queue.get(), timeout=1)

    assert event.text == "2"
    assert FakeClient.instances[0].closed is True
    assert len(FakeClient.instances) == 2
    await ingestor.stop()
