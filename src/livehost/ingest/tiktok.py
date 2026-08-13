"""TikTok Live connection lifecycle: connects, normalizes events onto a shared
queue, and reconnects on failure without ever affecting the rest of the
livehost session (voice keeps working if TikTok drops).

See docs/superpowers/specs/2026-07-05-livehost-tiktok-cohost-design.md section 3.

Adapts the real TikTokLive client's callback API to the connect()/events()/
close() protocol TikTokLiveIngestor expects (see ingestor.LiveClientProtocol),
normalizing its event objects into SocialEvent.

Mapping helpers (map_comment etc.) are pure and duck-typed so they're unit
testable without the real TikTokLive proto classes; only TikTokLiveClientAdapter
itself touches the actual library.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from collections.abc import Callable
from enum import Enum
from typing import Any, Protocol

from livehost.schemas import SocialEvent

logger = logging.getLogger(__name__)


class IngestorState(str, Enum):
    IDLE = "idle"
    CONNECTING = "connecting"
    LIVE = "live"
    RECONNECTING = "reconnecting"
    OFFLINE_WAITING = "offline_waiting"
    ERROR = "error"


class RoomOfflineError(Exception):
    """Raised by a client's connect() when the target room isn't currently live."""


class LiveClientProtocol(Protocol):
    async def connect(self) -> None: ...
    def events(self) -> Any: ...  # AsyncIterator[SocialEvent | None]
    async def close(self) -> None: ...


class TikTokLiveIngestor:
    def __init__(
        self,
        client_factory: Callable[[str], LiveClientProtocol],
        queue: "asyncio.Queue[SocialEvent]",
        backoff_initial: float = 1.0,
        backoff_max: float = 60.0,
        offline_poll_interval: float = 30.0,
        watchdog_idle_seconds: float = 300.0,
    ) -> None:
        self._client_factory = client_factory
        self.queue = queue
        self.backoff_initial = backoff_initial
        self.backoff_max = backoff_max
        self.offline_poll_interval = offline_poll_interval
        self.watchdog_idle_seconds = watchdog_idle_seconds

        self.state = IngestorState.IDLE
        self.unique_id: str | None = None
        self._task: asyncio.Task | None = None
        self._generation = 0
        self._stop_requested = False
        self._lock = asyncio.Lock()

    async def start(self, unique_id: str) -> None:
        async with self._lock:
            await self._stop_locked()
            self.unique_id = unique_id
            self._stop_requested = False
            self._generation += 1
            self.state = IngestorState.CONNECTING
            self._task = asyncio.create_task(self._run(self._generation))

    async def stop(self) -> None:
        async with self._lock:
            await self._stop_locked()

    async def _stop_locked(self) -> None:
        self._stop_requested = True
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001 - teardown must not raise
                pass
        self.state = IngestorState.IDLE
        self._task = None

    async def _run(self, generation: int) -> None:
        backoff = self.backoff_initial
        while not self._stop_requested and generation == self._generation:
            try:
                self.state = (
                    IngestorState.CONNECTING
                    if backoff == self.backoff_initial
                    else IngestorState.RECONNECTING
                )
                client = self._client_factory(self.unique_id)
                await client.connect()
            except RoomOfflineError:
                self.state = IngestorState.OFFLINE_WAITING
                await asyncio.sleep(self.offline_poll_interval)
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - transient connect failure
                logger.warning("tiktok ingestor connect failed for %s: %s", self.unique_id, exc)
                self.state = IngestorState.ERROR
                await self._sleep_backoff(backoff)
                backoff = min(backoff * 2, self.backoff_max)
                continue

            self.state = IngestorState.LIVE
            stale, received_event = await self._drain(client, generation)
            if received_event:
                # Backoff only resets once the connection has proven itself by
                # actually delivering an event, not merely completing the
                # handshake (a connect() that succeeds but then immediately
                # fails mid-stream must keep the exponential progression).
                backoff = self.backoff_initial

            if self._stop_requested or generation != self._generation:
                return
            if stale:
                continue  # reconnect immediately, no backoff for a stale (not failed) link
            self.state = IngestorState.RECONNECTING
            await self._sleep_backoff(backoff)
            backoff = min(backoff * 2, self.backoff_max)

    async def _drain(self, client: LiveClientProtocol, generation: int) -> tuple[bool, bool]:
        """Pull events from *client* into self.queue until it disconnects, errors,
        or goes stale. Returns (stale, received_event): stale is True if it ended
        because of watchdog staleness; received_event is True if at least one
        event was successfully pulled and queued during this connection."""
        events_iter = client.events().__aiter__()
        stale = False
        received_event = False
        try:
            while True:
                try:
                    raw_event = await asyncio.wait_for(
                        events_iter.__anext__(), timeout=self.watchdog_idle_seconds
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "tiktok ingestor stale for %s, forcing reconnect", self.unique_id
                    )
                    stale = True
                    break
                except StopAsyncIteration:
                    break
                if raw_event is None:  # adapter's clean-disconnect signal
                    break
                await self.queue.put(raw_event)
                received_event = True
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - transient mid-stream error
            logger.warning("tiktok ingestor stream error for %s: %s", self.unique_id, exc)
        finally:
            try:
                await client.close()
            except Exception:  # noqa: BLE001 - teardown must not raise
                pass
        return stale, received_event

    async def _sleep_backoff(self, backoff: float) -> None:
        jitter = random.uniform(0, backoff * 0.25)
        await asyncio.sleep(backoff + jitter)


def avatar_url(user) -> str | None:
    thumb = getattr(user, "avatar_thumb", None)
    urls = getattr(thumb, "m_urls", None) if thumb else None
    return urls[0] if urls else None


def map_comment(event) -> SocialEvent:
    return SocialEvent(
        id=str(uuid.uuid4()),
        kind="comment",
        user_id=event.user.unique_id,
        user_name=event.user.nickname,
        user_avatar_url=avatar_url(event.user),
        text=event.comment,
        timestamp=time.time(),
    )


def map_gift(event) -> SocialEvent | None:
    if event.streaking:
        return None  # wait for the streak to finish so gift_value is final
    return SocialEvent(
        id=str(uuid.uuid4()),
        kind="gift",
        user_id=event.user.unique_id,
        user_name=event.user.nickname,
        user_avatar_url=avatar_url(event.user),
        gift_name=event.gift.name,
        gift_value=event.repeat_count * event.gift.diamond_count,
        timestamp=time.time(),
    )


def map_like(event) -> SocialEvent:
    return SocialEvent(
        id=str(uuid.uuid4()),
        kind="like",
        user_id=event.user.unique_id,
        user_name=event.user.nickname,
        user_avatar_url=avatar_url(event.user),
        like_count=event.count,
        timestamp=time.time(),
    )


def map_follow(event) -> SocialEvent:
    return SocialEvent(
        id=str(uuid.uuid4()),
        kind="follow",
        user_id=event.user.unique_id,
        user_name=event.user.nickname,
        user_avatar_url=avatar_url(event.user),
        timestamp=time.time(),
    )


def map_share(event) -> SocialEvent:
    return SocialEvent(
        id=str(uuid.uuid4()),
        kind="share",
        user_id=event.user.unique_id,
        user_name=event.user.nickname,
        user_avatar_url=avatar_url(event.user),
        timestamp=time.time(),
    )


class TikTokLiveClientAdapter:
    def __init__(self, unique_id: str) -> None:
        from TikTokLive import TikTokLiveClient

        self._client = TikTokLiveClient(unique_id=unique_id)
        self._queue: asyncio.Queue = asyncio.Queue()
        self._register_handlers()

    def _register_handlers(self) -> None:
        # add_listener(), not on(): TikTokLiveClient.on() (client.py) calls
        # `super().on(event.get_type(), f)`, and pyee's base EventEmitter.on()
        # dispatches to `self.add_listener(event, f)` -- `self` is still this
        # TikTokLiveClient instance, so that lands back on
        # TikTokLiveClient.add_listener's own override, which calls
        # `event.get_type()` a SECOND time on what is by then already a
        # string. A genuine bug in TikTokLive 6.6.6 (confirmed: same crash
        # with a bare TikTokLiveClient + client.on(), no adapter code
        # involved) -- .add_listener() is the direct path and only resolves
        # the type once.
        from TikTokLive.events import CommentEvent, FollowEvent, GiftEvent, LikeEvent, ShareEvent

        self._client.add_listener(CommentEvent, self._on_comment)
        self._client.add_listener(GiftEvent, self._on_gift)
        self._client.add_listener(LikeEvent, self._on_like)
        self._client.add_listener(FollowEvent, self._on_follow)
        self._client.add_listener(ShareEvent, self._on_share)

        from TikTokLive.events.custom_events import DisconnectEvent, LiveEndEvent

        self._client.add_listener(DisconnectEvent, self._on_disconnect)
        self._client.add_listener(LiveEndEvent, self._on_disconnect)

    async def _on_comment(self, event) -> None:
        await self._queue.put(map_comment(event))

    async def _on_gift(self, event) -> None:
        mapped = map_gift(event)
        if mapped is not None:
            await self._queue.put(mapped)

    async def _on_like(self, event) -> None:
        await self._queue.put(map_like(event))

    async def _on_follow(self, event) -> None:
        await self._queue.put(map_follow(event))

    async def _on_share(self, event) -> None:
        await self._queue.put(map_share(event))

    async def _on_disconnect(self, event) -> None:
        await self._queue.put(None)  # signals TikTokLiveIngestor to reconnect immediately

    async def connect(self) -> None:
        from TikTokLive.client.errors import UserNotFoundError, UserOfflineError

        try:
            await self._client.start(fetch_live_check=True)
        except (UserOfflineError, UserNotFoundError) as exc:
            raise RoomOfflineError(str(exc)) from exc

    async def events(self):
        while True:
            yield await self._queue.get()

    async def close(self) -> None:
        await self._client.disconnect(close_client=True)
