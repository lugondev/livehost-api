# Viewer Memory Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the livehost co-host a persistent memory of TikTok viewers (comment/like/share/follow/gift history), so a returning commenter gets a short note appended to their line in the text sent to the LLM — the co-host can continue the conversation like a real streamer who remembers regulars.

**Architecture:** A new `ViewerMemoryStore` (SQLite file, stdlib `sqlite3`) keyed by `(owner_key, memory_id, platform_user_id)`. `owner_key` is the WS caller's `user_id` (streamer identity); `memory_id` is a UUID the browser generates once and persists in `localStorage`, sent as a query param, so the same browser reconnecting across live sessions shares one memory bucket. `ws.py`'s `_drain_social` loop records every social event into the store and stamps a new `SocialEvent.viewer_note` field with a short Vietnamese note built from that viewer's *prior* history (before this event). `orchestrator.format_social_turn` appends the note, when present, next to `@user_name` in the formatted turn text — no scheduler/priority changes.

**Tech Stack:** Python 3.10+, FastAPI, pydantic v2, stdlib `sqlite3` (no new dependency), vanilla JS (no build step) for the browser side.

## Global Constraints

- Python >=3.10, ruff line-length 100 (`pyproject.toml`) — run `ruff check src tests` clean before each commit that touches `.py` files.
- No new dependencies: `sqlite3` is stdlib.
- All new/changed env vars use the existing `LIVEHOST_` prefix (`pydantic-settings` `env_prefix` in `settings.py`).
- pytest: `asyncio_mode = "auto"`, `pythonpath = ["src"]`, `testpaths = ["tests"]`, 120s timeout (`pyproject.toml`). Run tests with `python -m pytest tests/ -v` from the repo root (`/Users/lugon/code/speech-text-transformer/servers/livehost-api`).
- Note text is Vietnamese, matching `relay.py`'s existing `IDLE_TOPIC_PROMPT` convention.
- Comments in code explain WHY, not WHAT, matching the existing style throughout this codebase (see any existing file for tone).
- `owner_key` anonymous-mode caveat: when auth is disabled, every caller's `user_id` is `None` → `owner_key = ""` for everyone, so memory is *not* isolated between anonymous callers. This mirrors `LivehostSessionRegistry.claim`'s own documented caveat — do not attempt to fix it here.

---

### Task 1: `SocialEvent.viewer_note` field + new settings

**Files:**
- Modify: `src/livehost/schemas.py`
- Modify: `src/livehost/settings.py`
- Test: `tests/test_schemas.py`

**Interfaces:**
- Produces: `SocialEvent.viewer_note: str | None` (default `None`) — every later task reads/writes this field by this exact name.
- Produces: `settings.memory_db_path: str`, `settings.memory_recent_comments: int`, `settings.memory_retention_days: int`.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_schemas.py`:

```python
def test_social_event_viewer_note_defaults_to_none_and_can_be_set():
    event = SocialEvent(
        id="e1", kind="comment", user_id="u1", user_name="Alice", text="hi", timestamp=1.0,
    )
    assert event.viewer_note is None

    event.viewer_note = "đã bình luận 2 lần"
    assert event.viewer_note == "đã bình luận 2 lần"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_schemas.py::test_social_event_viewer_note_defaults_to_none_and_can_be_set -v`
Expected: FAIL with `AttributeError` or a pydantic validation error — `viewer_note` does not exist yet.

- [ ] **Step 3: Add the field to `SocialEvent`**

In `src/livehost/schemas.py`, add one field after `timestamp: float` (the last field):

```python
    timestamp: float
    # Filled in by ws.py's _drain_social loop, from livehost.memory.
    # ViewerMemoryStore.note_and_record, before this event reaches the
    # scheduler -- ingestors (tiktok.py) never set this themselves.
    viewer_note: str | None = None
```

- [ ] **Step 4: Add the three new settings fields**

In `src/livehost/settings.py`, add after `watchdog_idle_seconds: float = 300.0`:

```python
    watchdog_idle_seconds: float = 300.0

    # Viewer memory (livehost.memory.ViewerMemoryStore): persists per-viewer
    # comment/like/share/follow/gift history across live sessions.
    memory_db_path: str = "livehost_memory.db"
    memory_recent_comments: int = 5
    memory_retention_days: int = 90
```

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m pytest tests/test_schemas.py -v`
Expected: PASS (all tests in the file, including the new one).

- [ ] **Step 6: Lint and commit**

```bash
ruff check src/livehost/schemas.py src/livehost/settings.py
git add src/livehost/schemas.py src/livehost/settings.py tests/test_schemas.py
git commit -m "feat: add SocialEvent.viewer_note and memory settings"
```

---

### Task 2: `ViewerMemoryStore` (SQLite-backed viewer history)

**Files:**
- Create: `src/livehost/memory.py`
- Test: `tests/test_memory.py`

**Interfaces:**
- Consumes: `livehost.schemas.SocialEvent` (Task 1's `viewer_note` field is not touched by this class — this task only *computes* the note string and returns it; the caller, Task 4, is what assigns it onto the event).
- Produces (used by Task 4):
  - `ViewerMemoryStore(db_path: str, recent_comments_limit: int = 5)`
  - `ViewerMemoryStore.note_and_record(owner_key: str, memory_id: str, event: SocialEvent) -> str | None`
  - `ViewerMemoryStore.cleanup(retention_days: int) -> int`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_memory.py`:

```python
import json
import time

from livehost.memory import ViewerMemoryStore
from livehost.schemas import SocialEvent


def _event(kind="comment", user_id="u1", user_name="Bao", **kwargs) -> SocialEvent:
    defaults = dict(id="e", timestamp=1.0)
    defaults.update(kwargs)
    return SocialEvent(kind=kind, user_id=user_id, user_name=user_name, **defaults)


def test_first_time_viewer_gets_no_note(tmp_path):
    store = ViewerMemoryStore(str(tmp_path / "memory.db"))
    note = store.note_and_record("owner-1", "mem-1", _event(text="hi"))
    assert note is None


def test_returning_commenter_gets_a_note_with_prior_count_and_text(tmp_path):
    store = ViewerMemoryStore(str(tmp_path / "memory.db"))
    store.note_and_record("owner-1", "mem-1", _event(text="hi"))
    note = store.note_and_record("owner-1", "mem-1", _event(text="lai la toi day"))
    assert note is not None
    assert "1 lần" in note
    assert "hi" in note


def test_like_share_follow_flags_surface_in_the_note(tmp_path):
    store = ViewerMemoryStore(str(tmp_path / "memory.db"))
    store.note_and_record("owner-1", "mem-1", _event(kind="like", like_count=3))
    store.note_and_record("owner-1", "mem-1", _event(kind="share"))
    note = store.note_and_record("owner-1", "mem-1", _event(kind="follow"))
    assert "từng thả tim" in note
    assert "từng chia sẻ live" in note


def test_gift_value_accumulates_and_surfaces(tmp_path):
    # note_and_record's returned note always reflects the state BEFORE the
    # current call's event is applied, so accumulation (50 + 25 = 75) only
    # becomes visible in the note returned by a THIRD call.
    store = ViewerMemoryStore(str(tmp_path / "memory.db"))
    store.note_and_record(
        "owner-1", "mem-1", _event(kind="gift", gift_name="Rose", gift_value=50)
    )
    store.note_and_record(
        "owner-1", "mem-1", _event(kind="gift", gift_name="Rose", gift_value=25)
    )
    note = store.note_and_record(
        "owner-1", "mem-1", _event(kind="gift", gift_name="Rose", gift_value=1)
    )
    assert "75" in note


def test_recent_comments_cap_at_the_configured_limit(tmp_path):
    store = ViewerMemoryStore(str(tmp_path / "memory.db"), recent_comments_limit=2)
    store.note_and_record("owner-1", "mem-1", _event(text="one"))
    store.note_and_record("owner-1", "mem-1", _event(text="two"))
    store.note_and_record("owner-1", "mem-1", _event(text="three"))
    row = store._conn.execute(
        "SELECT recent_comments FROM viewers "
        "WHERE owner_key='owner-1' AND memory_id='mem-1' AND platform_user_id='u1'"
    ).fetchone()
    comments = json.loads(row[0])
    assert [c["text"] for c in comments] == ["two", "three"]


def test_different_memory_ids_do_not_share_history(tmp_path):
    store = ViewerMemoryStore(str(tmp_path / "memory.db"))
    store.note_and_record("owner-1", "mem-A", _event(text="hi"))
    note = store.note_and_record("owner-1", "mem-B", _event(text="hi again"))
    assert note is None


def test_cleanup_removes_rows_past_the_retention_window(tmp_path):
    store = ViewerMemoryStore(str(tmp_path / "memory.db"))
    store.note_and_record("owner-1", "mem-1", _event(user_id="old-viewer", text="hi"))
    # Backdate last_seen directly instead of sleeping in the test.
    store._conn.execute(
        "UPDATE viewers SET last_seen=? WHERE platform_user_id='old-viewer'",
        (time.time() - 200 * 86400,),
    )
    store._conn.commit()
    store.note_and_record("owner-1", "mem-1", _event(user_id="fresh-viewer", text="hi"))

    deleted = store.cleanup(retention_days=90)

    assert deleted == 1
    remaining = store._conn.execute("SELECT platform_user_id FROM viewers").fetchall()
    assert remaining == [("fresh-viewer",)]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_memory.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'livehost.memory'`.

- [ ] **Step 3: Write `src/livehost/memory.py`**

```python
"""Persistent per-viewer history (comments/likes/shares/follows/gifts) so a
returning TikTok commenter can be greeted like a regular instead of a
stranger every single time. Backed by a local SQLite file -- reused across
live sessions (unlike EventScheduler/LivehostSession, which are per-WS-
connection) via the (owner_key, memory_id) scope key ws.py supplies. See
docs/superpowers/specs/2026-08-12-viewer-memory-design.md.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time

from livehost.schemas import SocialEvent

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS viewers (
    owner_key TEXT NOT NULL,
    memory_id TEXT NOT NULL,
    platform_user_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    comment_count INTEGER NOT NULL DEFAULT 0,
    liked INTEGER NOT NULL DEFAULT 0,
    shared INTEGER NOT NULL DEFAULT 0,
    followed INTEGER NOT NULL DEFAULT 0,
    gift_count INTEGER NOT NULL DEFAULT 0,
    gift_value_total INTEGER NOT NULL DEFAULT 0,
    recent_comments TEXT NOT NULL DEFAULT '[]',
    first_seen REAL NOT NULL,
    last_seen REAL NOT NULL,
    PRIMARY KEY (owner_key, memory_id, platform_user_id)
);
"""

# Columns fetched by note_and_record's lookup, in this exact order -- both
# _build_note and _upsert unpack a row using this order, so changing one
# without the other silently misreads columns.
_LOOKUP_COLUMNS = (
    "comment_count, liked, shared, followed, gift_count, gift_value_total, recent_comments"
)


class ViewerMemoryStore:
    """Not safe for concurrent writers across processes (SQLite file
    locking would serialize them anyway); within one process it is only
    ever called from the single asyncio event loop that owns ws.py's
    _drain_social loop, so no additional locking is needed here."""

    def __init__(self, db_path: str, recent_comments_limit: int = 5) -> None:
        self.recent_comments_limit = recent_comments_limit
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(_SCHEMA)
        self._conn.commit()

    def note_and_record(self, owner_key: str, memory_id: str, event: SocialEvent) -> str | None:
        """Return a note built from this viewer's history BEFORE `event`,
        then persist `event` into that history. Returns None if there is
        nothing worth mentioning yet (first-ever interaction). Never raises
        -- a storage error degrades to "no note," logged, not a dropped
        social event."""
        try:
            row = self._conn.execute(
                f"SELECT {_LOOKUP_COLUMNS} FROM viewers "
                "WHERE owner_key=? AND memory_id=? AND platform_user_id=?",
                (owner_key, memory_id, event.user_id),
            ).fetchone()
            note = _build_note(row)
            self._upsert(owner_key, memory_id, event, row)
            return note
        except sqlite3.Error as exc:
            logger.warning("viewer memory write failed: %s", exc)
            return None

    def _upsert(
        self, owner_key: str, memory_id: str, event: SocialEvent, row: tuple | None
    ) -> None:
        now = time.time()
        if row is None:
            comment_count = liked = shared = followed = gift_count = gift_value_total = 0
            recent_comments: list[dict] = []
        else:
            comment_count, liked, shared, followed, gift_count, gift_value_total, recent_json = row
            recent_comments = json.loads(recent_json)

        if event.kind == "comment":
            comment_count += 1
            if event.text:
                recent_comments.append({"text": event.text, "ts": now})
                recent_comments = recent_comments[-self.recent_comments_limit :]
        elif event.kind == "like":
            liked = 1
        elif event.kind == "share":
            shared = 1
        elif event.kind == "follow":
            followed = 1
        elif event.kind == "gift":
            gift_count += 1
            gift_value_total += event.gift_value or 0

        if row is None:
            self._conn.execute(
                "INSERT INTO viewers (owner_key, memory_id, platform_user_id, user_name, "
                "comment_count, liked, shared, followed, gift_count, gift_value_total, "
                "recent_comments, first_seen, last_seen) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    owner_key, memory_id, event.user_id, event.user_name,
                    comment_count, liked, shared, followed, gift_count, gift_value_total,
                    json.dumps(recent_comments), now, now,
                ),
            )
        else:
            self._conn.execute(
                "UPDATE viewers SET user_name=?, comment_count=?, liked=?, shared=?, "
                "followed=?, gift_count=?, gift_value_total=?, recent_comments=?, last_seen=? "
                "WHERE owner_key=? AND memory_id=? AND platform_user_id=?",
                (
                    event.user_name, comment_count, liked, shared, followed,
                    gift_count, gift_value_total, json.dumps(recent_comments), now,
                    owner_key, memory_id, event.user_id,
                ),
            )
        self._conn.commit()

    def cleanup(self, retention_days: int) -> int:
        """Delete rows whose last_seen is older than retention_days. Called
        once per new WS session claim (ws.py), not per event -- a DELETE
        scan on every social event would be wasted work on a busy stream."""
        cutoff = time.time() - retention_days * 86400
        try:
            cur = self._conn.execute("DELETE FROM viewers WHERE last_seen < ?", (cutoff,))
            self._conn.commit()
            return cur.rowcount
        except sqlite3.Error as exc:
            logger.warning("viewer memory cleanup failed: %s", exc)
            return 0


def _build_note(row: tuple | None) -> str | None:
    if row is None:
        return None
    comment_count, liked, shared, followed, gift_count, gift_value_total, recent_json = row
    parts: list[str] = []
    if comment_count > 0:
        parts.append(f"đã bình luận {comment_count} lần")
    if liked:
        parts.append("từng thả tim")
    if shared:
        parts.append("từng chia sẻ live")
    if followed:
        parts.append("đã follow")
    if gift_value_total > 0:
        parts.append(f"từng tặng quà ({gift_value_total} coin)")
    recent_comments = json.loads(recent_json)
    if recent_comments:
        last_text = recent_comments[-1]["text"]
        if len(last_text) > 80:
            last_text = last_text[:80] + "…"
        parts.append(f'lần trước nói: "{last_text}"')
    return ", ".join(parts) if parts else None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_memory.py -v`
Expected: PASS (all 7 tests).

- [ ] **Step 5: Lint and commit**

```bash
ruff check src/livehost/memory.py
git add src/livehost/memory.py tests/test_memory.py
git commit -m "feat: add ViewerMemoryStore for persistent viewer history"
```

---

### Task 3: Surface the note in `format_social_turn`

**Files:**
- Modify: `src/livehost/orchestrator.py`
- Test: `tests/test_orchestrator.py`

**Interfaces:**
- Consumes: `SocialEvent.viewer_note` (Task 1).
- No new public interface — `format_social_turn(turn: SocialTurn) -> str` keeps its existing signature.

Note: only the `comment`, `gift`, `follow`, and `share` lines get the note suffix — the `like` line (`[TikTok] {count} new likes`) never names a user today, so there is nothing to attach a note to without changing that line's format, which is out of scope here.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_orchestrator.py` (the existing `_event` helper already forwards `**kwargs` to `SocialEvent(...)`, so `viewer_note=...` works unchanged):

```python
def test_format_comment_turn_includes_viewer_note_when_present():
    turn = SocialTurn(events=[_event(text="xin chao", viewer_note="đã bình luận 3 lần")])
    text = format_social_turn(turn)
    # "]" closes the "[TikTok @name]" bracket before the note suffix starts.
    assert "@Bao] (đã bình luận 3 lần): xin chao" in text


def test_format_comment_turn_omits_note_suffix_when_absent():
    turn = SocialTurn(events=[_event(text="xin chao")])
    text = format_social_turn(turn)
    assert "@Bao]: xin chao" in text
    assert "(" not in text


def test_format_gift_turn_includes_viewer_note_when_present():
    turn = SocialTurn(
        events=[_event(kind="gift", gift_name="Rose", gift_value=50, viewer_note="từng follow")]
    )
    text = format_social_turn(turn)
    assert "@Bao] (từng follow) sent a gift" in text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_orchestrator.py -v`
Expected: FAIL on the two new tests — current output has no `(...)` suffix at all.

- [ ] **Step 3: Update `format_social_turn`**

Replace the full contents of `src/livehost/orchestrator.py`'s existing `format_social_turn` function (and add `_note_suffix` above it) — the file currently reads (lines 13–28):

```python
def format_social_turn(turn: SocialTurn) -> str:
    lines: list[str] = []
    for event in turn.events:
        if event.kind == "comment":
            lines.append(f"[TikTok @{event.user_name}]: {event.text}")
        elif event.kind == "gift":
            lines.append(f"[TikTok @{event.user_name}] sent a gift: {event.gift_name} (value {event.gift_value})")
        elif event.kind == "follow":
            lines.append(f"[TikTok @{event.user_name}] just followed the stream")
        elif event.kind == "share":
            lines.append(f"[TikTok @{event.user_name}] shared the stream")
        elif event.kind == "like":
            lines.append(f"[TikTok] {event.like_count or 0} new likes")
    if turn.overflow_count:
        lines.append(f"(and {turn.overflow_count} more from other viewers)")
    return "\n".join(lines)
```

Replace it with:

```python
def _note_suffix(event: SocialEvent) -> str:
    """A parenthetical viewer-memory note, or "" when there is none --
    ViewerMemoryStore.note_and_record returns None for a first-time viewer,
    and this must vanish cleanly rather than render "()"."""
    return f" ({event.viewer_note})" if event.viewer_note else ""


def format_social_turn(turn: SocialTurn) -> str:
    lines: list[str] = []
    for event in turn.events:
        if event.kind == "comment":
            lines.append(f"[TikTok @{event.user_name}]{_note_suffix(event)}: {event.text}")
        elif event.kind == "gift":
            lines.append(
                f"[TikTok @{event.user_name}]{_note_suffix(event)} sent a gift: "
                f"{event.gift_name} (value {event.gift_value})"
            )
        elif event.kind == "follow":
            lines.append(
                f"[TikTok @{event.user_name}]{_note_suffix(event)} just followed the stream"
            )
        elif event.kind == "share":
            lines.append(f"[TikTok @{event.user_name}]{_note_suffix(event)} shared the stream")
        elif event.kind == "like":
            lines.append(f"[TikTok] {event.like_count or 0} new likes")
    if turn.overflow_count:
        lines.append(f"(and {turn.overflow_count} more from other viewers)")
    return "\n".join(lines)
```

Also add the import at the top of the file (after the existing `from livehost.scheduler import ...` line):

```python
from livehost.scheduler import EventScheduler, SocialTurn
from livehost.schemas import SocialEvent
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_orchestrator.py -v`
Expected: PASS (all tests in the file).

- [ ] **Step 5: Lint and commit**

```bash
ruff check src/livehost/orchestrator.py
git add src/livehost/orchestrator.py tests/test_orchestrator.py
git commit -m "feat: append viewer-memory note to formatted social turns"
```

---

### Task 4: Wire `ViewerMemoryStore` into the WS route

**Files:**
- Modify: `src/livehost/api/ws.py`
- Test: `tests/test_ws_social.py`

**Interfaces:**
- Consumes: `ViewerMemoryStore(db_path, recent_comments_limit)`, `.note_and_record(owner_key, memory_id, event) -> str | None`, `.cleanup(retention_days) -> int` (Task 2); `SocialEvent.viewer_note` (Task 1).

- [ ] **Step 1: Write the failing test**

Add to `tests/test_ws_social.py`:

```python
def test_returning_commenter_gets_a_viewer_note_in_the_second_turn(
    gateway, authed, monkeypatch, tmp_path
):
    """A second comment from the same TikTok user_id, under the same
    memory_id, carries a note referencing the earlier comment -- the
    co-host 'remembers' having talked to them already this browser."""
    from livehost.app import app
    from livehost.registry import livehost_registry
    from livehost.schemas import SocialEvent

    monkeypatch.setattr("livehost.settings.settings.memory_db_path", str(tmp_path / "memory.db"))
    monkeypatch.setattr("livehost.api.ws._SOCIAL_POLL_SECONDS", 0.0)

    def _texts():
        return [c["text"] for c in gateway["control"] if c.get("type") == "text"]

    session_id = "memory-1"
    with TestClient(app) as client:
        with client.websocket_connect(
            f"/v1/livehost/stream?ticket=good&session_id={session_id}&memory_id=browser-abc"
        ) as ws:
            ws.receive_json()
            session = livehost_registry.get(session_id)

            session.ingestor.queue.put_nowait(
                SocialEvent(
                    id="e1", kind="comment", user_id="u1", user_name="ann",
                    text="gia bao nhieu vay shop", timestamp=1.0,
                )
            )
            deadline = time.monotonic() + 5
            while len(_texts()) < 1 and time.monotonic() < deadline:
                time.sleep(0.01)

            session.ingestor.queue.put_nowait(
                SocialEvent(
                    id="e2", kind="comment", user_id="u1", user_name="ann",
                    text="minh hoi lai nha", timestamp=2.0,
                )
            )
            deadline = time.monotonic() + 5
            while len(_texts()) < 2 and time.monotonic() < deadline:
                time.sleep(0.01)

    texts = _texts()
    assert len(texts) == 2
    assert "đã bình luận 1 lần" in texts[1]
    assert "gia bao nhieu vay shop" in texts[1]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_ws_social.py::test_returning_commenter_gets_a_viewer_note_in_the_second_turn -v`
Expected: FAIL — the second text turn has no `"đã bình luận 1 lần"` note (memory isn't wired up yet), or the test times out waiting for 2 texts if wiring is entirely absent (it won't time out — turns fire either way, they just won't have notes; the `assert "đã bình luận 1 lần" in texts[1]` line fails).

- [ ] **Step 3: Wire the store into `ws.py`**

Add the import near the top of `src/livehost/api/ws.py` (alongside the other `livehost.*` imports):

```python
from livehost.ingest.tiktok import TikTokLiveIngestor
from livehost.memory import ViewerMemoryStore
from livehost.registry import LivehostSession, livehost_registry
```

Add a lazy, path-aware accessor right after the module-level constants (after `_UPSTREAM_RECONNECT_MAX_ATTEMPTS = 5`, before `def _mention_keywords():`):

```python
_memory_store: ViewerMemoryStore | None = None
_memory_store_path: str | None = None


def _get_memory_store() -> ViewerMemoryStore:
    """Lazily (re)build the module-level ViewerMemoryStore, rebuilding it
    whenever settings.memory_db_path has changed since the last call. Reads
    the path fresh on every call, the same pattern this module already uses
    for settings.gateway_url (see Upstream(...) below) -- lets tests
    monkeypatch the path before opening a WS connection and get an isolated
    store, instead of every test sharing one real on-disk database."""
    global _memory_store, _memory_store_path
    if _memory_store is None or _memory_store_path != settings.memory_db_path:
        _memory_store = ViewerMemoryStore(settings.memory_db_path, settings.memory_recent_comments)
        _memory_store_path = settings.memory_db_path
    return _memory_store
```

In `livehost_stream`, add the two new query-derived values right after the existing `batch_wait_seconds` block (after the `try/except ValueError` for it, before `scheduler = EventScheduler(...)`):

```python
    memory_id = q.get("memory_id") or ""
    # "" (never None) is the anonymous-mode collapse point -- same
    # convention as owner_key below and LivehostSession.user_id elsewhere:
    # every unauthenticated caller shares one owner scope.
    owner_key = user_id or ""
```

Immediately after the successful `livehost_registry.claim(session_id, session)` check (right after its `if not ...: ... return` block, before the `upstream = Upstream(...)` construction), add one line to opportunistically prune old memory once per new session:

```python
    _get_memory_store().cleanup(settings.memory_retention_days)
```

Finally, update `_drain_social` to record every event into memory (before the skip-share filter, so likes/shares are still remembered even when not spoken aloud) and stamp the note onto the event:

```python
    async def _drain_social() -> None:
        while True:
            event = await raw_social_queue.get()
            # Recorded unconditionally -- memory is a viewer profile,
            # independent of whether skip_like_share keeps this particular
            # event out of the spoken queue below.
            event.viewer_note = _get_memory_store().note_and_record(owner_key, memory_id, event)
            # A room's like/share volume is not treated as "activity"
            # either (no note_activity() call on this branch) -- silent
            # likes must not keep poll_idle from firing when nothing else
            # is happening.
            if _skip_social_event(event, skip_like_share):
                continue
            scheduler.enqueue(event)
            # A comment that arrives but hasn't fired a turn yet (still
            # batching, or the streamer is mid-turn) is still evidence the
            # room is live -- must not let poll_idle fire underneath it.
            relay.note_activity()
            # Starts the batch-wait clock on the FIRST event of a fresh
            # window -- see Relay.note_pending's own docstring for why it
            # must not reset on every subsequent event in the same batch.
            relay.note_pending()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_ws_social.py -v`
Expected: PASS (all tests in the file, including the new one — confirm none of the pre-existing tests regressed).

- [ ] **Step 5: Lint and commit**

```bash
ruff check src/livehost/api/ws.py
git add src/livehost/api/ws.py tests/test_ws_social.py
git commit -m "feat: record viewer history and attach memory notes in ws.py"
```

---

### Task 5: Browser — persistent `memory_id`

**Files:**
- Modify: `src/livehost/static/livehost.js`

**Interfaces:**
- Produces: `lhGetOrCreateMemoryId(): string` (module-local helper, not exported — only used within this file's connect flow).
- No automated test: this repo has no JS test harness (no `package.json`, no JS test runner). Verification is a manual browser check (Step 3 below) plus reading the diff.

- [ ] **Step 1: Add the helper**

In `src/livehost/static/livehost.js`, add after the `pluginFetch` function (before `export const lh = {`):

```javascript
// Persistent per-browser id for the backend's viewer-memory feature
// (livehost.memory.ViewerMemoryStore): generated once, reused forever, so
// repeated live sessions from this browser share the same "who's commented
// before" history under this streamer's account.
const LH_MEMORY_ID_KEY = "lh-memory-id";
function lhGetOrCreateMemoryId() {
  let id = localStorage.getItem(LH_MEMORY_ID_KEY);
  if (!id) {
    id = crypto.randomUUID();
    localStorage.setItem(LH_MEMORY_ID_KEY, id);
  }
  return id;
}
```

- [ ] **Step 2: Send it on connect**

In the same file, find the params-building block (`let params = \`session_id=${encodeURIComponent(lh.sessionId)}\`;` followed by `params += \`&sample_rate=${STREAM_SAMPLE_RATE}\`;`). Add one line right after the `sample_rate` line:

```javascript
  let params = `session_id=${encodeURIComponent(lh.sessionId)}`;
  params += `&sample_rate=${STREAM_SAMPLE_RATE}`;
  params += `&memory_id=${encodeURIComponent(lhGetOrCreateMemoryId())}`;
```

- [ ] **Step 3: Manually verify in a browser**

Run: `cd /Users/lugon/code/speech-text-transformer/servers/livehost-api && python -m livehost.cli` (or however the dev server is normally started — check `README.md` if unsure), open the `/ui` page, open the browser's Network tab, start a session, and confirm the `/v1/livehost/stream` WebSocket URL contains `memory_id=<some-uuid>`. Reload the page and start a second session; confirm the `memory_id` value is identical (persisted via `localStorage`).

- [ ] **Step 4: Commit**

```bash
git add src/livehost/static/livehost.js
git commit -m "feat: send a persistent browser memory_id on connect"
```

---

## Final check

- [ ] Run the full suite: `python -m pytest tests/ -v` — all tests pass.
- [ ] Run the linter across everything touched: `ruff check src tests`.
- [ ] Confirm `docs/superpowers/specs/2026-08-12-viewer-memory-design.md` and this plan stay in sync with what was actually built (update either if implementation deviated).
