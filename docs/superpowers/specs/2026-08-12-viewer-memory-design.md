# Viewer memory — design

Date: 2026-08-12

## Problem

Right now every social event (comment/like/share/gift/follow) is treated in
isolation: `orchestrator.format_social_turn` has no idea whether `@an12` has
commented once or fifty times tonight, or ever liked/shared/followed/gifted
before. A real streamer remembers regulars and continues the conversation
with them ("chào bạn quay lại, lần trước bạn hỏi..."). This spec adds a
small persistent memory of per-viewer history that gets surfaced back into
the text sent to the LLM.

## Scope

- Persisted across live sessions (not just one connection), reusable across
  multiple streams by the same streamer.
- Scoped per streamer (`owner_key`, from the WS caller's `user_id`, same
  convention as `LivehostSession.user_id`) **and** per a browser-generated
  `memory_id`, so memory is opt-in-shared: same browser reconnecting always
  reuses the same bucket, without needing any server-side account/DB beyond
  this feature.
- Tracks, per `(owner_key, memory_id, platform_user_id)`: comment count,
  ever-liked/shared/followed flags, gift count + total value, and the last N
  comment texts (default N=5).
- Surfaced as a short note appended to that viewer's line when
  `format_social_turn` renders a turn — nothing else about scheduling
  priority changes.
- Data older than a retention window (default 90 days, unused) is deleted
  opportunistically on session start.

Out of scope: cross-owner shared memory, scheduler priority changes for
returning viewers, any UI for inspecting/editing memory, external DB
backends (Postgres/Redis).

## Storage

New module `livehost/memory.py`, one `ViewerMemoryStore` per process,
constructed once (module-level singleton, like `livehost_registry`) backed
by a SQLite file at `settings.memory_db_path` (default
`livehost_memory.db`, `LIVEHOST_MEMORY_DB_PATH` env var). `PRAGMA
journal_mode=WAL` for concurrent-friendly local writes. Plain synchronous
`sqlite3` calls made directly from the asyncio loop (single-row
upsert/select, sub-millisecond on a local file) — no `to_thread` wrapping;
revisit only if profiling shows it matters.

### Schema

```sql
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
    recent_comments TEXT NOT NULL DEFAULT '[]',  -- JSON array of {text, ts}, capped at N
    first_seen REAL NOT NULL,
    last_seen REAL NOT NULL,
    PRIMARY KEY (owner_key, memory_id, platform_user_id)
);
```

`owner_key` is `user_id or ""` — same anonymous-mode caveat as
`LivehostSessionRegistry.claim`: with auth disabled every caller collapses
into the same owner scope, so memory would mix across different anonymous
users. Documented, not solved (matches the existing precedent).

Timestamps are wall-clock (`time.time()`), not `time.monotonic()`, because
they must survive a process restart.

### `ViewerMemoryStore` API

```python
class ViewerMemoryStore:
    def __init__(self, db_path: str, recent_comments_limit: int = 5) -> None: ...

    def note_and_record(self, owner_key: str, memory_id: str, event: SocialEvent) -> str | None:
        """Look up the existing row (if any) for this viewer, build a note
        string from the state BEFORE this event, update the row for this
        event (increment counters / set flags / append comment text),  and
        return the note (None if there's nothing worth mentioning yet —
        first-ever interaction with no prior history)."""

    def cleanup(self, retention_days: int) -> int:
        """Delete rows whose last_seen is older than retention_days. Returns
        rows deleted. Called once per new WS session claim, not per event."""
```

Only `comment`, `like`, `share`, `follow`, `gift` event kinds touch a row
(these are the only `SocialEvent.kind` values, so no filtering needed
inside `note_and_record`).

## Note content

Built only from data that predates the current event. Order, comma-joined,
first applicable pieces included (skip anything at zero/unset):

1. `"đã bình luận N lần"` if prior `comment_count > 0`
2. `"từng thả tim"` / `"từng chia sẻ live"` / `"đã follow"` — whichever of
   `liked`/`shared`/`followed` were already true
3. `"từng tặng quà (V coin)"` if prior `gift_value_total > 0`
4. `'lần trước nói: "…"'` — the most recent entry in prior
   `recent_comments`, truncated to ~80 chars

Returns `None` (no note) if none of the above apply — a first-time viewer
gets no parenthetical, matching how a streamer doesn't invent history for a
stranger.

## Wiring

### `schemas.py`

Add `viewer_note: str | None = None` to `SocialEvent`. Ingestors never set
it (stays `None` from `tiktok.py`); it is filled in by `ws.py` after ingest,
before the event reaches the scheduler.

### `ws.py`

- Read `memory_id = q.get("memory_id") or ""` alongside the other query
  params.
- `owner_key = user_id or ""` (reuses the `user_id` already resolved from
  `introspect`).
- In `_drain_social()`, **before** the `_skip_social_event` check (so
  like/share history is recorded even when `skip_like_share=1` keeps them
  out of the spoken queue — memory is a viewer profile, independent of
  whether that event was read aloud):
  ```python
  event.viewer_note = memory_store.note_and_record(owner_key, memory_id, event)
  ```
- After a session is claimed (`livehost_registry.claim(...)` succeeds),
  call `memory_store.cleanup(settings.memory_retention_days)` once.

### `orchestrator.py`

`format_social_turn` gains a small helper to build the `@user_name` suffix:

```python
def _note_suffix(event: SocialEvent) -> str:
    return f" ({event.viewer_note})" if event.viewer_note else ""
```

Applied uniformly in four of the five `event.kind` branches --
`comment`/`gift`/`follow`/`share`. `like` is deliberately excluded: its
line (`"[TikTok] {like_count} new likes"`) never names an individual
viewer, so there is no `@user_name` position to attach a per-viewer note
to. e.g.:

```python
lines.append(f"[TikTok @{event.user_name}]{_note_suffix(event)}: {event.text}")
```

### `settings.py`

```python
memory_db_path: str = "livehost_memory.db"
memory_recent_comments: int = 5
memory_retention_days: int = 90
```

All under the existing `LIVEHOST_` env prefix.

### Browser (`livehost.js` / `index.html`)

Lazily, on first connect (not eagerly on page load): if
`localStorage.getItem("lh-memory-id")` is unset, generate one
(`crypto.randomUUID()`) and store it. Always include `memory_id=<value>`
as a query param when opening the `/v1/livehost/stream` WebSocket. No new
UI control.

## Error handling

`ViewerMemoryStore` methods must not raise into the hot path: a SQLite
error (locked file, disk issue) degrades to "no note" / "cleanup skipped"
with a logged warning, not a dropped social event. Mirrors the
teardown-must-not-raise convention already used throughout `ws.py`.

## Testing

- `tests/test_memory.py` (new): `ViewerMemoryStore` against a temp SQLite
  file — first-time viewer gets no note, returning commenter gets an
  accumulating note, like/share/follow/gift flags surface correctly,
  `recent_comments` caps at N and drops oldest, `cleanup` removes rows past
  the retention window and leaves recent ones.
- `tests/test_orchestrator.py`: `format_social_turn` includes the
  `(note)` suffix when `viewer_note` is set, omits it when `None`.
- `tests/test_ws_social.py`: an end-to-end case where the same
  `platform_user_id` comments twice across two events confirms the second
  turn's text contains a note referencing the first.
