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

# memory_id is caller-supplied (the browser's own localStorage value, sent
# verbatim as a WS query param) and lands straight in the primary key --
# bound it defensively so an authenticated caller cannot mint unbounded
# distinct buckets by sending an ever-growing string.
_MAX_MEMORY_ID_LENGTH = 64


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
        social event. Catches Exception broadly (not just sqlite3.Error):
        a corrupted recent_comments cell can raise json.JSONDecodeError from
        _upsert's json.loads, and that must degrade the same way a sqlite
        error does, or it kills the caller's _drain_social task silently."""
        memory_id = memory_id[:_MAX_MEMORY_ID_LENGTH]
        try:
            row = self._conn.execute(
                f"SELECT {_LOOKUP_COLUMNS} FROM viewers "
                "WHERE owner_key=? AND memory_id=? AND platform_user_id=?",
                (owner_key, memory_id, event.user_id),
            ).fetchone()
            note = _build_note(row)
            self._upsert(owner_key, memory_id, event, row)
            return note
        except Exception as exc:  # noqa: BLE001 - degrade to "no note", never raise
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

    def close(self) -> None:
        """Close the underlying sqlite3 connection. Callers that rebuild a
        ViewerMemoryStore (e.g. ws.py's _get_memory_store() when
        settings.memory_db_path changes) must call this on the old instance
        first, or its connection's file descriptor leaks."""
        self._conn.close()


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
