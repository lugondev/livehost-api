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
