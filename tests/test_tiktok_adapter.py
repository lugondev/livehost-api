from types import SimpleNamespace

from livehost.ingest.tiktok import (
    avatar_url,
    map_comment,
    map_follow,
    map_gift,
    map_like,
    map_share,
)


def _user(unique_id="alice", nickname="Alice", avatar=None):
    thumb = SimpleNamespace(m_urls=[avatar]) if avatar else None
    return SimpleNamespace(unique_id=unique_id, nickname=nickname, avatar_thumb=thumb)


def test_avatar_url_returns_first_url_or_none():
    assert avatar_url(_user(avatar="http://x/a.png")) == "http://x/a.png"
    assert avatar_url(_user(avatar=None)) is None


def test_map_comment():
    event = SimpleNamespace(user=_user(), comment="hello there")
    social = map_comment(event)
    assert social.kind == "comment"
    assert social.user_id == "alice"
    assert social.user_name == "Alice"
    assert social.text == "hello there"


def test_map_gift_skips_ongoing_streak():
    streaking_event = SimpleNamespace(
        user=_user(),
        streaking=True,
        repeat_count=3,
        gift=SimpleNamespace(name="Rose", diamond_count=1),
    )
    assert map_gift(streaking_event) is None

    finished_event = SimpleNamespace(
        user=_user(),
        streaking=False,
        repeat_count=3,
        gift=SimpleNamespace(name="Rose", diamond_count=1),
    )
    social = map_gift(finished_event)
    assert social.kind == "gift"
    assert social.gift_name == "Rose"
    assert social.gift_value == 3


def test_map_like():
    event = SimpleNamespace(user=_user(), count=7)
    social = map_like(event)
    assert social.kind == "like"
    assert social.like_count == 7


def test_map_follow_and_share():
    event = SimpleNamespace(user=_user())
    assert map_follow(event).kind == "follow"
    assert map_share(event).kind == "share"
