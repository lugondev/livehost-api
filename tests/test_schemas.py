from livehost.schemas import SocialEvent


def test_social_event_requires_kind_and_user():
    event = SocialEvent(
        id="e1", kind="comment", user_id="u1", user_name="Alice", text="hi", timestamp=1.0,
    )
    assert event.platform == "tiktok"
    assert event.gift_value is None


def test_social_event_rejects_unknown_kind():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        SocialEvent(id="e1", kind="not-a-kind", user_id="u1", user_name="Alice", timestamp=1.0)
