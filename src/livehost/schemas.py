from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class SocialEvent(BaseModel):
    """A single normalized event from a social live-stream platform (comment,
    gift, like, follow, share). Platform-specific ingestors (TikTokLiveIngestor
    etc.) translate their native event shapes into this one."""

    id: str
    platform: Literal["tiktok"] = "tiktok"
    kind: Literal["comment", "gift", "like", "follow", "share"]
    user_id: str
    user_name: str
    user_avatar_url: str | None = None
    text: str | None = None
    gift_name: str | None = None
    gift_value: int | None = None
    like_count: int | None = None
    timestamp: float
