from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Where the gateway lives, and how this plugin authenticates to it.
    gateway_url: str = "http://127.0.0.1:8000"
    plugin_name: str = "livehost"
    plugin_secret: str = ""

    host: str = "0.0.0.0"
    port: int = 8091

    # Carried over verbatim from the gateway's core/settings.py.
    mention_keywords: str = ""
    individual_threshold: int = 3
    batch_top_k: int = 3
    queue_max_size: int = 200
    backoff_initial_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    offline_poll_interval_seconds: float = 30.0
    watchdog_idle_seconds: float = 300.0

    # Viewer memory (livehost.memory.ViewerMemoryStore): persists per-viewer
    # comment/like/share/follow/gift history across live sessions.
    memory_db_path: str = "livehost_memory.db"
    memory_recent_comments: int = 5
    memory_retention_days: int = 90

    class Config:
        env_prefix = "LIVEHOST_"


settings = Settings()
