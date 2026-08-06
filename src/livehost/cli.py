"""livehost doctor | serve -- same contract kb uses: serve refuses to start on
a configuration doctor already failed."""

import sys

from livehost.settings import settings


def doctor() -> list[str]:
    """Return a list of problems. Empty means healthy."""
    problems = []
    if not settings.plugin_secret:
        problems.append("LIVEHOST_PLUGIN_SECRET is unset (needed to call /api/auth/introspect)")
    if not settings.gateway_url.startswith(("http://", "https://")):
        problems.append(f"LIVEHOST_GATEWAY_URL is not an http(s) url: {settings.gateway_url!r}")
    try:
        import TikTokLive  # noqa: F401
    except ImportError:
        problems.append("TikTokLive is not installed")
    return problems


def main() -> int:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "doctor"
    problems = doctor()
    if cmd == "doctor":
        for p in problems:
            print(f"FAIL  {p}")
        if not problems:
            print("OK    configuration is complete")
        return 1 if problems else 0
    if cmd == "serve":
        if problems:
            for p in problems:
                print(f"FAIL  {p}")
            print("refusing to serve on a failing doctor")
            return 1
        import uvicorn

        from livehost.app import app

        uvicorn.run(app, host=settings.host, port=settings.port)
        return 0
    print(f"unknown command {cmd!r}; expected doctor or serve")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
