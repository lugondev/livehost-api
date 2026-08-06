# livehost-api

The TikTok Live AI co-host, packaged as an out-of-process plugin for the API
gateway. It ingests a TikTok LIVE room's comments, gifts, likes, follows, and
shares, scores and schedules them against the streamer's live voice turns, and
formats the result into text a co-host can speak.

## Run it

```bash
pip install -e ".[dev]"

export LIVEHOST_GATEWAY_URL=http://127.0.0.1:8000
export LIVEHOST_PLUGIN_SECRET=pick-a-long-random-string

livehost doctor     # says what is missing, and nothing else
livehost serve      # refuses to start on a configuration doctor already failed
```

## Configuration

| | |
| --- | --- |
| `LIVEHOST_GATEWAY_URL` | Base URL of the gateway this plugin authenticates to and registers with |
| `LIVEHOST_PLUGIN_NAME` | Plugin identity presented to the gateway (default `livehost`) |
| `LIVEHOST_PLUGIN_SECRET` | Credential used to call the gateway's `POST /api/auth/introspect` |
| `LIVEHOST_HOST` / `LIVEHOST_PORT` | Where this service listens (default `0.0.0.0:8091`) |

The remaining `LIVEHOST_*` settings (`MENTION_KEYWORDS`, `INDIVIDUAL_THRESHOLD`,
`BATCH_TOP_K`, `QUEUE_MAX_SIZE`, `BACKOFF_INITIAL_SECONDS`,
`BACKOFF_MAX_SECONDS`, `OFFLINE_POLL_INTERVAL_SECONDS`,
`WATCHDOG_IDLE_SECONDS`) tune the event scheduler and TikTok ingestor and carry
the same defaults they had in the gateway.

## The gateway is a hard runtime dependency

This service does not stand on its own: it authenticates itself to the gateway
using `LIVEHOST_PLUGIN_SECRET`, and every voice/session capability it needs —
identity, quota, TTS, the LLM — is reached through the gateway's plugin
contract (`POST /api/auth/introspect`, `/v1/plugins`), not held locally. There
is no local fallback for a gateway that is unreachable; `livehost doctor` and
`livehost serve` both exist to fail loudly and immediately when that
connection isn't configured, rather than start into a broken state.

## Tests

```bash
pytest -v
```
