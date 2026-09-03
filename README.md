# livehost-api

The TikTok Live AI co-host, packaged as an out-of-process plugin for the API
gateway. It ingests a TikTok LIVE room's comments, gifts, likes, follows, and
shares, scores and schedules them against the streamer's live voice turns, and
formats the result into text a co-host can speak. It has no local fallback
for the gateway: identity, quota, TTS, and the LLM are all reached through the
gateway's plugin contract, never held locally.

## Run it

```bash
pip install -e ".[dev]"

export LIVEHOST_GATEWAY_URL=http://127.0.0.1:8000
export LIVEHOST_PLUGIN_SECRET=pick-a-long-random-string   # must match registration, below

livehost doctor     # says what is missing, and nothing else
livehost serve      # refuses to start on a configuration doctor already failed
```

Or with Docker:

```bash
cp .env.example .env   # fill in LIVEHOST_GATEWAY_URL / LIVEHOST_PLUGIN_SECRET
docker compose up -d
```

## Registering with the gateway

The gateway has to know this plugin exists before any browser can reach it.
As a gateway admin (cookie session), register it once:

```bash
curl -X POST http://127.0.0.1:8000/v1/plugins \
  -H "Content-Type: application/json" \
  -b admin-session-cookies.txt \
  -d '{
    "name": "livehost",
    "url": "http://127.0.0.1:8091",
    "secret": "the-same-string-as-LIVEHOST_PLUGIN_SECRET",
    "kind": "feature",
    "mounts": [{"path": "/v1/livehost/stream", "kind": "ws", "public": true}]
  }'
```

`name` must match `LIVEHOST_PLUGIN_NAME` (default `livehost`), and `secret`
must be the exact value of this service's own `LIVEHOST_PLUGIN_SECRET` --
that shared string is what `POST /api/auth/introspect` authenticates the
plugin's own backend with, server-to-server, whenever it needs to resolve a
browser's ticket into a user id. The gateway's admin UI's plugin nav opens
`GET /ui` on this service with `?gateway=&token=` already attached, minting a
short-lived ticket for the browser first -- see `src/livehost/static/`.

## Using the web UI

`GET /ui` (served by this plugin, cross-origin from the gateway) is a single
page covering both the voice co-host and the TikTok ingestion side:

- **LLM Profile / TTS Profile** -- which gateway profile drives the LLM
  (and, unless overridden, that profile's own linked TTS voice too).
- **Persona** -- free text replacing the profile's `system_prompt` for this
  session only. Left blank (and no profile picked), it falls back to a
  built-in TikTok-host persona: the AI plays the actual channel owner, reacts
  to `[TikTok @user]: ...` lines by name, and avoids AI disclaimers/hedging.
  Picking a profile that has its own `system_prompt` (e.g. a profile with a
  custom character) uses that instead, matching how profile selection works
  in the gateway's regular chat UI. An explicitly typed persona always wins
  over both.
- **Tông giọng (tone)** -- nghiêm túc / vui vẻ / cợt nhả, appended after
  whichever persona is in play.
- **Độ dài trả lời (reply length)** -- constrain replies to 1 / 1-2 / 2-3
  sentences.
- **Tự nói khi im lặng (idle auto-topic)** -- after N seconds with no
  comment, gift, follow, or voice turn, the co-host raises its own topic
  instead of sitting in silence.
- **Bỏ qua like/share** -- like/share events never enter the scheduler at
  all (not spoken, not counted as context) when checked. Comments/gifts/
  follows are unaffected.
- **Đợi tối thiểu (comment) / Thời gian chờ tối đa (batching)** -- instead of
  replying to every comment the instant it lands, wait for 2-3 to pile up OR
  a max wait (5/10/20/30s), whichever comes first, then answer once.

All of the above persist per-browser via `localStorage`, and are plumbed
through as `?system_prompt=`, `?idle_topic_seconds=`, `?skip_like_share=1`,
`?batch_min_events=`, `?batch_wait_seconds=` on the `/v1/livehost/stream`
WebSocket -- see `src/livehost/api/ws.py` and `src/livehost/relay.py`.

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
the same defaults they had in the gateway. See `.env.example` for the full
list with one-line descriptions.

Viewer memory (persistent per-viewer comment/like/share/follow/gift history,
see `src/livehost/memory.py`) is tuned by three more: `LIVEHOST_MEMORY_DB_PATH`
(the SQLite file's path -- in `docker-compose.yml` this points at a named
volume so it survives container recreates), `LIVEHOST_MEMORY_RECENT_COMMENTS`
(how many of a viewer's recent comments to keep), and
`LIVEHOST_MEMORY_RETENTION_DAYS` (how long an inactive viewer's history is
kept before it's purged).

## The gateway is a hard runtime dependency

This service does not stand on its own: it authenticates itself to the gateway
using `LIVEHOST_PLUGIN_SECRET`, and every voice/session capability it needs --
identity, quota, TTS, the LLM -- is reached through the gateway's plugin
contract (`POST /api/auth/introspect`, `/v1/plugins`), not held locally. There
is no local fallback for a gateway that is unreachable; `livehost doctor` and
`livehost serve` both exist to fail loudly and immediately when that
connection isn't configured, rather than start into a broken state.

## Tests

```bash
pytest -v
```

---

## Part of LUGO

**LUGO** is a self-hosted AI companion platform — models supply the intelligence, LUGO
supplies the experience: one assistant that talks, remembers and acts across the browser,
ESP32 boards and a Raspberry Pi.

This repository is one piece of it. Every client and service talks to the gateway:

| Repo | Role |
| --- | --- |
| [lugo-gateway](https://github.com/lugondev/lugo-gateway) | The hub — STT/TTS/LLM engines, auth, device pairing, MCP tools, per-user chat memory. Everything below talks to this. |
| [lugo-web-client](https://github.com/lugondev/lugo-web-client) | React + TypeScript web client: talk, devices, history, tools. |
| [esp32-assistant](https://github.com/lugondev/esp32-assistant) | ESP-IDF firmware for ESP32-S3 / ESP32-C3 — a hands-free voice terminal. |
| [rpi-assistant](https://github.com/lugondev/rpi-assistant) | Raspberry Pi voice client (mic capture, Opus duplex, systemd unit). |
| [knowledge-api](https://github.com/lugondev/knowledge-api) | **kbase** — RAG knowledge base: documents in, retrievable chunks out. |
| [router-memory-services](https://github.com/lugondev/router-memory-services) | **memgw** — one API in front of any AI memory provider (Mem0, Zep, pgvector). |
| [mcp-basic-tools](https://github.com/lugondev/mcp-basic-tools) | Remote MCP tool server (timedate, fetch, ipinfo, web search). |
| **livehost-api** &nbsp;&larr; you are here | TikTok Live AI co-host, an out-of-process gateway plugin. |
| [voiceprint-api](https://github.com/lugondev/voiceprint-api) | Speaker recognition (3D-Speaker), forked from [xinnan-tech/voiceprint-api](https://github.com/xinnan-tech/voiceprint-api). |
| lugo-landing | Marketing landing page (bilingual VI/EN). **Private** — a recursive clone will skip it. |
