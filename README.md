# JellyReaper

## 1) What This Project Is

JellyReaper is a media lifecycle management server for Jellyfin. It ingests Jellyfin activity/catalog events, projects media into operational targets, and runs a durable scheduler to drive cleanup decisions through Discord HITL (Human-In-The-Loop) prompts.

At a high level:
- It watches Jellyfin events and maintains durable flow state in bbolt.
- It sends Discord decision prompts (`Archive`, `Delay`, `Delete`) and executes queued actions safely.
- It supports backfill/reconciliation so startup and drift recovery are operationally reliable.
- It ships an optional Discord @mention AI assistant that can discover projection targets, inspect state, and submit domain-safe decisions.

AI compatibility notes:
- AI operations are projection-centric (`movie`, `season`) for scheduler/HITL, with higher-level intents mapped into those targets.
- AI uses domain services for decisions (queueing/scheduler consistency stays centralized).
- AI can update global defaults, delay a specific target by arbitrary days, and finalize active HITL prompts when it records a delay decision.

Core runtime pieces:
- HTTP ingress (`/webhooks/jellyfin`, `/discord/interactions`, `/healthz`)
- App orchestration service (`internal/app`)
- Durable storage + queue (`bbolt` via repository interfaces)
- Scheduler loop + worker dispatcher/job handlers
- Optional AI mention assistant

## 2) Setup Guide

### Prerequisites
- Go `1.24+` (for local run)
- A Jellyfin server + API key
- A Discord application with:
  - Interactions endpoint configured
  - Bot token (for gateway/messages)
  - Message content intent enabled (for @mention assistant)

### Recommended Security Setup

Do this before exposing to the internet:
- Keep JellyReaper behind a reverse proxy and only expose required routes.
- Require Discord signature verification (built-in via `DISCORD_PUBLIC_KEY_HEX`).
- Configure `JELLYFIN_WEBHOOK_TOKEN` and require it on `/webhooks/jellyfin`.
- Restrict source IP/network for Jellyfin webhook route when possible.

Generate a webhook token:

```bash
openssl rand -hex 32
```

Set this token:
- In JellyReaper env as `JELLYFIN_WEBHOOK_TOKEN`
- In Jellyfin webhook custom headers as one of:
  - `X-Jellyreaper-Token: <token>` (recommended)
  - `X-Webhook-Token: <token>`
  - `Authorization: Bearer <token>`

### Quick Start (Docker Compose)

```bash
cp .env.example .env

# if you use a shared external network with Jellyfin
docker network create media_shared || true

docker compose -f docker-compose.example.yml up -d
```

Embedded `docker-compose.example.yml`:

```yaml
services:
  jellyreaper:
    image: ghcr.io/alice-wondered/jellyreaper:latest
    pull_policy: always
    container_name: jellyreaper
    restart: unless-stopped
    ports:
      - "6767:6767"
    env_file:
      - .env
    environment:
      HTTP_PORT: "6767"
      HTTP_ADDR: ":6767"
      DB_PATH: /data/jellyreaper.db
      LOG_DIR: /logs
      EMBED_PERSIST_DIR: /embeds
      JELLYFIN_URL: http://jellyfin:8096
      JELLYFIN_WEBHOOK_TOKEN: ${JELLYFIN_WEBHOOK_TOKEN:-}
      # Optional: enable Discord @mention AI assistant
      OPENAI_MODEL: ${OPENAI_MODEL:-gpt-4o-mini}
    volumes:
      - ./data:/data
      - ./logs:/logs
      - ./embeds:/embeds
    networks:
      - media_shared

networks:
  media_shared:
    external: true
    name: ${JELLYREAPER_SHARED_NETWORK:-media_shared}
```

Persistence mounts:
- `./data -> /data`
- `./logs -> /logs`
- `./embeds -> /embeds`

### Local Run (Go)

```bash
go test ./...
go run ./cmd/jellyreaper
```

### Jellyfin Configuration Walkthrough

1. Create/get Jellyfin API key.
2. Set `JELLYFIN_URL` and `JELLYFIN_API_KEY` in `.env`.
3. Configure Jellyfin webhook/plugin to call:
   - `POST https://<your-host>/webhooks/jellyfin`
4. Add custom header token (recommended):
   - `X-Jellyreaper-Token: <JELLYFIN_WEBHOOK_TOKEN>`
5. Keep webhook route private/allowlisted if possible.

### Discord Configuration Walkthrough

1. Create Discord application and bot.
2. Set interactions endpoint to:
   - `POST https://<your-host>/discord/interactions`
3. Copy application public key (hex) to `DISCORD_PUBLIC_KEY_HEX`.
4. Set `DISCORD_BOT_TOKEN`.
5. Set `DISCORD_CHANNEL_ID` as fallback channel for HITL prompts.
6. Enable message content intent if using the AI mention assistant.

### Enabling AI Assistant

Set:
- `OPENAI_API_KEY`
- optional `OPENAI_MODEL` (default `gpt-4o-mini`)

Behavior:
- Responds to @mentions in Discord threads.
- Uses tool-calling for discovery/actions.
- Keeps per-thread context in-memory with bounded retention and restore-on-miss from Discord thread history.

## 3) Technical Knobs (All Configurable Controls)

### Core Runtime

| Variable | Required | Default | Description |
|---|---:|---|---|
| `HTTP_PORT` | no | `6767` | HTTP port when `HTTP_ADDR` is unset |
| `HTTP_ADDR` | no | `:${HTTP_PORT}` | HTTP bind address |
| `DB_PATH` | no | `./data/jellyreaper.db` | bbolt DB path |
| `LOG_DIR` | no | `./logs` | log directory |
| `EMBED_PERSIST_DIR` | no | `./embeds` | directory for persisted downloaded images |
| `WORKER_ID` | no | hostname | lease owner ID for scheduler/worker |
| `LEASE_TTL` | no | `30s` | job lease timeout |

### Discord + AI

| Variable | Required | Default | Description |
|---|---:|---|---|
| `DISCORD_PUBLIC_KEY_HEX` | yes | - | verifies Discord interaction signatures |
| `DISCORD_BOT_TOKEN` | yes (for sending messages) | - | bot token for gateway/message APIs |
| `DISCORD_CHANNEL_ID` | yes (recommended) | - | fallback channel for HITL prompt delivery |
| `OPENAI_API_KEY` | no | - | enables Discord @mention AI assistant |
| `OPENAI_MODEL` | no | `gpt-4o-mini` | OpenAI model for assistant |

### Jellyfin

| Variable | Required | Default | Description |
|---|---:|---|---|
| `JELLYFIN_URL` | no | from host/port | Jellyfin base URL |
| `JELLYFIN_HOST` | no | `localhost` | used when `JELLYFIN_URL` is unset |
| `JELLYFIN_PORT` | no | `8096` | used when `JELLYFIN_URL` is unset |
| `JELLYFIN_API_KEY` | yes | - | Jellyfin API key |
| `JELLYFIN_WEBHOOK_TOKEN` | no (strongly recommended) | - | required token for Jellyfin webhook auth |

### Policy Defaults

| Variable | Required | Default | Description |
|---|---:|---|---|
| `DEFAULT_DELAY_WINDOW` | no | `15d` | default delay window for Delay actions (supports `d` suffix, e.g. `15d`) |
| `DEFAULT_LAST_PLAYED_THRESHOLD_DAYS` | no | `60` | default review threshold when policy is unset/new |

Notes:
- Global AI policy updates are meta-based and lazy by design.
- They apply on next relevant action/evaluation, not by rewriting every existing flow.

### Backfill / Reconciliation

| Variable | Required | Default | Description |
|---|---:|---|---|
| `BACKFILL_ENABLED` | no | `true` | enables startup + periodic backfill |
| `BACKFILL_INTERVAL` | no | `15m` | periodic backfill interval |
| `BACKFILL_FULL_SWEEP_ON_STARTUP` | no | `true` | first-run full historical sweep |
| `BACKFILL_PLAYBACK_ENABLED` | no | `false` | include playback activity backfill stream |
| `BACKFILL_LOOKBACK` | no | `24h` | startup lookback when no checkpoint and full sweep disabled |
| `BACKFILL_OVERLAP` | no | `2m` | checkpoint overlap for event safety |
| `BACKFILL_LIMIT` | no | `500` | provider page size |
| `BACKFILL_WRITE_BATCH_SIZE` | no | `100` | storage write batch size |
| `BACKFILL_WRITE_BATCH_TIMEOUT` | no | `500ms` | flush timeout for partial backfill batches |
| `BACKFILL_WRITE_QUEUE_CAPACITY` | no | `2000` | queue capacity for batched backfill writes |

### Endpoints

- `GET /healthz`
- `POST /webhooks/jellyfin`
- `POST /discord/interactions`

### Provider Contract Notes

- Discord interactions are validated with Ed25519 signature verification.
- Jellyfin webhook payloads are mapped into typed internal events before orchestration.
- API/provider contracts are documented in `api/openapi.yaml` and `api/external/jellyfin-openapi-stable.json`.
