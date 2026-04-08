# JellyReaper

JellyReaper is a Go service that listens to Jellyfin and Discord webhooks, persists workflow state in bbolt, and runs a durable scheduler for HITL media lifecycle decisions.

## What It Does
- Ingests Jellyfin webhook events.
- Creates/updates target flows and schedules jobs.
- Sends Discord HITL prompts with buttons (`Archive`, `Delay`, `Keep`, `Delete`).
- Executes timeout/delete workflows via durable jobs.
- Recovers queue state from disk-backed storage.

## Architecture At A Glance
- HTTP ingress (`net/http`) with typed request mapping.
- App orchestration service (`internal/app`).
- Durable storage (`bbolt`) behind repository interfaces.
- Scheduler loop (own goroutine) + wake channel.
- Job dispatcher + handler registry.

## Requirements
- Go 1.24+
- Discord application configured for Interactions Endpoint.
- Jellyfin server API key.

## Configuration

| Variable | Required | Default | Description |
|---|---:|---|---|
| `HTTP_PORT` | no | `6767` | HTTP port when `HTTP_ADDR` is unset |
| `HTTP_ADDR` | no | `:${HTTP_PORT}` | HTTP bind address |
| `DB_PATH` | no | `./data/jellyreaper.db` | bbolt file path |
| `LOG_DIR` | no | `./logs` | application log directory |
| `EMBED_PERSIST_DIR` | no | `./embeds` | directory to persist downloaded embed images |
| `WORKER_ID` | no | hostname | scheduler lease owner id |
| `LEASE_TTL` | no | `30s` | job lease timeout |
| `DISCORD_PUBLIC_KEY_HEX` | yes | - | Discord app public key (hex) |
| `DISCORD_BOT_TOKEN` | yes (for sending HITL prompts) | - | bot token |
| `DISCORD_CHANNEL_ID` | yes (for prompt fallback) | - | default channel for HITL messages |
| `DEFAULT_DELAY_WINDOW` | no | `15d` | default delay duration for Delay actions |
| `DEFAULT_LAST_PLAYED_THRESHOLD_DAYS` | no | `60` | default stale threshold in days when flow policy is unset/new |
| `OPENAI_API_KEY` | no | - | enables @mention AI assistant in Discord |
| `OPENAI_MODEL` | no | `gpt-4o-mini` | model for mention assistant intent/tool routing |
| `JELLYFIN_URL` | no | derived from host/port | Jellyfin base URL |
| `JELLYFIN_HOST` | no | `localhost` | Jellyfin host when `JELLYFIN_URL` is unset |
| `JELLYFIN_PORT` | no | `8096` | Jellyfin port |
| `JELLYFIN_API_KEY` | yes | - | Jellyfin API key |
| `BACKFILL_ENABLED` | no | `true` | enables startup + periodic backfill |
| `BACKFILL_INTERVAL` | no | `15m` | periodic backfill interval |
| `BACKFILL_FULL_SWEEP_ON_STARTUP` | no | `true` | on first run (no checkpoint), sweep full Jellyfin history |
| `BACKFILL_PLAYBACK_ENABLED` | no | `false` | enables activity-log playback backfill; item snapshot user data is used by default |
| `BACKFILL_LOOKBACK` | no | `24h` | startup lookback if full sweep is disabled and no checkpoint exists |
| `BACKFILL_OVERLAP` | no | `2m` | overlap from last checkpoint to avoid missing events |
| `BACKFILL_LIMIT` | no | `500` | page size per Jellyfin backfill request (all pages are fetched) |
| `BACKFILL_WRITE_BATCH_SIZE` | no | `100` | number of webhook-style backfill writes per storage batch |
| `BACKFILL_WRITE_BATCH_TIMEOUT` | no | `500ms` | max wait before flushing a partial backfill write batch |
| `BACKFILL_WRITE_QUEUE_CAPACITY` | no | `2000` | buffered queue capacity feeding batched backfill writes |

## Run
```bash
go test ./...
go run ./cmd/jellyreaper
```

Service endpoints:
- `GET /healthz`
- `POST /webhooks/jellyfin`
- `POST /discord/interactions`

## Build
```bash
go build ./cmd/jellyreaper
```

## Docker
```bash
cp .env.example .env

# if you use a shared external docker network with Jellyfin
docker network create media_shared || true

docker compose -f docker-compose.example.yml up -d
```

If you want the Discord @mention assistant, set `OPENAI_API_KEY` in `.env`.

Mounted persistence paths in compose:
- `./data` -> `/data`
- `./logs` -> `/logs`
- `./embeds` -> `/embeds`

## OpenAPI And Provider Contracts
- Service spec lives at `api/openapi.yaml`.
- Official Jellyfin OpenAPI spec is vendored at `api/external/jellyfin-openapi-stable.json`.
- Generate API types with:
```bash
go generate ./api
```

### Provider Payload Sources
- Discord interactions use `discordgo.Interaction` decoding and signature validation (`discordgo.VerifyInteraction`) based on Discord's documented interactions contract.
- Jellyfin webhook ingress is mapped from documented Jellyfin webhook/plugin field names (`EventId`, `NotificationId`, `NotificationType`, `ItemId`, `UserId`, `ServerId`, `Name`).

### Target Projection Behavior
- TV episode events project to season targets.
- Scheduler/HITL runs on targets to reduce per-episode Discord noise.
- Aggregate target deletion resolves child items and deletes each supported media item.
- Discord prompts use human-readable names and include image embeds when available.
- On startup the bot announces online status and that backfill has started.

### Backfill Behavior
- Backfill is paginated and performs a full sweep from the checkpoint boundary (not just the first page).
- First startup defaults to a full historical sweep; later runs sweep from the saved checkpoint with overlap.
- Backfill reuses the same typed orchestration path as webhooks to keep flow/job behavior consistent.

### Discord Mention Assistant
- Enabled when `OPENAI_API_KEY` is set.
- Runs on Discord mention messages and responds in a thread (creates one when needed).
- Uses tool-calling with `auto` tool choice so the model can decide whether to call tools or respond directly.
- Includes discovery and control tools for fuzzy target search, target-state inspection, alias memory, and delete/archive scheduling.
- Supports projection-level delete scheduling (season/series targets) so series cleanup can fan out through normal service workflows.
- Receives serialized thread context each turn (selected target, pending action, candidate targets, known aliases).
- Context memory is in-process and LRU-capped by thread to avoid unbounded growth; cache misses can restore recent thread messages from Discord.
- Responses are designed to stay user-facing (Discord markdown, human-readable wording, no internal IDs).

## Notes On Provider Payloads
- Discord ingress is validated and decoded using `discordgo` payload semantics.
- Jellyfin ingress is mapped from known Jellyfin keys (`EventId`, `NotificationId`, `NotificationType`, `ItemId`, etc.) before orchestration.
- Internal orchestration consumes typed ingress structs (`discord.IncomingInteraction`, `jellyfin.WebhookEvent`).
