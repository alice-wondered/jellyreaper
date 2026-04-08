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
| `JELLYFIN_URL` | no | derived from host/port | Jellyfin base URL |
| `JELLYFIN_HOST` | no | `localhost` | Jellyfin host when `JELLYFIN_URL` is unset |
| `JELLYFIN_PORT` | no | `8096` | Jellyfin port |
| `JELLYFIN_API_KEY` | yes | - | Jellyfin API key |
| `BACKFILL_ENABLED` | no | `true` | enables startup + periodic backfill |
| `BACKFILL_INTERVAL` | no | `15m` | periodic backfill interval |
| `BACKFILL_LOOKBACK` | no | `24h` | startup lookback if no checkpoint exists |
| `BACKFILL_OVERLAP` | no | `2m` | overlap from last checkpoint to avoid missing events |
| `BACKFILL_LIMIT` | no | `500` | max records per Jellyfin backfill request |

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
docker compose -f docker-compose.example.yml up -d
```

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
- TV episode events project to season and series targets.
- Scheduler/HITL runs on targets to reduce per-episode Discord noise.
- Aggregate target deletion resolves child items and deletes each supported media item.
- Discord prompts use human-readable names and include image embeds when available.
- On startup the bot announces online status and that backfill has started.

## Notes On Provider Payloads
- Discord ingress is validated and decoded using `discordgo` payload semantics.
- Jellyfin ingress is mapped from known Jellyfin keys (`EventId`, `NotificationId`, `NotificationType`, `ItemId`, etc.) before orchestration.
- Internal orchestration consumes typed ingress structs (`discord.IncomingInteraction`, `jellyfin.WebhookEvent`).
