# Contributing

## Project Structure
- `cmd/jellyreaper/main.go`: composition root and process lifecycle.
- `internal/http`: transport layer (request validation + payload mapping).
- `internal/discord`: Discord protocol/service adapter.
- `internal/jellyfin`: Jellyfin payload and API adapters.
- `internal/app`: orchestration logic and flow transitions.
- `internal/scheduler`: wake/sleep loop and due-job leasing.
- `internal/worker`: job dispatch and retry/failure handling.
- `internal/jobs/handlers`: atomic job steps.
- `internal/repo`: repository interfaces.
- `internal/repo/bbolt`: storage implementation.
- `internal/domain`: core entities and enums.

## Architectural Rules
1. Keep storage abstraction at repository layer only.
2. Map external payloads to typed internal structures in HTTP adapters.
   - Use provider-documented field names and contracts.
   - Do not invent field shapes in app/service layers.
3. App service orchestrates domain transitions and scheduling.
4. Each job handler should perform one atomic step and schedule the next step.
5. Persist all state/job/event changes transactionally (`WithTx`).
6. Keep ingress and jobs idempotent (dedupe keys are required).
7. Prefer target-projection behavior (season/series/movie targets) over per-episode scheduling.

## Testing Expectations
- Unit tests for pure logic and helpers.
- Component tests for repo/scheduler/worker/handlers.
- Integration tests for ingress -> orchestration -> scheduler/worker outcomes.
- Run before submitting:
```bash
go test ./...
```

## OpenAPI Generation
- Keep service API contract in `api/openapi.yaml`.
- Keep external Jellyfin OpenAPI source in `api/external/jellyfin-openapi-stable.json`.
- Regenerate service types with:
```bash
go generate ./api
```

## Adding New Job Types
1. Add `JobKind` in `internal/domain/types.go`.
2. Add payload type in `internal/jobs/types.go`.
3. Implement handler in `internal/jobs/handlers`.
4. Register handler in `cmd/jellyreaper/main.go`.
5. Add tests for transition behavior.

## Adding New Ingress Fields
1. Update provider typed payload structure (`internal/discord` or `internal/jellyfin`).
2. Map in HTTP handler.
3. Keep app layer API stable where possible.
4. Add tests for expected and malformed payloads.
