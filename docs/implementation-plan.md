# JellyReaper Implementation Plan

## Goals
- Build a Go-based media reaper/gardener for Jellyfin with Discord HITL control.
- Use durable local storage (`bbolt`) behind a repository interface to allow future backend swaps.
- Treat scheduling as first-class: durable jobs + in-memory min-heap + sleep-until-next-due timer.
- Keep handlers idempotent and atomic.

## Final Architecture
- **HTTP ingress** (`net/http`)
  - `POST /webhooks/jellyfin`
  - `POST /discord/interactions`
  - `GET /healthz`
  - optional: `POST /admin/resync`
- **Scheduler**
  - In-memory min-heap of upcoming jobs.
  - Single timer sleeps until the next due job.
  - Wake channel interrupts when an earlier job is enqueued.
- **Worker dispatcher**
  - Generic job dispatch by `kind`.
  - Registry of handlers (`map[JobKind]JobHandler`).
- **Reconciler**
  - Startup catch-up and periodic Jellyfin reconciliation.
  - Webhooks + backfill overlap window + dedupe.
- **Storage**
  - bbolt is source of truth.
  - Repository interface isolates persistence implementation.

## Durable Data Model
- `media`: canonical media metadata and play stats.
- `flows`: lifecycle state per item (`state`, `version`, policy snapshot).
- `events`: append-only domain event log.
- `jobs`: executable job records with scheduling/retry fields.
- `due_index`: sorted key `run_at_unix_nano|job_id` for next-due scanning.
- `dedupe`: idempotency keys from webhooks/interactions/jobs.
- `meta`: checkpoints (e.g. `last_ingested_at`, schema version).

## State Machine
- States: `active`, `pending_review`, `delete_queued`, `delete_in_progress`, `deleted`, `archived`, `delete_failed`.
- HITL is the only stop/redirect window before deletion.
- Grace behavior is represented by delayed jobs, not special scheduler queries.

## Job Kinds (v1)
- `evaluate_policy`
- `send_hitl_prompt`
- `hitl_timeout`
- `execute_delete`
- `verify_delete`
- `reconcile_item` (optional utility)

## Discord Interactions Model
- Use interactions endpoint mode over HTTP.
- Verify request signature headers.
- Use buttons for HITL outcomes (`Archive`, `Delay`, `Keep`, `Delete`).
- Encode context in `custom_id` (`jr:v1:<action>:<flowID>:<version>`).
- Handle stale clicks via flow version checks.

## Atomicity and Idempotency Rules
- Every external trigger uses idempotency keys.
- Each transition runs in one transaction:
  - check preconditions
  - update flow
  - append event
  - mutate/schedule jobs
  - write dedupe marker
- Stale/out-of-order jobs become safe no-ops.

## Delivery Phases
1. Scaffold module and package layout.
2. Repository interface + bbolt implementation.
3. Job model + scheduler + handler registry.
4. HTTP server and ingress routes.
5. Discord interaction handlers and message composition.
6. Jellyfin webhook/reconciliation integration.
7. Hardening: retries, observability, tests.
