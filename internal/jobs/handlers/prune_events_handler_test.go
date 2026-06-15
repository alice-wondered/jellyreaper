package handlers

import (
	"context"
	"testing"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/repo"
)

// appendHandlerTestEvent writes a single domain.Event into the store.
func appendHandlerTestEvent(t *testing.T, store interface{ repo.Repository }, eventID string, occurredAt time.Time) {
	t.Helper()
	if err := store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		return tx.AppendEvent(ctx, domain.Event{
			EventID:        eventID,
			FlowID:         "flow:test",
			ItemID:         "test:item",
			Type:           "test.event",
			Source:         "test",
			OccurredAt:     occurredAt,
			IdempotencyKey: eventID,
			Payload:        map[string]any{"id": eventID},
		})
	}); err != nil {
		t.Fatalf("appendHandlerTestEvent %s: %v", eventID, err)
	}
}

// mockEventPruner is a test double for EventPruner.
type mockEventPruner struct {
	callCount     int
	lastCtx       context.Context
	lastRetention time.Duration
	nToReturn     int
	errToReturn   error
}

func (m *mockEventPruner) PruneOldEvents(ctx context.Context, retention time.Duration) (int, error) {
	m.callCount++
	m.lastCtx = ctx
	m.lastRetention = retention
	return m.nToReturn, m.errToReturn
}

// ── PruneEventsHandler unit tests ─────────────────────────────────────────────

// Handler delegates to the EventPruner with the configured retention.
func TestPruneEventsHandler_DelegatesToPruner(t *testing.T) {
	retention := 90 * 24 * time.Hour
	pruner := &mockEventPruner{nToReturn: 42}
	h := NewPruneEventsHandler(pruner, retention, nil)

	job := domain.JobRecord{
		JobID:  "job:prune:events:2026-06-01",
		ItemID: "prune:events",
		Kind:   domain.JobKindPruneEvents,
	}
	if err := h.Handle(context.Background(), job); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if pruner.callCount != 1 {
		t.Errorf("expected 1 pruner call, got %d", pruner.callCount)
	}
	if pruner.lastRetention != retention {
		t.Errorf("expected retention %v, got %v", retention, pruner.lastRetention)
	}
}

// Handler returns nil and logs a skip when no pruner is configured.
func TestPruneEventsHandler_NoPruner_SkipsGracefully(t *testing.T) {
	h := NewPruneEventsHandler(nil, 90*24*time.Hour, nil)
	job := domain.JobRecord{
		JobID:  "job:prune:events:2026-06-01",
		ItemID: "prune:events",
		Kind:   domain.JobKindPruneEvents,
	}
	if err := h.Handle(context.Background(), job); err != nil {
		t.Fatalf("expected nil error when no pruner configured, got: %v", err)
	}
}

// Handler propagates pruner errors.
func TestPruneEventsHandler_PrunerError_Propagates(t *testing.T) {
	import_err := context.DeadlineExceeded
	pruner := &mockEventPruner{errToReturn: import_err}
	h := NewPruneEventsHandler(pruner, 90*24*time.Hour, nil)
	job := domain.JobRecord{
		JobID:  "job:prune:events:2026-06-01",
		ItemID: "prune:events",
		Kind:   domain.JobKindPruneEvents,
	}
	err := h.Handle(context.Background(), job)
	if err == nil {
		t.Fatal("expected error from pruner to propagate")
	}
}

// Handler Kind returns JobKindPruneEvents.
func TestPruneEventsHandler_Kind(t *testing.T) {
	h := NewPruneEventsHandler(nil, 90*24*time.Hour, nil)
	if h.Kind() != domain.JobKindPruneEvents {
		t.Errorf("expected kind %s, got %s", domain.JobKindPruneEvents, h.Kind())
	}
}

// Default retention is 90 days when an invalid (<=0) value is passed.
func TestPruneEventsHandler_ZeroRetention_UsesDefault(t *testing.T) {
	pruner := &mockEventPruner{nToReturn: 0}
	h := NewPruneEventsHandler(pruner, 0, nil)
	job := domain.JobRecord{JobID: "job:prune:1", ItemID: "prune:events", Kind: domain.JobKindPruneEvents}
	if err := h.Handle(context.Background(), job); err != nil {
		t.Fatalf("handle: %v", err)
	}
	expected := 90 * 24 * time.Hour
	if pruner.lastRetention != expected {
		t.Errorf("expected default retention %v, got %v", expected, pruner.lastRetention)
	}
}
