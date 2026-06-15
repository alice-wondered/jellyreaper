package app

import (
	"context"
	"testing"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/repo"
)

// appendTestEvent writes a single event with the given OccurredAt into the store.
func appendTestEvent(t *testing.T, store interface{ repo.Repository }, eventID string, occurredAt time.Time) {
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
		t.Fatalf("appendTestEvent %s: %v", eventID, err)
	}
}

// pruneEvents calls PruneEvents inside a WithTx and returns (count, error).
func pruneEvents(t *testing.T, store interface{ repo.Repository }, olderThan time.Time) (int, error) {
	t.Helper()
	var n int
	var pruneErr error
	if err := store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		n, pruneErr = tx.PruneEvents(ctx, olderThan)
		return pruneErr
	}); err != nil {
		return 0, err
	}
	return n, nil
}

// ── PruneEvents unit tests (bbolt store via TxRepository) ────────────────────

// 0-case: empty events bucket → prune returns 0.
func TestPruneEvents_Zero_EmptyBucket(t *testing.T) {
	store := newTestStore(t)
	cutoff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	n, err := pruneEvents(t, store, cutoff)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 pruned from empty bucket, got %d", n)
	}
}

// 1-case: single old event → deleted; single recent event → survives.
func TestPruneEvents_One_OldEventDeleted(t *testing.T) {
	store := newTestStore(t)
	cutoff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	old := cutoff.Add(-time.Hour) // one hour before cutoff → should be pruned

	appendTestEvent(t, store, "evt:old:1", old)

	n, err := pruneEvents(t, store, cutoff)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 pruned, got %d", n)
	}

	// Confirm the bucket is now empty.
	remaining := pruneAndCountAll(t, store)
	if remaining != 0 {
		t.Errorf("expected 0 events remaining, got %d", remaining)
	}
}

func TestPruneEvents_One_RecentEventSurvives(t *testing.T) {
	store := newTestStore(t)
	cutoff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	recent := cutoff.Add(time.Hour) // one hour after cutoff → must survive

	appendTestEvent(t, store, "evt:recent:1", recent)

	n, err := pruneEvents(t, store, cutoff)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 pruned (recent event must survive), got %d", n)
	}

	remaining := pruneAndCountAll(t, store)
	if remaining != 1 {
		t.Errorf("expected 1 event remaining (not pruned), got %d", remaining)
	}
}

// n-case: mix of old and recent events → only old ones deleted.
func TestPruneEvents_N_OnlyOldDeleted(t *testing.T) {
	store := newTestStore(t)
	cutoff := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	// 3 old events.
	for i := 0; i < 3; i++ {
		appendTestEvent(t, store, "evt:old:n:"+string(rune('a'+i)), cutoff.Add(-time.Duration(i+1)*time.Hour))
	}
	// 2 recent events.
	for i := 0; i < 2; i++ {
		appendTestEvent(t, store, "evt:recent:n:"+string(rune('a'+i)), cutoff.Add(time.Duration(i+1)*time.Hour))
	}

	n, err := pruneEvents(t, store, cutoff)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 3 {
		t.Errorf("expected 3 pruned, got %d", n)
	}

	remaining := pruneAndCountAll(t, store)
	if remaining != 2 {
		t.Errorf("expected 2 events remaining, got %d", remaining)
	}
}

// n+1-case: adding one more old event still gets pruned correctly.
func TestPruneEvents_NplusOne_AdditionalOldEventPruned(t *testing.T) {
	store := newTestStore(t)
	cutoff := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	const n = 3
	for i := 0; i < n; i++ {
		appendTestEvent(t, store, "evt:nplusone:"+string(rune('a'+i)), cutoff.Add(-time.Duration(i+1)*time.Hour))
	}
	pruned, err := pruneEvents(t, store, cutoff)
	if err != nil {
		t.Fatalf("first prune: %v", err)
	}
	if pruned != n {
		t.Errorf("expected %d pruned, got %d", n, pruned)
	}

	// Now seed one more old event and re-prune.
	appendTestEvent(t, store, "evt:nplusone:extra", cutoff.Add(-30*time.Minute))
	pruned2, err := pruneEvents(t, store, cutoff)
	if err != nil {
		t.Fatalf("second prune: %v", err)
	}
	if pruned2 != 1 {
		t.Errorf("expected 1 pruned on n+1 run, got %d", pruned2)
	}
}

// counterexample: an event exactly AT the cutoff must NOT be deleted (strictly before).
func TestPruneEvents_Counterexample_EventAtCutoffSurvives(t *testing.T) {
	store := newTestStore(t)
	cutoff := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	appendTestEvent(t, store, "evt:exact:cutoff", cutoff) // exactly at cutoff

	n, err := pruneEvents(t, store, cutoff)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 0 {
		t.Errorf("counterexample: event at cutoff must NOT be pruned, but %d were deleted", n)
	}
}

// ── Service.PruneOldEvents integration tests ──────────────────────────────────

// 0-case: empty store.
func TestService_PruneOldEvents_Zero_NoEvents(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	n, err := svc.PruneOldEvents(context.Background(), 90*24*time.Hour)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 pruned, got %d", n)
	}
}

// 1-case: one old event pruned.
func TestService_PruneOldEvents_One_OldEventPruned(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	retention := 90 * 24 * time.Hour
	cutoff := now.Add(-retention)
	appendTestEvent(t, store, "evt:svc:old:1", cutoff.Add(-time.Hour))

	n, err := svc.PruneOldEvents(context.Background(), retention)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 pruned, got %d", n)
	}
}

// counterexample: a recent event (within retention window) must NOT be pruned.
func TestService_PruneOldEvents_Counterexample_RecentEventSurvives(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	retention := 90 * 24 * time.Hour
	// An event that occurred 1 day ago is well within the 90-day window.
	appendTestEvent(t, store, "evt:svc:recent:1", now.Add(-24*time.Hour))

	n, err := svc.PruneOldEvents(context.Background(), retention)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 0 {
		t.Errorf("counterexample: recent event must survive 90-day retention, but %d were pruned", n)
	}
}

// error case: zero retention must return an error.
func TestService_PruneOldEvents_ZeroRetention_Errors(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	_, err := svc.PruneOldEvents(context.Background(), 0)
	if err == nil {
		t.Fatal("expected error for zero retention")
	}
}
