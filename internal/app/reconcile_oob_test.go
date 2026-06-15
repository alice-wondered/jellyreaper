package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/repo"
)

// mockChecker is a test double for itemExistenceChecker. It returns the
// provided present set for every call. If errToReturn is non-nil it is
// returned instead.
type mockChecker struct {
	present     map[string]bool
	errToReturn error
	callCount   int
	lastItemIDs []string
}

func (m *mockChecker) CheckItemsExist(_ context.Context, itemIDs []string) (map[string]bool, error) {
	m.callCount++
	m.lastItemIDs = itemIDs
	if m.errToReturn != nil {
		return nil, m.errToReturn
	}
	return m.present, nil
}

// seedFlow creates a flow in the store using the canonical target key format.
func seedFlowOOB(t *testing.T, store interface {
	repo.Repository
}, itemID string, state domain.FlowState, now time.Time) {
	t.Helper()
	err := store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:         "flow:" + itemID,
			ItemID:         itemID,
			SubjectType:    inferSubjectType(itemID),
			DisplayName:    "Test " + itemID,
			State:          state,
			Version:        0,
			PolicySnapshot: domain.PolicySnapshot{ExpireAfterDays: 30, HITLTimeoutHrs: 48, TimeoutAction: "delete"},
			CreatedAt:      now,
			UpdatedAt:      now,
		}, 0)
	})
	if t != nil && err != nil {
		t.Fatalf("seed flow %s: %v", itemID, err)
	}
}

// seedMediaOOB creates a MediaItem record in the store.
func seedMediaOOB(t *testing.T, store interface {
	repo.Repository
}, itemID string, now time.Time) {
	t.Helper()
	err := store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		return tx.UpsertMedia(context.Background(), domain.MediaItem{
			ItemID:    itemID,
			Name:      "Media " + itemID,
			CreatedAt: now,
			UpdatedAt: now,
		})
	})
	if err != nil {
		t.Fatalf("seed media %s: %v", itemID, err)
	}
}

// flowExists returns true if a flow record with the given itemID exists.
func flowExists(t *testing.T, store interface {
	repo.Repository
}, itemID string) bool {
	t.Helper()
	found := false
	if err := store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		_, f, err := tx.GetFlow(context.Background(), itemID)
		found = f
		return err
	}); err != nil {
		t.Fatalf("check flow %s: %v", itemID, err)
	}
	return found
}

// mediaExists returns true if a media record with the given itemID exists.
func mediaExists(t *testing.T, store interface {
	repo.Repository
}, itemID string) bool {
	t.Helper()
	found := false
	if err := store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		_, f, err := tx.GetMedia(context.Background(), itemID)
		found = f
		return err
	}); err != nil {
		t.Fatalf("check media %s: %v", itemID, err)
	}
	return found
}

// pendingJobsForItem returns all leased/pending jobs for a given ItemID.
func pendingJobsForItem(t *testing.T, store interface {
	repo.Repository
	LeaseDueJobs(context.Context, time.Time, int, string, time.Duration) ([]domain.JobRecord, error)
}, itemID string, now time.Time) []domain.JobRecord {
	t.Helper()
	jobs, err := store.LeaseDueJobs(context.Background(), now.Add(365*24*time.Hour), 100, "test-probe", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	var out []domain.JobRecord
	for _, j := range jobs {
		if j.ItemID == itemID {
			out = append(out, j)
		}
	}
	return out
}

// ── 0-case: nothing stale ────────────────────────────────────────────────────

func TestReconcileOOB_Zero_NothingStale(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	// All items are live.
	itemID := "target:movie:aaaabbbbccccdddd"
	rawID := "aaaabbbbccccdddd"
	seedFlowOOB(t, store, itemID, domain.FlowStateActive, now)

	checker := &mockChecker{present: map[string]bool{rawID: true}}
	pruned, err := svc.ReconcileOOBDeletionsWithChecker(context.Background(), checker)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if pruned != 0 {
		t.Errorf("expected 0 pruned, got %d", pruned)
	}
	if !flowExists(t, store, itemID) {
		t.Error("live item flow should not have been pruned")
	}
}

// ── 0-case: no flows at all ───────────────────────────────────────────────────

func TestReconcileOOB_Zero_NoFlows(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	checker := &mockChecker{present: map[string]bool{}}
	pruned, err := svc.ReconcileOOBDeletionsWithChecker(context.Background(), checker)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if pruned != 0 {
		t.Errorf("expected 0 pruned, got %d", pruned)
	}
	// checker should not have been called at all (no candidates)
	if checker.callCount != 0 {
		t.Errorf("expected checker not called with empty flow list, got %d calls", checker.callCount)
	}
}

// ── 1-case: single stale item ─────────────────────────────────────────────────

func TestReconcileOOB_One_StaleItem(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	itemID := "target:movie:deadbeef11223344"
	rawID := "deadbeef11223344"
	seedFlowOOB(t, store, itemID, domain.FlowStateActive, now)
	seedMediaOOB(t, store, rawID, now)

	// Jellyfin says the item no longer exists.
	checker := &mockChecker{present: map[string]bool{}}
	pruned, err := svc.ReconcileOOBDeletionsWithChecker(context.Background(), checker)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if pruned != 1 {
		t.Errorf("expected 1 pruned, got %d", pruned)
	}
	// Flow must be gone.
	if flowExists(t, store, itemID) {
		t.Error("stale flow should have been pruned")
	}
	// Media record must be gone.
	if mediaExists(t, store, rawID) {
		t.Error("stale media should have been pruned")
	}
}

// ── 1-case: stale item cleans up its pending jobs (no orphan jobs) ────────────

func TestReconcileOOB_One_StaleItemClearsJobs(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	itemID := "target:item:cafebabe00000001"
	seedFlowOOB(t, store, itemID, domain.FlowStateActive, now)

	// Seed a pending eval job for this item.
	err := store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		return tx.EnqueueJob(context.Background(), domain.JobRecord{
			JobID:       "job:eval:scheduled:" + itemID,
			FlowID:      "flow:" + itemID,
			ItemID:      itemID,
			Kind:        domain.JobKindEvaluatePolicy,
			Status:      domain.JobStatusPending,
			RunAt:       now.Add(24 * time.Hour),
			MaxAttempts: 5,
			CreatedAt:   now,
			UpdatedAt:   now,
		})
	})
	if err != nil {
		t.Fatalf("seed job: %v", err)
	}

	checker := &mockChecker{present: map[string]bool{}}
	if _, err := svc.ReconcileOOBDeletionsWithChecker(context.Background(), checker); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// No pending jobs should remain for this item.
	jobs := pendingJobsForItem(t, store, itemID, now)
	if len(jobs) != 0 {
		t.Errorf("expected no orphan jobs after prune, got %d", len(jobs))
	}
}

// ── n-case: multiple items, mixed live/stale ─────────────────────────────────

func TestReconcileOOB_N_MixedLiveAndStale(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	liveID := "target:movie:1111111111111111"
	stale1ID := "target:movie:2222222222222222"
	stale2ID := "target:season:3333333333333333"

	seedFlowOOB(t, store, liveID, domain.FlowStateActive, now)
	seedFlowOOB(t, store, stale1ID, domain.FlowStateActive, now)
	seedFlowOOB(t, store, stale2ID, domain.FlowStatePendingReview, now)

	// Only the live item is present in Jellyfin.
	checker := &mockChecker{present: map[string]bool{
		"1111111111111111": true,
	}}
	pruned, err := svc.ReconcileOOBDeletionsWithChecker(context.Background(), checker)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if pruned != 2 {
		t.Errorf("expected 2 pruned, got %d", pruned)
	}
	if !flowExists(t, store, liveID) {
		t.Error("live item flow should survive reconciliation")
	}
	if flowExists(t, store, stale1ID) {
		t.Error("stale movie flow should have been pruned")
	}
	if flowExists(t, store, stale2ID) {
		t.Error("stale season flow should have been pruned")
	}
}

// ── counterexample: live items are NOT pruned ─────────────────────────────────

func TestReconcileOOB_Counterexample_LiveItemNotPruned(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	const nItems = 5
	rawIDs := [nItems]string{
		"aaaaaaaaaaaaaaa1",
		"aaaaaaaaaaaaaaa2",
		"aaaaaaaaaaaaaaa3",
		"aaaaaaaaaaaaaaa4",
		"aaaaaaaaaaaaaaa5",
	}
	present := map[string]bool{}
	for _, id := range rawIDs {
		flowID := "target:movie:" + id
		seedFlowOOB(t, store, flowID, domain.FlowStateActive, now)
		present[id] = true
	}

	checker := &mockChecker{present: present}
	pruned, err := svc.ReconcileOOBDeletionsWithChecker(context.Background(), checker)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if pruned != 0 {
		t.Errorf("counterexample: expected 0 pruned when all items are live, got %d", pruned)
	}
	for _, id := range rawIDs {
		flowID := "target:movie:" + id
		if !flowExists(t, store, flowID) {
			t.Errorf("live item %s should not have been pruned", flowID)
		}
	}
}

// ── idempotency: running twice changes nothing the second time ────────────────

func TestReconcileOOB_Idempotent(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	itemID := "target:movie:fedcba9876543210"
	rawID := "fedcba9876543210"
	seedFlowOOB(t, store, itemID, domain.FlowStateActive, now)
	seedMediaOOB(t, store, rawID, now)

	checker := &mockChecker{present: map[string]bool{}}

	// First run — prunes.
	pruned1, err := svc.ReconcileOOBDeletionsWithChecker(context.Background(), checker)
	if err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if pruned1 != 1 {
		t.Errorf("expected 1 pruned on first run, got %d", pruned1)
	}

	// Second run — nothing left to prune, no error.
	pruned2, err := svc.ReconcileOOBDeletionsWithChecker(context.Background(), checker)
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if pruned2 != 0 {
		t.Errorf("expected 0 pruned on second run (idempotent), got %d", pruned2)
	}
}

// ── terminal-state flows are not touched ─────────────────────────────────────

func TestReconcileOOB_SkipsTerminalFlows(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	terminalCases := []struct {
		id    string
		state domain.FlowState
	}{
		{"target:movie:term000000000001", domain.FlowStateDeleteQueued},
		{"target:movie:term000000000002", domain.FlowStateDeleteFailed},
		{"target:movie:term000000000003", domain.FlowStateDeleted},
		{"target:movie:term000000000004", domain.FlowStateArchived},
	}
	for _, tc := range terminalCases {
		seedFlowOOB(t, store, tc.id, tc.state, now)
	}

	// Jellyfin says none of these exist (to confirm we skip them anyway).
	checker := &mockChecker{present: map[string]bool{}}
	pruned, err := svc.ReconcileOOBDeletionsWithChecker(context.Background(), checker)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if pruned != 0 {
		t.Errorf("expected 0 pruned for terminal flows, got %d", pruned)
	}
	// Checker should not have been called (no candidates after filtering).
	if checker.callCount != 0 {
		t.Errorf("expected checker not called when all flows are terminal, got %d calls", checker.callCount)
	}
	// All flow records still present (we didn't touch them).
	for _, tc := range terminalCases {
		if !flowExists(t, store, tc.id) {
			t.Errorf("terminal flow %s (%s) should not have been touched", tc.id, tc.state)
		}
	}
}

// ── deleted item does NOT reappear after reconciliation ──────────────────────
// Invariant: a pruned item must not be re-created by a later no-op reconcile.

func TestReconcileOOB_PrunedItemDoesNotReappear(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	itemID := "target:movie:0000000000000001"
	seedFlowOOB(t, store, itemID, domain.FlowStateActive, now)

	checker := &mockChecker{present: map[string]bool{}}

	// Prune it.
	if _, err := svc.ReconcileOOBDeletionsWithChecker(context.Background(), checker); err != nil {
		t.Fatalf("prune run: %v", err)
	}
	if flowExists(t, store, itemID) {
		t.Fatal("item should have been pruned")
	}

	// A subsequent no-op reconcile with still-absent item must not re-create it.
	if _, err := svc.ReconcileOOBDeletionsWithChecker(context.Background(), checker); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if flowExists(t, store, itemID) {
		t.Fatal("pruned item must not reappear after second reconcile run")
	}
}

// ── Jellyfin client error propagates ─────────────────────────────────────────

func TestReconcileOOB_CheckerError_Propagates(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	itemID := "target:movie:0000000000000002"
	seedFlowOOB(t, store, itemID, domain.FlowStateActive, now)

	wantErr := errors.New("jellyfin unreachable")
	checker := &mockChecker{errToReturn: wantErr}

	_, err := svc.ReconcileOOBDeletionsWithChecker(context.Background(), checker)
	if err == nil {
		t.Fatal("expected error from checker to propagate")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("expected wrapped checker error, got: %v", err)
	}
	// Flow should still be present (we didn't prune on error).
	if !flowExists(t, store, itemID) {
		t.Error("flow should not be pruned when checker errors")
	}
}

// ── no Jellyfin client configured: graceful skip ─────────────────────────────

func TestReconcileOOB_NoClientConfigured_GracefulSkip(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	// Do NOT call SetJellyfinClient.

	pruned, err := svc.ReconcileOOBDeletions(context.Background())
	if err != nil {
		t.Fatalf("expected no error when client not configured, got: %v", err)
	}
	if pruned != 0 {
		t.Errorf("expected 0 pruned when no client, got %d", pruned)
	}
}
