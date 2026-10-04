package scheduler

import (
	"context"
	"testing"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/repo"
)

func TestExtendDecisionDeadlines(t *testing.T) {
	store := newTestStore(t)
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	seed := []domain.Flow{
		{ItemID: "pending-long", State: domain.FlowStatePendingReview, DecisionDeadlineAt: now.Add(-24 * time.Hour), Version: 7},
		{ItemID: "pending-short", State: domain.FlowStatePendingReview, DecisionDeadlineAt: now.Add(-50 * time.Hour), Version: 3},
		{ItemID: "pending-nodeadline", State: domain.FlowStatePendingReview, Version: 1},
		{ItemID: "active", State: domain.FlowStateActive, NextActionAt: now.Add(-time.Hour), DecisionDeadlineAt: now.Add(-time.Hour), Version: 1},
	}
	if err := store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		for _, f := range seed {
			f.FlowID = "flow:" + f.ItemID
			if err := tx.UpsertFlowCAS(ctx, f, 0); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	m := NewFlowManager(store, nil, nil)
	m.SetNowFunc(func() time.Time { return now })
	n, err := m.ExtendDecisionDeadlines(context.Background(), 48*time.Hour, 6*time.Hour)
	if err != nil || n != 2 {
		t.Fatalf("want 2 flows extended, got %d err %v", n, err)
	}

	want := map[string]time.Time{
		"pending-long":       now.Add(24 * time.Hour), // -24h + 48h
		"pending-short":      now.Add(6 * time.Hour),  // -50h + 48h = -2h → floored at now+6h
		"pending-nodeadline": {},                      // no deadline, no timeout job to protect
		"active":             now.Add(-time.Hour),     // not pending_review: untouched
	}
	_ = store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		for _, s := range seed {
			got, _, _ := tx.GetFlow(ctx, s.ItemID)
			if !got.DecisionDeadlineAt.Equal(want[s.ItemID]) {
				t.Errorf("%s: deadline %v, want %v", s.ItemID, got.DecisionDeadlineAt, want[s.ItemID])
			}
			if got.Version != s.Version {
				t.Errorf("%s: version bumped %d→%d; queued timeout jobs would go stale", s.ItemID, s.Version, got.Version)
			}
		}
		return nil
	})
}
