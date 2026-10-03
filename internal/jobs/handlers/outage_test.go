package handlers

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/jobs"
	"jellyreaper/internal/repo"
	"jellyreaper/internal/scheduler"
)

// Counterexample pair: the same expired HITL timeout deletes without an
// outage shift and re-defers with one.
func TestHITLTimeoutAfterOutageShiftRedefersInsteadOfDeleting(t *testing.T) {
	for _, tc := range []struct {
		name      string
		shift     bool
		wantState domain.FlowState
	}{
		{"no shift deletes", false, domain.FlowStateDeleteQueued},
		{"shift re-defers", true, domain.FlowStatePendingReview},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := testStore(t)
			now := time.Now().UTC()
			payload, _ := json.Marshal(jobs.HITLTimeoutPayload{DefaultAction: "delete", FlowVersion: 4})
			if err := store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
				return tx.UpsertFlowCAS(ctx, domain.Flow{
					FlowID: "flow:target:movie:m1", ItemID: "target:movie:m1", SubjectType: "movie",
					State: domain.FlowStatePendingReview, Version: 4,
					DecisionDeadlineAt: now.Add(-time.Hour), // lapsed while jellyreaper was down
				}, 0)
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			fm := scheduler.NewFlowManager(store, nil, nil)
			if tc.shift {
				if _, err := fm.ExtendDecisionDeadlines(context.Background(), 30*24*time.Hour, 24*time.Hour); err != nil {
					t.Fatalf("extend: %v", err)
				}
			}
			h := NewHITLTimeoutHandler(store, nil, nil)
			h.SetFlowManager(fm)
			if err := h.Handle(context.Background(), domain.JobRecord{JobID: "job:timeout:old", ItemID: "target:movie:m1", Kind: domain.JobKindHITLTimeout, PayloadJSON: payload}); err != nil {
				t.Fatalf("handle: %v", err)
			}

			_ = store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
				flow, _, _ := tx.GetFlow(ctx, "target:movie:m1")
				if flow.State != tc.wantState {
					t.Fatalf("state %s, want %s", flow.State, tc.wantState)
				}
				return nil
			})
			if tc.shift {
				next, ok, _ := store.GetNextDueAt(context.Background())
				if !ok || next.Before(now.Add(29*24*time.Hour)) {
					t.Fatalf("want a re-deferred timeout ~30d out, got %v ok=%v", next, ok)
				}
			}
		})
	}
}
