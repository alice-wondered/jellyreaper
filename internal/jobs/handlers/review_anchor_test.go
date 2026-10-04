package handlers

import (
	"context"
	"testing"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/repo"
)

// Each case is one of the two reported bugs plus a control that must still
// be reviewed. Before ReviewAnchor, both bug cases went straight to
// pending_review (epoch fallback / restored play).
func TestEvaluatePolicyNeverReviewsBeforeItemEnteredLibrary(t *testing.T) {
	day := 24 * time.Hour
	for _, tc := range []struct {
		name      string
		itemID    string
		flowAge   time.Duration
		media     []domain.MediaItem
		wantState domain.FlowState
	}{
		{
			name:      "new season, episodes not indexed yet",
			itemID:    "target:season:s-new",
			flowAge:   time.Minute,
			wantState: domain.FlowStateActive,
		},
		{
			name:    "re-added movie carrying a 586d-old restored play",
			itemID:  "target:movie:m-readd",
			flowAge: time.Minute,
			media: []domain.MediaItem{{ItemID: "m-readd", ItemType: "Movie",
				LastPlayedAt: time.Now().Add(-586 * day), CreatedAt: time.Now().Add(-time.Minute)}},
			wantState: domain.FlowStateActive,
		},
		{
			name:    "control: played 40d ago, known 60d",
			itemID:  "target:movie:m-stale",
			flowAge: 60 * day,
			media: []domain.MediaItem{{ItemID: "m-stale", ItemType: "Movie",
				LastPlayedAt: time.Now().Add(-40 * day), CreatedAt: time.Now().Add(-60 * day)}},
			wantState: domain.FlowStatePendingReview,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := testStore(t)
			now := time.Now().UTC()
			if err := store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
				for _, m := range tc.media {
					if err := tx.UpsertMedia(ctx, m); err != nil {
						return err
					}
				}
				return tx.UpsertFlowCAS(ctx, domain.Flow{
					FlowID: "flow:" + tc.itemID, ItemID: tc.itemID, State: domain.FlowStateActive,
					PolicySnapshot: domain.PolicySnapshot{ExpireAfterDays: 30, HITLTimeoutHrs: 48, TimeoutAction: "delete"},
					CreatedAt:      now.Add(-tc.flowAge),
				}, 0)
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			if err := NewEvaluatePolicyHandler(store, nil).Handle(context.Background(), domain.JobRecord{JobID: "job:eval", ItemID: tc.itemID}); err != nil {
				t.Fatalf("evaluate: %v", err)
			}

			_ = store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
				flow, _, _ := tx.GetFlow(ctx, tc.itemID)
				if flow.State != tc.wantState {
					t.Fatalf("state %s, want %s", flow.State, tc.wantState)
				}
				if tc.wantState == domain.FlowStateActive {
					want := now.Add(-tc.flowAge).Add(30 * day)
					if d := flow.NextActionAt.Sub(want); d < -time.Minute || d > time.Minute {
						t.Fatalf("due %v, want flow creation + 30d (%v)", flow.NextActionAt, want)
					}
				}
				return nil
			})
		})
	}
}
