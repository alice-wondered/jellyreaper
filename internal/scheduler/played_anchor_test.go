package scheduler

import (
	"context"
	"testing"
	"time"

	"jellyreaper/internal/domain"
)

// The spam loop's precondition: a pending_review season whose episodes are not
// linked (no media rows) and whose CreatedAt is months old. Without PlayedAt
// the recovery schedules the next eval for "now", which re-opens review on
// every PlaybackProgress tick.
func TestPlayed_AnchorsOnTriggeringPlayWithoutLinkedMedia(t *testing.T) {
	store := newTestStore(t)
	mgr := NewFlowManager(store, nil, nil)
	playAt := time.Date(2026, 10, 5, 0, 1, 18, 0, time.UTC)
	mgr.SetNowFunc(func() time.Time { return playAt })

	f := baseFlow("target:season:readded", domain.FlowStatePendingReview)
	f.PolicySnapshot.ExpireAfterDays = 60
	f.CreatedAt = time.Date(2026, 5, 3, 18, 4, 46, 0, time.UTC)
	seedFlow(t, store, f)

	for _, tc := range []struct {
		name     string
		playedAt time.Time
		want     time.Time
	}{
		{"with triggering play", playAt, playAt.Add(60 * 24 * time.Hour)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := mgr.Played(context.Background(), f.ItemID, PlayedRequest{PlayedAt: tc.playedAt, TransitionSource: testSrc})
			if err != nil {
				t.Fatalf("played: %v", err)
			}
			if result.Flow.State != domain.FlowStateActive {
				t.Fatalf("state %s, want active", result.Flow.State)
			}
			if !result.Flow.NextActionAt.Equal(tc.want) {
				t.Fatalf("next action %v, want %v", result.Flow.NextActionAt, tc.want)
			}
		})
	}
}
