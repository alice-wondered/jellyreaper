package domain

import (
	"testing"
	"time"
)

func TestReviewAnchor(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour
	for _, tc := range []struct {
		name                   string
		created, played, added time.Time
		want                   time.Time
	}{
		{"play after flow wins", now.Add(-40 * day), now.Add(-2 * day), now.Add(-40 * day), now.Add(-2 * day)},
		{"no play: added date", now.Add(-40 * day), time.Time{}, now.Add(-20 * day), now.Add(-20 * day)},
		{"re-added: stale restored play floored to flow", now, now.Add(-586 * day), now, now},
		{"season before episodes: flow only", now, time.Time{}, time.Time{}, now},
		{"added before flow: flow floors", now.Add(-day), time.Time{}, now.Add(-30 * day), now.Add(-day)},
		{"nothing known", time.Time{}, time.Time{}, time.Time{}, time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ReviewAnchor(Flow{CreatedAt: tc.created}, tc.played, tc.added); !got.Equal(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
