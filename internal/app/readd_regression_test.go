package app

import (
	"context"
	"testing"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/jellyfin"
	"jellyreaper/internal/repo"
)

// Replays Invincible S1 after its 2026-10-03 re-add: Jellyfin restores the
// episode under its old ID with its May play history, the ItemAdded webhook
// carries no SeasonId, and a viewer later streams it (PlaybackProgress ~1/s).
func TestReAddedSeasonFirstPlayStaysQuiet(t *testing.T) {
	const (
		episode = "40a902ca34838d111ba4845e90c39d40"
		season  = "c9f8920dfd52412251ea49ff7f0b366d"
		series  = "9e2015f5b82644f498db4fb0af83afc6"
	)
	restoredPlay := time.Date(2026, 5, 3, 18, 4, 46, 0, time.UTC)
	readdedAt := time.Date(2026, 10, 3, 17, 41, 38, 0, time.UTC)
	backfillAt := time.Date(2026, 10, 3, 17, 50, 25, 0, time.UTC)
	playAt := time.Date(2026, 10, 5, 0, 1, 18, 0, time.UTC)

	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	clock := readdedAt
	svc.now = func() time.Time { return clock }
	svc.SyncFlowManagerClock()
	ctx := context.Background()

	if err := svc.HandleJellyfinWebhook(ctx, jellyfin.WebhookEvent{
		Payload:   jellyfin.WebhookPayload{ItemID: episode, ItemType: "Episode", Name: "IT'S ABOUT TIME", SeriesID: series, SeriesName: "INVINCIBLE", NotificationType: "ItemAdded"},
		ItemID:    episode,
		EventType: "ItemAdded",
		DedupeKey: "jellyfin:itemadded",
	}); err != nil {
		t.Fatalf("itemadded: %v", err)
	}

	clock = backfillAt
	if err := svc.IngestBackfillItemsWithCursor(ctx, []jellyfin.ItemSnapshot{{
		ItemID: episode, ItemType: "Episode", Name: "IT'S ABOUT TIME",
		SeasonID: season, SeasonName: "Season 1", SeriesID: series, SeriesName: "INVINCIBLE",
		LastPlayedAt: restoredPlay, PlayCount: 1, DateCreated: readdedAt,
		SeasonNumber: seasonNo(1),
	}}, "", ""); err != nil {
		t.Fatalf("backfill: %v", err)
	}

	read := func() (domain.MediaItem, domain.Flow, bool) {
		var m domain.MediaItem
		var f domain.Flow
		var found bool
		_ = store.WithTx(ctx, func(ctx context.Context, tx repo.TxRepository) error {
			m, _, _ = tx.GetMedia(ctx, episode)
			f, found, _ = tx.GetFlow(ctx, "target:season:"+season)
			return nil
		})
		return m, f, found
	}

	media, flow, found := read()
	t.Run("backfill links the episode to its season", func(t *testing.T) {
		if media.SeasonID != season {
			t.Errorf("episode season_id = %q, want %q", media.SeasonID, season)
		}
	})
	t.Run("season flow is born when the episode entered the library", func(t *testing.T) {
		if !found {
			t.Fatal("backfill should create the season flow")
		}
		if !flow.CreatedAt.Equal(readdedAt) {
			t.Errorf("flow.CreatedAt = %v, want the re-add date %v, not the restored play", flow.CreatedAt, readdedAt)
		}
	})

	clock = playAt
	for i := 0; i < 5; i++ {
		tick := playAt.Add(time.Duration(i) * time.Second)
		clock = tick
		if err := svc.HandleJellyfinWebhook(ctx, jellyfin.WebhookEvent{
			Payload:   jellyfin.WebhookPayload{ItemID: episode, ItemType: "Episode", Name: "IT'S ABOUT TIME", SeasonID: season, SeriesID: series, SeriesName: "INVINCIBLE", NotificationType: "PlaybackProgress", SeasonNumber: seasonNo(1)},
			ItemID:    episode,
			EventType: "PlaybackProgress",
			DedupeKey: "jellyfin:progress:" + tick.Format(time.RFC3339),
		}); err != nil {
			t.Fatalf("progress tick %d: %v", i, err)
		}
	}

	_, flow, _ = read()
	t.Run("a play pushes review out instead of pulling it in", func(t *testing.T) {
		if flow.State != domain.FlowStateActive {
			t.Errorf("flow state = %s, want active", flow.State)
		}
		if want := playAt.Add(time.Duration(flow.PolicySnapshot.ExpireAfterDays) * 24 * time.Hour); flow.NextActionAt.Before(want) {
			t.Errorf("next action %v, want >= play + review days (%v)", flow.NextActionAt, want)
		}
	})
}
