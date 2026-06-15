package app

import (
	"context"
	"testing"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/jellyfin"
	"jellyreaper/internal/repo"
)

// countEvents returns the number of event records in the store.
func countEvents(t *testing.T, store interface{ repo.Repository }, now time.Time) int {
	t.Helper()
	var n int
	if err := store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		// PruneEvents with a zero cutoff doesn't delete anything but we just
		// need a count; instead, re-seed and check via a dedicated helper.
		// We achieve the count by pruning everything older than a far-future
		// cutoff and counting what was deleted — then we'd need to undo it.
		// A simpler approach: use the events from a well-known set inserted
		// below. So this helper just needs the raw count.
		// bbolt exposes no Count() on TxRepository; we use PruneEvents with
		// a far-future cutoff on a *copy* is not available. We instead count
		// via a dedicated countEventsInTx helper.
		n = countEventsInTx(t, tx)
		return nil
	}); err != nil {
		t.Fatalf("count events: %v", err)
	}
	return n
}

// countEventsInTx counts event records by pruning a scratch store or by
// using PruneEvents with the far-future sentinel and observing the delta.
// Because TxRepository.PruneEvents actually deletes, we count by calling
// PruneEvents(far-future) on a tx that we roll back — but WithTx doesn't
// support read-only views. Instead we record count as a side effect by
// comparing before/after a prune of nothing.
//
// Simpler: we leverage PruneEvents(zero) to delete nothing and observe the
// returned count is 0, then we use PruneEvents(far future) and *assert* the
// caller hasn't passed a store that already has extra events. For the tests
// in this file we always start from a fresh store with known state, so we
// compute the expected count directly.
//
// In practice: just use AppendEvent + IsProcessed/MarkProcessed pattern and
// count by seeding known events and checking their presence. The helper below
// is a simplification that counts via PruneEvents(epoch) — prunes nothing —
// and uses a sentinel trick.
func countEventsInTx(_ *testing.T, tx repo.TxRepository) int {
	// We use PruneEvents with the zero time (the epoch) as cutoff so nothing
	// is deleted (all events have OccurredAt >= epoch), and get back 0. That
	// doesn't help us count. Instead we use PruneEvents with a far-future
	// cutoff to delete all events and return the count — but this is
	// destructive. Since we only call this right before a prune assertion
	// that verifies specific counts, we can use a separate counting strategy:
	// count events written to the bucket using PruneEvents with the known
	// cutoff and restoring is not possible.
	//
	// Pragmatic solution: count by using the PruneEvents return value directly
	// in tests that know the cutoff, and use a dedicated "no-op" prune for
	// simple existence checks. For this helper we simply return -1 to signal
	// "not supported"; callers should use pruneAndCount instead.
	n, _ := tx.PruneEvents(context.Background(), time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC))
	return n
}

// pruneAndCountAll prunes all events from the store (using a far-future
// cutoff) and returns how many were deleted. Useful for counting before/after
// assertions. DESTRUCTIVE — only call on stores where you expect to empty
// the events bucket.
func pruneAndCountAll(t *testing.T, store interface{ repo.Repository }) int {
	t.Helper()
	var n int
	if err := store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		var err error
		n, err = tx.PruneEvents(ctx, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC))
		return err
	}); err != nil {
		t.Fatalf("pruneAndCountAll: %v", err)
	}
	return n
}

// makeWebhookEvent builds a minimal WebhookEvent for the given eventType.
func makeWebhookEvent(eventType, itemID, dedupeKey string, occurredAt time.Time) jellyfin.WebhookEvent {
	return jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			ItemID:           itemID,
			NotificationType: eventType,
			ItemType:         "Episode",
		},
		Raw:        map[string]any{"source": "test", "type": eventType},
		ItemID:     itemID,
		EventType:  eventType,
		DedupeKey:  dedupeKey,
		OccurredAt: occurredAt,
	}
}

// seedMediaItem writes a MediaItem into the store so playback events find it.
func seedMediaItem(t *testing.T, store interface{ repo.Repository }, itemID string, now time.Time) {
	t.Helper()
	if err := store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		return tx.UpsertMedia(ctx, domain.MediaItem{
			ItemID:    itemID,
			Name:      "Test " + itemID,
			ItemType:  "Episode",
			CreatedAt: now,
			UpdatedAt: now,
		})
	}); err != nil {
		t.Fatalf("seed media %s: %v", itemID, err)
	}
}

// isProcessed returns true if a dedupe key exists in the store.
func isProcessed(t *testing.T, store interface{ repo.Repository }, key string) bool {
	t.Helper()
	found := false
	if err := store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		var err error
		found, err = tx.IsProcessed(ctx, key)
		return err
	}); err != nil {
		t.Fatalf("isProcessed %s: %v", key, err)
	}
	return found
}

// ── isPlaybackProgressEvent predicate ───────────────────────────────────────

func TestIsPlaybackProgressEvent(t *testing.T) {
	cases := []struct {
		eventType string
		want      bool
	}{
		// exact match (various cases)
		{"PlaybackProgress", true},
		{"playbackprogress", true},
		{"PLAYBACKPROGRESS", true},
		{"  PlaybackProgress  ", true},
		// related but distinct events — must NOT match
		{"PlaybackStart", false},
		{"PlaybackStop", false},
		{"PlaybackPause", false},
		{"ItemAdded", false},
		{"ItemUpdated", false},
		{"UserDataSaved", false},
		{"", false},
	}
	for _, tc := range cases {
		got := isPlaybackProgressEvent(tc.eventType)
		if got != tc.want {
			t.Errorf("isPlaybackProgressEvent(%q) = %v, want %v", tc.eventType, got, tc.want)
		}
	}
}

// ── Fix 1: PlaybackProgress must NOT write to events bucket ─────────────────

// 0-case: no webhooks → no events in bucket.
func TestWebhook_PlaybackProgress_ZeroWebhooks_NoEvents(t *testing.T) {
	store := newTestStore(t)
	n := pruneAndCountAll(t, store)
	if n != 0 {
		t.Errorf("expected 0 events in a fresh store, got %d", n)
	}
}

// 1-case: a single PlaybackProgress webhook must not add to the events bucket
// but MUST mark dedupe and update media (play state).
func TestWebhook_PlaybackProgress_OneEvent_NoEventRecord(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	itemID := "aaaa0000aaaa0001"
	dedupeKey := "progress:1"
	seedMediaItem(t, store, itemID, now.Add(-time.Hour))

	evt := makeWebhookEvent("PlaybackProgress", itemID, dedupeKey, now)
	if err := svc.HandleJellyfinWebhook(context.Background(), evt); err != nil {
		t.Fatalf("handle webhook: %v", err)
	}

	// Events bucket must be empty.
	if n := pruneAndCountAll(t, store); n != 0 {
		t.Errorf("expected 0 events for PlaybackProgress, got %d", n)
	}

	// Dedupe must have been marked so a duplicate is silently ignored.
	if !isProcessed(t, store, dedupeKey) {
		t.Error("expected PlaybackProgress dedupe key to be marked processed")
	}

	// Play state must have been updated on the media record.
	media := mustGetMedia(t, store, itemID)
	if media.PlayCountTotal == 0 {
		t.Error("expected PlayCountTotal to be incremented by PlaybackProgress")
	}
}

// n-case: multiple PlaybackProgress webhooks → still no event records.
func TestWebhook_PlaybackProgress_MultipleEvents_NoEventRecords(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	itemID := "aaaa0000aaaa0002"
	seedMediaItem(t, store, itemID, now.Add(-time.Hour))

	for i := 0; i < 5; i++ {
		dedupeKey := "progress:multi:" + string(rune('a'+i))
		evt := makeWebhookEvent("PlaybackProgress", itemID, dedupeKey, now.Add(time.Duration(i)*time.Minute))
		if err := svc.HandleJellyfinWebhook(context.Background(), evt); err != nil {
			t.Fatalf("handle webhook %d: %v", i, err)
		}
	}

	if n := pruneAndCountAll(t, store); n != 0 {
		t.Errorf("expected 0 events for multiple PlaybackProgress webhooks, got %d", n)
	}
}

// n+1-case: adding one more PlaybackProgress after n others still writes no event.
func TestWebhook_PlaybackProgress_NplusOne_StillNoEvents(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	itemID := "aaaa0000aaaa0003"
	seedMediaItem(t, store, itemID, now.Add(-time.Hour))

	const n = 3
	for i := 0; i < n; i++ {
		dedupeKey := "progress:nplusone:" + string(rune('a'+i))
		evt := makeWebhookEvent("PlaybackProgress", itemID, dedupeKey, now.Add(time.Duration(i)*time.Minute))
		if err := svc.HandleJellyfinWebhook(context.Background(), evt); err != nil {
			t.Fatalf("handle webhook %d: %v", i, err)
		}
	}
	if n2 := pruneAndCountAll(t, store); n2 != 0 {
		t.Errorf("after %d progress events expected 0 event records, got %d", n, n2)
	}

	// The (n+1)-th webhook also produces no event record.
	nPlusOneKey := "progress:nplusone:extra"
	evt := makeWebhookEvent("PlaybackProgress", itemID, nPlusOneKey, now.Add(n*time.Minute))
	if err := svc.HandleJellyfinWebhook(context.Background(), evt); err != nil {
		t.Fatalf("handle n+1 webhook: %v", err)
	}
	if n2 := pruneAndCountAll(t, store); n2 != 0 {
		t.Errorf("after n+1 progress events expected 0 event records, got %d", n2)
	}
}

// counterexample: PlaybackStart and PlaybackStop MUST still write event records.
func TestWebhook_NonProgress_PlaybackStart_WritesEventRecord(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	itemID := "aaaa0000aaaa0004"
	seedMediaItem(t, store, itemID, now.Add(-time.Hour))

	startKey := "playbackstart:1"
	stopKey := "playbackstop:1"

	startEvt := makeWebhookEvent("PlaybackStart", itemID, startKey, now)
	if err := svc.HandleJellyfinWebhook(context.Background(), startEvt); err != nil {
		t.Fatalf("handle PlaybackStart: %v", err)
	}

	stopEvt := makeWebhookEvent("PlaybackStop", itemID, stopKey, now.Add(time.Hour))
	if err := svc.HandleJellyfinWebhook(context.Background(), stopEvt); err != nil {
		t.Fatalf("handle PlaybackStop: %v", err)
	}

	// Both start and stop must have written an event record.
	if n := pruneAndCountAll(t, store); n != 2 {
		t.Errorf("expected 2 event records for PlaybackStart+Stop, got %d", n)
	}
}

// counterexample: ItemAdded (catalog) events MUST still write event records.
func TestWebhook_ItemAdded_WritesEventRecord(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	// Use a Movie item type so it creates a target flow.
	itemID := "aaaa0000aaaa0005"
	dedupeKey := "itemadded:1"
	evt := jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			ItemID:           itemID,
			NotificationType: "ItemAdded",
			ItemType:         "Movie",
			Name:             "Test Movie",
		},
		Raw:        map[string]any{"source": "test"},
		ItemID:     itemID,
		EventType:  "ItemAdded",
		DedupeKey:  dedupeKey,
		OccurredAt: now,
	}
	if err := svc.HandleJellyfinWebhook(context.Background(), evt); err != nil {
		t.Fatalf("handle ItemAdded: %v", err)
	}

	if n := pruneAndCountAll(t, store); n != 1 {
		t.Errorf("expected 1 event record for ItemAdded, got %d", n)
	}
}

// counterexample: duplicate PlaybackProgress must be silently ignored (dedupe).
func TestWebhook_PlaybackProgress_DuplicateIsIgnored(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	itemID := "aaaa0000aaaa0006"
	dedupeKey := "progress:dup:1"
	seedMediaItem(t, store, itemID, now.Add(-time.Hour))

	evt := makeWebhookEvent("PlaybackProgress", itemID, dedupeKey, now)

	// First call.
	if err := svc.HandleJellyfinWebhook(context.Background(), evt); err != nil {
		t.Fatalf("first call: %v", err)
	}
	mediaAfterFirst := mustGetMedia(t, store, itemID)
	countAfterFirst := mediaAfterFirst.PlayCountTotal

	// Second call with same dedupeKey — must be a no-op.
	if err := svc.HandleJellyfinWebhook(context.Background(), evt); err != nil {
		t.Fatalf("second call: %v", err)
	}
	mediaAfterSecond := mustGetMedia(t, store, itemID)
	if mediaAfterSecond.PlayCountTotal != countAfterFirst {
		t.Errorf("duplicate PlaybackProgress must not increment PlayCountTotal: want %d, got %d",
			countAfterFirst, mediaAfterSecond.PlayCountTotal)
	}
}
