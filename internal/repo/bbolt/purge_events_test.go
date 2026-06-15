package bbolt

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	bboltlib "go.etcd.io/bbolt"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/repo"
)

func TestPurgeEventsByTypePrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "purge.db")
	store, err := Open(path, 0o600, &bboltlib.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC()
	seed := []domain.Event{
		{EventID: "evt:p1", ItemID: "i1", Type: "jellyfin.PlaybackProgress", Source: "jellyfin", OccurredAt: now},
		{EventID: "evt:p2", ItemID: "i2", Type: "jellyfin.PlaybackProgress", Source: "jellyfin", OccurredAt: now},
		{EventID: "evt:start", ItemID: "i3", Type: "jellyfin.PlaybackStart", Source: "jellyfin", OccurredAt: now},
		{EventID: "evt:added", ItemID: "i4", Type: "jellyfin.ItemAdded", Source: "jellyfin", OccurredAt: now},
	}
	if err := store.WithTx(ctx, func(ctx context.Context, tx repo.TxRepository) error {
		for _, e := range seed {
			if err := tx.AppendEvent(ctx, e); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// PurgeEventsByTypePrefix opens the file itself; bbolt is single-writer so
	// the seeding store must be closed first.
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// n-case: deletes exactly the two PlaybackProgress events.
	deleted, err := PurgeEventsByTypePrefix(path, "jellyfin.PlaybackProgress")
	if err != nil {
		t.Fatalf("purge progress: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 progress events deleted, got %d", deleted)
	}

	// idempotent / 0-case: nothing left matching the prefix.
	again, err := PurgeEventsByTypePrefix(path, "jellyfin.PlaybackProgress")
	if err != nil {
		t.Fatalf("purge progress again: %v", err)
	}
	if again != 0 {
		t.Fatalf("expected 0 on second purge, got %d", again)
	}

	// counterexample: a non-progress event must survive the progress purge.
	start, err := PurgeEventsByTypePrefix(path, "jellyfin.PlaybackStart")
	if err != nil {
		t.Fatalf("purge start: %v", err)
	}
	if start != 1 {
		t.Fatalf("expected PlaybackStart to survive the progress purge (1), got %d", start)
	}

	// no-match prefix deletes nothing.
	none, err := PurgeEventsByTypePrefix(path, "discord.")
	if err != nil {
		t.Fatalf("purge none: %v", err)
	}
	if none != 0 {
		t.Fatalf("expected 0 for non-matching prefix, got %d", none)
	}
}
