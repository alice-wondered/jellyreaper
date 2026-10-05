package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	bbolt "go.etcd.io/bbolt"

	bboltrepo "jellyreaper/internal/repo/bbolt"
)

func testRepoStore(t *testing.T) *bboltrepo.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "main-backfill.db")
	store, err := bboltrepo.Open(path, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = os.Remove(path) })
	return store
}

func TestRetryBackoffDelayCapsAtMaximum(t *testing.T) {
	base := time.Second
	max := 8 * time.Second

	if got := retryBackoffDelay(0, base, max); got != time.Second {
		t.Fatalf("attempt 0: got=%s want=%s", got, time.Second)
	}
	if got := retryBackoffDelay(1, base, max); got != 2*time.Second {
		t.Fatalf("attempt 1: got=%s want=%s", got, 2*time.Second)
	}
	if got := retryBackoffDelay(2, base, max); got != 4*time.Second {
		t.Fatalf("attempt 2: got=%s want=%s", got, 4*time.Second)
	}
	if got := retryBackoffDelay(3, base, max); got != 8*time.Second {
		t.Fatalf("attempt 3: got=%s want=%s", got, 8*time.Second)
	}
	if got := retryBackoffDelay(20, base, max); got != 8*time.Second {
		t.Fatalf("attempt 20: got=%s want=%s", got, 8*time.Second)
	}
}

func TestIsRateLimitErr(t *testing.T) {
	if !isRateLimitErr(errors.New("items query returned status 429")) {
		t.Fatal("expected 429 errors to be treated as rate limited")
	}
	if !isRateLimitErr(errors.New("Too Many Requests from upstream")) {
		t.Fatal("expected too-many-requests errors to be treated as rate limited")
	}
	if isRateLimitErr(errors.New("connection reset by peer")) {
		t.Fatal("did not expect unrelated errors to be rate limited")
	}
}

func TestBackfillCursorRoundTrip(t *testing.T) {
	store := testRepoStore(t)
	ctx := context.Background()

	state := backfillCursorState{
		Active:             true,
		Since:              time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC),
		Phase:              "items",
		PlaybackStartIndex: 25,
		ItemsStartIndex:    50,
		PlaysProcessed:     10,
		ItemsProcessed:     20,
	}

	if err := saveBackfillCursor(ctx, store, state); err != nil {
		t.Fatalf("save cursor: %v", err)
	}

	loaded, err := loadBackfillCursor(ctx, store)
	if err != nil {
		t.Fatalf("load cursor: %v", err)
	}
	if !loaded.Active || loaded.ItemsStartIndex != 50 || loaded.Phase != "items" {
		t.Fatalf("unexpected loaded cursor: %#v", loaded)
	}

	if err := clearBackfillCursor(ctx, store); err != nil {
		t.Fatalf("clear cursor: %v", err)
	}
	cleared, err := loadBackfillCursor(ctx, store)
	if err != nil {
		t.Fatalf("reload cleared cursor: %v", err)
	}
	if cleared.Active {
		t.Fatalf("expected cleared cursor to be inactive, got %#v", cleared)
	}
}
