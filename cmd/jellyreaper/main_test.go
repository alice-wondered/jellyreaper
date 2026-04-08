package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"jellyreaper/internal/jellyfin"
)

func TestIngestBackfillBatchProcessesItemsBeforePlayback(t *testing.T) {
	called := make([]string, 0, 2)
	err := ingestBackfillBatch(
		context.Background(),
		func(context.Context, []jellyfin.ItemSnapshot) error {
			called = append(called, "items")
			return nil
		},
		func(context.Context, []jellyfin.PlaybackEvent) error {
			called = append(called, "playback")
			return nil
		},
		[]jellyfin.ItemSnapshot{{ItemID: "item-1"}},
		[]jellyfin.PlaybackEvent{{ItemID: "item-1", Type: "PlaybackStart"}},
	)
	if err != nil {
		t.Fatalf("ingest backfill batch: %v", err)
	}

	if want := []string{"items", "playback"}; !reflect.DeepEqual(called, want) {
		t.Fatalf("unexpected ingest order: got=%v want=%v", called, want)
	}
}

func TestIngestBackfillBatchStopsWhenItemIngestFails(t *testing.T) {
	calledPlayback := false
	itemErr := errors.New("items failed")

	err := ingestBackfillBatch(
		context.Background(),
		func(context.Context, []jellyfin.ItemSnapshot) error {
			return itemErr
		},
		func(context.Context, []jellyfin.PlaybackEvent) error {
			calledPlayback = true
			return nil
		},
		[]jellyfin.ItemSnapshot{{ItemID: "item-1"}},
		[]jellyfin.PlaybackEvent{{ItemID: "item-1", Type: "PlaybackStart"}},
	)
	if err == nil {
		t.Fatal("expected item ingest error")
	}
	if calledPlayback {
		t.Fatal("playback ingest should not run after item ingest failure")
	}
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
