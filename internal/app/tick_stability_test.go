package app

import (
	"context"
	"strconv"
	"testing"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/repo"
)

type tickSnapshot struct {
	media domain.MediaItem
	flow  domain.Flow
	job   domain.JobRecord
}

func snapshotTick(t *testing.T, svc *Service) tickSnapshot {
	t.Helper()
	var s tickSnapshot
	_ = svc.repository.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		s.media, _, _ = tx.GetMedia(ctx, idEpisode)
		s.flow, _, _ = tx.GetFlow(ctx, "target:season:"+idSeason)
		s.job, _, _ = tx.GetJob(ctx, "job:eval:scheduled:target:season:"+idSeason)
		return nil
	})
	return s
}

// A watch is one play, not one write per second: after the start, progress
// ticks inside the throttle window change nothing durable.
func TestProgressTicksLeaveTheDatabaseStable(t *testing.T) {
	svc, _, clock := identityService(t, nil, season1Catalog())
	ctx := context.Background()
	if err := svc.HandleJellyfinWebhook(ctx, episodeEvent("ItemAdded", "", "added")); err != nil {
		t.Fatal(err)
	}
	*clock = clock.Add(time.Minute)
	if err := svc.HandleJellyfinWebhook(ctx, episodeEvent("PlaybackStart", "", "start")); err != nil {
		t.Fatal(err)
	}
	before := snapshotTick(t, svc)

	var keys []string
	for i := 0; i < 100; i++ {
		*clock = clock.Add(time.Second)
		ev := episodeEvent("PlaybackProgress", "", "tick"+strconv.Itoa(i))
		keys = append(keys, ev.DedupeKey)
		if err := svc.HandleJellyfinWebhook(ctx, ev); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
	}
	after := snapshotTick(t, svc)

	if after.flow.Version != before.flow.Version || !after.flow.NextActionAt.Equal(before.flow.NextActionAt) {
		t.Errorf("flow rewritten by ticks: version %d→%d next %v→%v", before.flow.Version, after.flow.Version, before.flow.NextActionAt, after.flow.NextActionAt)
	}
	if !after.job.UpdatedAt.Equal(before.job.UpdatedAt) {
		t.Errorf("eval job rewritten by ticks: %v→%v", before.job.UpdatedAt, after.job.UpdatedAt)
	}
	if !after.media.UpdatedAt.Equal(before.media.UpdatedAt) {
		t.Errorf("media rewritten inside the throttle window: %v→%v", before.media.UpdatedAt, after.media.UpdatedAt)
	}
	_ = svc.repository.WithTx(ctx, func(ctx context.Context, tx repo.TxRepository) error {
		for _, k := range keys {
			if ok, _ := tx.IsProcessed(ctx, k); ok {
				t.Fatalf("progress tick %s left a dedupe record", k)
			}
		}
		return nil
	})

	t.Run("a tick past the throttle advances the play clock once, not the flow", func(t *testing.T) {
		*clock = clock.Add(progressWriteInterval)
		if err := svc.HandleJellyfinWebhook(ctx, episodeEvent("PlaybackProgress", "", "late")); err != nil {
			t.Fatal(err)
		}
		late := snapshotTick(t, svc)
		if !late.media.LastPlayedAt.Equal(*clock) {
			t.Errorf("LastPlayedAt = %v, want %v", late.media.LastPlayedAt, *clock)
		}
		if late.flow.Version != before.flow.Version {
			t.Errorf("a %v due-date shift rewrote the flow (version %d→%d)", progressWriteInterval+100*time.Second, before.flow.Version, late.flow.Version)
		}
	})
}

func TestPlayOverridesDelayEvenInsideSlack(t *testing.T) {
	svc, _, clock := identityService(t, nil, season1Catalog())
	ctx := context.Background()
	if err := svc.HandleJellyfinWebhook(ctx, episodeEvent("ItemAdded", "", "added")); err != nil {
		t.Fatal(err)
	}
	flow := snapshotTick(t, svc).flow
	if err := svc.repository.WithTx(ctx, func(ctx context.Context, tx repo.TxRepository) error {
		expected := flow.Version
		flow.HITLOutcome = "delay"
		flow.Version++
		return tx.UpsertFlowCAS(ctx, flow, expected)
	}); err != nil {
		t.Fatal(err)
	}
	*clock = clock.Add(time.Second)
	if err := svc.HandleJellyfinWebhook(ctx, episodeEvent("PlaybackStart", "", "start")); err != nil {
		t.Fatal(err)
	}
	if got := snapshotTick(t, svc).flow.HITLOutcome; got != "played" {
		t.Fatalf("a play must clear a Delay outcome, got %q", got)
	}
}
