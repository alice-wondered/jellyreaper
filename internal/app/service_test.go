package app

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	bbolt "go.etcd.io/bbolt"

	"jellyreaper/internal/discord"
	"jellyreaper/internal/domain"
	"jellyreaper/internal/jellyfin"
	"jellyreaper/internal/repo"
	bboltrepo "jellyreaper/internal/repo/bbolt"
)

func newTestStore(t *testing.T) *bboltrepo.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := bboltrepo.Open(path, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = os.Remove(path) })
	return store
}

func interaction(action, itemID string, version int64, id string) discord.IncomingInteraction {
	return discord.IncomingInteraction{
		Raw:           &discordgo.Interaction{},
		Type:          discordgo.InteractionMessageComponent,
		InteractionID: id,
		Token:         "tok",
		CustomID:      "jr:v1:" + action + ":" + itemID + ":" + strconv.FormatInt(version, 10),
	}
}

func TestHITLArchiveLeavesNoDeletionJob(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	targetID := "target:item:item-archive"
	resp, err := svc.HandleDiscordComponentInteraction(context.Background(), interaction("archive", targetID, 0, "i-archive"))
	if err != nil {
		t.Fatalf("handle interaction: %v", err)
	}
	if resp == nil || resp.Data == nil {
		t.Fatal("expected response data")
	}

	flow := mustGetFlow(t, store, targetID)
	if flow.State != domain.FlowStateArchived {
		t.Fatalf("unexpected flow state: %s", flow.State)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), now.Add(365*24*time.Hour), 100, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	for _, job := range jobs {
		if job.ItemID == targetID {
			t.Fatalf("expected no queued jobs for archive action, found: %#v", job)
		}
	}
}

func TestHITLDeleteQueuesImmediateDeleteJob(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 12, 30, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	targetID := "target:item:item-delete"
	_, err := svc.HandleDiscordComponentInteraction(context.Background(), interaction("delete", targetID, 0, "i-delete"))
	if err != nil {
		t.Fatalf("handle interaction: %v", err)
	}

	flow := mustGetFlow(t, store, targetID)
	if flow.State != domain.FlowStateDeleteQueued {
		t.Fatalf("unexpected flow state: %s", flow.State)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), now, 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected one job, got %d", len(jobs))
	}
	if jobs[0].Kind != domain.JobKindExecuteDelete {
		t.Fatalf("unexpected job kind: %s", jobs[0].Kind)
	}
}

func TestHITLDelaySchedulesFutureEvaluation(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 13, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	targetID := "target:item:item-delay"
	_, err := svc.HandleDiscordComponentInteraction(context.Background(), interaction("delay", targetID, 0, "i-delay"))
	if err != nil {
		t.Fatalf("handle interaction: %v", err)
	}

	flow := mustGetFlow(t, store, targetID)
	if flow.State != domain.FlowStatePendingReview {
		t.Fatalf("unexpected flow state: %s", flow.State)
	}
	if want := now.Add(24 * time.Hour); !flow.NextActionAt.Equal(want) {
		t.Fatalf("unexpected next action: got=%s want=%s", flow.NextActionAt, want)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), now.Add(48*time.Hour), 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].Kind != domain.JobKindEvaluatePolicy {
		t.Fatalf("unexpected jobs: %#v", jobs)
	}
	if !jobs[0].RunAt.Equal(now.Add(24 * time.Hour)) {
		t.Fatalf("unexpected evaluate runAt: got=%s want=%s", jobs[0].RunAt, now.Add(24*time.Hour))
	}
}

func TestWebhookIndexesFlowAndJobAndDedupe(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 14, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	event := jellyfin.WebhookEvent{
		Payload:   jellyfin.WebhookPayload{ItemID: "item-webhook", ItemType: "Movie", Name: "Webhook Movie", NotificationType: "ItemAdded", EventID: "evt-1"},
		Raw:       map[string]any{"ItemId": "item-webhook", "NotificationType": "ItemAdded", "EventId": "evt-1"},
		ItemID:    "item-webhook",
		EventID:   "evt-1",
		EventType: "ItemAdded",
		DedupeKey: "jellyfin:evt-1",
	}

	if err := svc.HandleJellyfinWebhook(context.Background(), event); err != nil {
		t.Fatalf("first webhook handle: %v", err)
	}
	if err := svc.HandleJellyfinWebhook(context.Background(), event); err != nil {
		t.Fatalf("duplicate webhook handle should be no-op: %v", err)
	}

	flow := mustGetFlow(t, store, "target:movie:item-webhook")
	if flow.State != domain.FlowStateActive {
		t.Fatalf("unexpected flow state: %s", flow.State)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		processed, err := tx.IsProcessed(context.Background(), "jellyfin:evt-1")
		if err != nil {
			return err
		}
		if !processed {
			t.Fatalf("expected dedupe key to be marked processed")
		}
		return nil
	}); err != nil {
		t.Fatalf("verify dedupe key: %v", err)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), now, 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].Kind != domain.JobKindEvaluatePolicy {
		t.Fatalf("expected single evaluate job after dedupe, got %#v", jobs)
	}
}

func TestWebhookEpisodeAggregatesToSeasonAndSeries(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 7, 15, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	event := jellyfin.WebhookEvent{
		Payload: jellyfin.WebhookPayload{
			ItemID:           "ep-1",
			ItemType:         "Episode",
			Name:             "Pilot",
			SeasonID:         "season-1",
			SeasonName:       "Season 1",
			SeriesID:         "series-1",
			SeriesName:       "My Show",
			NotificationType: "PlaybackStart",
			EventID:          "evt-episode-1",
		},
		Raw:       map[string]any{"ItemId": "ep-1", "ItemType": "Episode", "SeasonId": "season-1", "SeriesId": "series-1", "EventId": "evt-episode-1"},
		ItemID:    "ep-1",
		EventID:   "evt-episode-1",
		EventType: "PlaybackStart",
		DedupeKey: "jellyfin:evt-episode-1",
	}

	if err := svc.HandleJellyfinWebhook(context.Background(), event); err != nil {
		t.Fatalf("handle webhook: %v", err)
	}

	seasonFlow := mustGetFlow(t, store, "target:season:season-1")
	if seasonFlow.DisplayName != "Season 1 of My Show" {
		t.Fatalf("unexpected season flow display name: %s", seasonFlow.DisplayName)
	}
	jobs, err := store.LeaseDueJobs(context.Background(), now.Add(120*24*time.Hour), 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected one evaluate job for season target, got %d (%#v)", len(jobs), jobs)
	}
}

func TestParseCustomIDWithColonsInTargetID(t *testing.T) {
	parsed, err := parseCustomID("jr:v1:archive:target:series:series:part:1:42")
	if err != nil {
		t.Fatalf("parse custom id: %v", err)
	}
	if parsed.Action != "archive" {
		t.Fatalf("unexpected action: %s", parsed.Action)
	}
	if parsed.ItemID != "target:series:series:part:1" {
		t.Fatalf("unexpected parsed item id: %s", parsed.ItemID)
	}
	if parsed.Version != 42 {
		t.Fatalf("unexpected version: %d", parsed.Version)
	}
}

func TestDeriveTargetsUsesIDsNotTitles(t *testing.T) {
	event := jellyfin.WebhookEvent{Payload: jellyfin.WebhookPayload{
		ItemType:   "Episode",
		Name:       "Some:Anime:Episode",
		SeasonID:   "season-01",
		SeasonName: "Season: 1",
		SeriesID:   "series-abc",
		SeriesName: "Anime: Saga",
	}}

	targets := deriveTargets(event)
	if len(targets) != 1 {
		t.Fatalf("expected one target, got %d", len(targets))
	}
	if targets[0].Canonical != "target:season:season-01" {
		t.Fatalf("unexpected season canonical key: %s", targets[0].Canonical)
	}
}

func TestDeriveTargetsSkipsSeriesItems(t *testing.T) {
	event := jellyfin.WebhookEvent{Payload: jellyfin.WebhookPayload{
		ItemType:   "Series",
		ItemID:     "series-abc",
		SeriesName: "My Show",
		Name:       "My Show",
	}}

	targets := deriveTargets(event)
	if len(targets) != 0 {
		t.Fatalf("expected no targets for series payloads, got %#v", targets)
	}
}

func TestIngestBackfillItemsSchedulesDeferredEvaluateFromLastPlay(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 8, 9, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	lastPlayed := now.Add(-2 * time.Hour)
	err := svc.IngestBackfillItems(context.Background(), []jellyfin.ItemSnapshot{{
		ItemID:       "ep-backfill-1",
		ItemType:     "Episode",
		SeasonID:     "season-1",
		SeasonName:   "Season 1",
		SeriesID:     "series-1",
		SeriesName:   "Show A",
		Name:         "Pilot",
		LastPlayedAt: lastPlayed,
		PlayCount:    3,
	}})
	if err != nil {
		t.Fatalf("ingest backfill items: %v", err)
	}

	flow := mustGetFlow(t, store, "target:season:season-1")
	if flow.State != domain.FlowStateActive {
		t.Fatalf("unexpected flow state: %s", flow.State)
	}
	wantRunAt := lastPlayed.Add(90 * 24 * time.Hour)
	if !flow.NextActionAt.Equal(wantRunAt) {
		t.Fatalf("unexpected next action: got=%s want=%s", flow.NextActionAt, wantRunAt)
	}

	jobsEarly, err := store.LeaseDueJobs(context.Background(), now.Add(time.Hour), 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs early: %v", err)
	}
	if len(jobsEarly) != 0 {
		t.Fatalf("expected no due jobs before scheduled time, got %#v", jobsEarly)
	}

	jobsDue, err := store.LeaseDueJobs(context.Background(), wantRunAt.Add(time.Minute), 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs due: %v", err)
	}
	if len(jobsDue) != 1 || jobsDue[0].Kind != domain.JobKindEvaluatePolicy {
		t.Fatalf("unexpected due jobs: %#v", jobsDue)
	}
}

func TestIngestBackfillItemsPreservesHigherPlaybackMetrics(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store, nil, nil)
	now := time.Date(2026, 4, 8, 10, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertMedia(context.Background(), domain.MediaItem{
			ItemID:         "movie-1",
			Name:           "Movie One",
			Title:          "Movie One",
			ItemType:       "Movie",
			LastPlayedAt:   now.Add(-time.Hour),
			PlayCountTotal: 9,
			UpdatedAt:      now.Add(-time.Hour),
		})
	}); err != nil {
		t.Fatalf("seed media: %v", err)
	}

	err := svc.IngestBackfillItems(context.Background(), []jellyfin.ItemSnapshot{{
		ItemID:     "movie-1",
		ItemType:   "Movie",
		Name:       "Movie One",
		PlayCount:  0,
		ImageURL:   "",
		SeriesName: "",
	}})
	if err != nil {
		t.Fatalf("ingest backfill items: %v", err)
	}

	media := mustGetMedia(t, store, "movie-1")
	if media.PlayCountTotal != 9 {
		t.Fatalf("expected preserved play count 9, got %d", media.PlayCountTotal)
	}
	if media.LastPlayedAt.IsZero() {
		t.Fatal("expected preserved last played timestamp")
	}
}

func mustGetFlow(t *testing.T, store *bboltrepo.Store, itemID string) domain.Flow {
	t.Helper()
	var flow domain.Flow
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		var found bool
		var err error
		flow, found, err = tx.GetFlow(context.Background(), itemID)
		if err != nil {
			return err
		}
		if !found {
			t.Fatalf("flow %s not found", itemID)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("get flow %s: %v", itemID, err)
	}
	return flow
}

func mustGetMedia(t *testing.T, store *bboltrepo.Store, itemID string) domain.MediaItem {
	t.Helper()
	var media domain.MediaItem
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		item, found, err := tx.GetMedia(context.Background(), itemID)
		if err != nil {
			return err
		}
		if !found {
			t.Fatalf("media %s not found", itemID)
		}
		media = item
		return nil
	})
	if err != nil {
		t.Fatalf("get media %s: %v", itemID, err)
	}
	return media
}
