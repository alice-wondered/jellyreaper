package handlers

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	bbolt "go.etcd.io/bbolt"

	"jellyreaper/internal/discord"
	"jellyreaper/internal/domain"
	"jellyreaper/internal/jellyfin"
	"jellyreaper/internal/repo"
	bboltrepo "jellyreaper/internal/repo/bbolt"
)

func testStore(t *testing.T) *bboltrepo.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "workflow.db")
	store, err := bboltrepo.Open(path, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = os.Remove(path) })
	return store
}

func TestExecuteDeleteHandlerTransitionsToDeleted(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:    "flow:item1",
			ItemID:    "item1",
			State:     domain.FlowStateDeleteQueued,
			Version:   0,
			CreatedAt: now,
			UpdatedAt: now,
		}, 0)
	}); err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	deleteCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && r.URL.Path == "/Items/item1" {
			deleteCalled = true
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := jellyfin.NewClient(server.URL, "api-key", server.Client())
	h := NewExecuteDeleteHandler(store, client)

	err := h.Handle(context.Background(), domain.JobRecord{JobID: "job1", ItemID: "item1", IdempotencyKey: "dedupe:1"})
	if err != nil {
		t.Fatalf("execute delete handle: %v", err)
	}
	if !deleteCalled {
		t.Fatal("expected jellyfin delete call")
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		_, found, err := tx.GetFlow(context.Background(), "item1")
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected flow to be deleted from store")
		}
		return nil
	}); err != nil {
		t.Fatalf("verify flow: %v", err)
	}
}

func TestEvaluatePolicyAssumesNeverPlayedWhenMetricsMissing(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:target:item:item-never-played",
			ItemID:      "target:item:item-never-played",
			SubjectType: "item",
			DisplayName: "Never Played",
			State:       domain.FlowStateActive,
			Version:     0,
			PolicySnapshot: domain.PolicySnapshot{
				ExpireAfterDays: 30,
				HITLTimeoutHrs:  48,
				TimeoutAction:   "delete",
			},
			CreatedAt: now,
			UpdatedAt: now,
		}, 0)
	}); err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	h := NewEvaluatePolicyHandler(store, nil)
	if err := h.Handle(context.Background(), domain.JobRecord{JobID: "job-eval-never-played", ItemID: "target:item:item-never-played"}); err != nil {
		t.Fatalf("evaluate policy: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(context.Background(), "target:item:item-never-played")
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected flow")
		}
		if flow.State != domain.FlowStatePendingReview {
			t.Fatalf("expected pending review, got %s", flow.State)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify flow state: %v", err)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), time.Now().UTC(), 20, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	foundPrompt := false
	for _, job := range jobs {
		if job.Kind == domain.JobKindSendHITLPrompt && job.ItemID == "target:item:item-never-played" {
			foundPrompt = true
		}
	}
	if !foundPrompt {
		t.Fatalf("expected hitl prompt job, got %#v", jobs)
	}
}

func TestMostRecentPlayForFlowFallsBackAcrossDashedAndNonDashedIDs(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()
	nonDashed := "1bb7dcaf2c6e04a75d91c4f0ee6b3cfd"
	dashed := "1bb7dcaf-2c6e-04a7-5d91-c4f0ee6b3cfd"

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertMedia(context.Background(), domain.MediaItem{
			ItemID:       nonDashed,
			ItemType:     "Movie",
			Name:         "Sample Movie",
			LastPlayedAt: now,
			UpdatedAt:    now,
		})
	}); err != nil {
		t.Fatalf("seed media: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		played, known, err := mostRecentPlayForFlow(context.Background(), tx, domain.Flow{ItemID: "target:movie:" + dashed})
		if err != nil {
			return err
		}
		if !known {
			t.Fatal("expected fallback to find last played via alternate id form")
		}
		if !played.Equal(now) {
			t.Fatalf("unexpected last played timestamp: got=%s want=%s", played, now)
		}
		return nil
	}); err != nil {
		t.Fatalf("evaluate fallback play timestamp: %v", err)
	}
}

func TestEvaluatePolicyFallsBackToCreatedAtWhenNeverPlayed(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()
	createdAt := now.Add(-10 * 24 * time.Hour)

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.UpsertMedia(context.Background(), domain.MediaItem{
			ItemID:    "movie-created-only",
			Name:      "Created Only",
			Title:     "Created Only",
			ItemType:  "Movie",
			CreatedAt: createdAt,
			UpdatedAt: now,
		}); err != nil {
			return err
		}
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:target:item:movie-created-only",
			ItemID:      "target:item:movie-created-only",
			SubjectType: "item",
			DisplayName: "Created Only",
			State:       domain.FlowStateActive,
			Version:     0,
			PolicySnapshot: domain.PolicySnapshot{
				ExpireAfterDays: 30,
				HITLTimeoutHrs:  48,
				TimeoutAction:   "delete",
			},
			CreatedAt: now,
			UpdatedAt: now,
		}, 0)
	}); err != nil {
		t.Fatalf("seed flow/media: %v", err)
	}

	h := NewEvaluatePolicyHandler(store, nil)
	if err := h.Handle(context.Background(), domain.JobRecord{JobID: "job-eval-created-only", ItemID: "target:item:movie-created-only"}); err != nil {
		t.Fatalf("evaluate policy: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(context.Background(), "target:item:movie-created-only")
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected flow")
		}
		if flow.State != domain.FlowStateActive {
			t.Fatalf("expected active state (not yet stale by creation age), got %s", flow.State)
		}
		if flow.NextActionAt.Before(now.Add(19 * 24 * time.Hour)) {
			t.Fatalf("expected deferred next action based on creation date, got %s", flow.NextActionAt)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify flow state: %v", err)
	}
}

func TestEvaluatePolicyUsesGlobalReviewDaysMetaLazily(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()
	lastPlayed := now.Add(-2 * time.Hour)

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.SetMeta(context.Background(), "settings.review_days", "60"); err != nil {
			return err
		}
		if err := tx.UpsertMedia(context.Background(), domain.MediaItem{
			ItemID:       "item-global-review",
			ItemType:     "Movie",
			LastPlayedAt: lastPlayed,
			CreatedAt:    now.Add(-10 * 24 * time.Hour),
			UpdatedAt:    now,
		}); err != nil {
			return err
		}
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:target:item:item-global-review",
			ItemID:      "target:item:item-global-review",
			SubjectType: "item",
			DisplayName: "Global Review Movie",
			State:       domain.FlowStateActive,
			Version:     0,
			PolicySnapshot: domain.PolicySnapshot{
				ExpireAfterDays: 30,
				HITLTimeoutHrs:  48,
				TimeoutAction:   "delete",
			},
			CreatedAt: now,
			UpdatedAt: now,
		}, 0)
	}); err != nil {
		t.Fatalf("seed flow/media/meta: %v", err)
	}

	h := NewEvaluatePolicyHandler(store, nil)
	if err := h.Handle(context.Background(), domain.JobRecord{JobID: "job-eval-global-review", ItemID: "target:item:item-global-review"}); err != nil {
		t.Fatalf("evaluate policy: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(context.Background(), "target:item:item-global-review")
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected flow")
		}
		if flow.State != domain.FlowStateActive {
			t.Fatalf("expected active state, got %s", flow.State)
		}
		wantDueAt := lastPlayed.Add(60 * 24 * time.Hour)
		if flow.NextActionAt.Before(wantDueAt.Add(-time.Minute)) || flow.NextActionAt.After(wantDueAt.Add(time.Minute)) {
			t.Fatalf("expected due time near %s from global review days, got %s", wantDueAt, flow.NextActionAt)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify flow state: %v", err)
	}
}

func TestHITLTimeoutHandlerQueuesDeleteWhenPendingReview(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:             "flow:item2",
			ItemID:             "item2",
			State:              domain.FlowStatePendingReview,
			DecisionDeadlineAt: now.Add(-time.Minute),
			Version:            0,
			CreatedAt:          now,
			UpdatedAt:          now,
		}, 0)
	}); err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	h := NewHITLTimeoutHandler(store, nil, nil)
	if err := h.Handle(context.Background(), domain.JobRecord{JobID: "timeout1", ItemID: "item2"}); err != nil {
		t.Fatalf("timeout handle: %v", err)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), time.Now().UTC(), 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	foundDelete := false
	for _, job := range jobs {
		if job.Kind == domain.JobKindExecuteDelete && job.ItemID == "item2" {
			foundDelete = true
		}
	}
	if !foundDelete {
		t.Fatalf("expected execute_delete job, got %#v", jobs)
	}
}

func TestHITLTimeoutHandlerDefersDeleteUntilDeadline(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:             "flow:item3",
			ItemID:             "item3",
			State:              domain.FlowStatePendingReview,
			DecisionDeadlineAt: now.Add(2 * time.Hour),
			Version:            0,
			CreatedAt:          now,
			UpdatedAt:          now,
		}, 0)
	}); err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	h := NewHITLTimeoutHandler(store, nil, nil)
	if err := h.Handle(context.Background(), domain.JobRecord{JobID: "timeout2", ItemID: "item3"}); err != nil {
		t.Fatalf("timeout handle: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(context.Background(), "item3")
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected flow")
		}
		if flow.State != domain.FlowStatePendingReview {
			t.Fatalf("expected pending review, got %s", flow.State)
		}
		if flow.NextActionAt.Before(flow.DecisionDeadlineAt) {
			t.Fatalf("expected next action to be at/after deadline, next=%s deadline=%s", flow.NextActionAt, flow.DecisionDeadlineAt)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify flow: %v", err)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), now.Add(90*time.Minute), 10, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	for _, job := range jobs {
		if job.Kind == domain.JobKindExecuteDelete && job.ItemID == "item3" {
			t.Fatalf("unexpected delete before deadline: %#v", job)
		}
	}
}

func TestHITLTimeoutHandlerFinalizesPromptMessage(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:             "flow:item4",
			ItemID:             "item4",
			DisplayName:        "Season 2 of Test Show",
			State:              domain.FlowStatePendingReview,
			DecisionDeadlineAt: now.Add(-time.Minute),
			Discord:            domain.DiscordContext{ChannelID: "ch-1", MessageID: "msg-1"},
			Version:            0,
			CreatedAt:          now,
			UpdatedAt:          now,
		}, 0)
	}); err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	pub, _, _ := ed25519.GenerateKey(nil)
	discordSvc, err := discord.NewService("", pub)
	if err != nil {
		t.Fatalf("discord service: %v", err)
	}
	called := false
	discordSvc.SetEditPromptHookForTest(func(context.Context, string, string, string) error {
		called = true
		return nil
	})

	h := NewHITLTimeoutHandler(store, discordSvc, nil)
	if err := h.Handle(context.Background(), domain.JobRecord{JobID: "timeout3", ItemID: "item4"}); err != nil {
		t.Fatalf("timeout handle: %v", err)
	}

	if !called {
		t.Fatal("expected timeout handler to finalize original HITL message")
	}
}

func TestExecuteDeleteHandlerDeletesChildrenForSeriesTarget(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:target:series:series-1",
			ItemID:      "target:series:series-1",
			SubjectType: "series",
			State:       domain.FlowStateDeleteQueued,
			Version:     0,
			CreatedAt:   now,
			UpdatedAt:   now,
		}, 0); err != nil {
			return err
		}
		if err := tx.UpsertMedia(context.Background(), domain.MediaItem{ItemID: "ep-1", SeriesID: "series-1", SeasonID: "season-1", UpdatedAt: now}); err != nil {
			return err
		}
		if err := tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:target:season:season-1",
			ItemID:      "target:season:season-1",
			SubjectType: "season",
			DisplayName: "Season 1",
			State:       domain.FlowStateActive,
			Version:     0,
			CreatedAt:   now,
			UpdatedAt:   now,
		}, 0); err != nil {
			return err
		}
		if err := tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:target:item:ep-1",
			ItemID:      "target:item:ep-1",
			SubjectType: "item",
			DisplayName: "Episode 1",
			State:       domain.FlowStateActive,
			Version:     0,
			CreatedAt:   now,
			UpdatedAt:   now,
		}, 0); err != nil {
			return err
		}
		if err := tx.UpsertMedia(context.Background(), domain.MediaItem{ItemID: "ep-2", SeriesID: "series-1", SeasonID: "season-1", UpdatedAt: now}); err != nil {
			return err
		}
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:target:item:ep-2",
			ItemID:      "target:item:ep-2",
			SubjectType: "item",
			DisplayName: "Episode 2",
			State:       domain.FlowStateActive,
			Version:     0,
			CreatedAt:   now,
			UpdatedAt:   now,
		}, 0)
	}); err != nil {
		t.Fatalf("seed aggregate flow/media: %v", err)
	}

	deleteCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && (r.URL.Path == "/Items/ep-1" || r.URL.Path == "/Items/ep-2") {
			deleteCount++
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	h := NewExecuteDeleteHandler(store, jellyfin.NewClient(server.URL, "api-key", server.Client()))
	if err := h.Handle(context.Background(), domain.JobRecord{JobID: "job-agg", ItemID: "target:series:series-1", IdempotencyKey: "dedupe:agg"}); err != nil {
		t.Fatalf("execute delete aggregate: %v", err)
	}
	if deleteCount != 2 {
		t.Fatalf("expected 2 child deletes, got %d", deleteCount)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		_, found, err := tx.GetMedia(context.Background(), "ep-1")
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected ep-1 media deleted")
		}

		_, found, err = tx.GetFlow(context.Background(), "target:item:ep-1")
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected child item flow deleted")
		}

		_, found, err = tx.GetFlow(context.Background(), "target:item:ep-2")
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected second child item flow deleted")
		}

		_, found, err = tx.GetFlow(context.Background(), "target:season:season-1")
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected season projection flow deleted for series delete")
		}
		return nil
	}); err != nil {
		t.Fatalf("verify post-delete state: %v", err)
	}
}

func TestExecuteDeleteHandlerDeletesChildrenForSeasonTarget(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()
	pub, _, _ := ed25519.GenerateKey(nil)
	discordSvc, err := discord.NewService("", pub)
	if err != nil {
		t.Fatalf("discord service: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:target:season:season-9",
			ItemID:      "target:season:season-9",
			SubjectType: "season",
			DisplayName: "Season 9",
			Discord:     domain.DiscordContext{ChannelID: "ch-season", MessageID: "msg-season"},
			State:       domain.FlowStateDeleteQueued,
			Version:     0,
			CreatedAt:   now,
			UpdatedAt:   now,
		}, 0); err != nil {
			return err
		}
		if err := tx.UpsertMedia(context.Background(), domain.MediaItem{ItemID: "s9e1", SeasonID: "season-9", UpdatedAt: now}); err != nil {
			return err
		}
		if err := tx.UpsertMedia(context.Background(), domain.MediaItem{ItemID: "s9e2", SeasonID: "season-9", UpdatedAt: now}); err != nil {
			return err
		}
		if err := tx.UpsertFlowCAS(context.Background(), domain.Flow{FlowID: "flow:target:item:s9e1", ItemID: "target:item:s9e1", SubjectType: "item", State: domain.FlowStateActive, Version: 0, CreatedAt: now, UpdatedAt: now}, 0); err != nil {
			return err
		}
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{FlowID: "flow:target:item:s9e2", ItemID: "target:item:s9e2", SubjectType: "item", State: domain.FlowStateActive, Version: 0, CreatedAt: now, UpdatedAt: now}, 0)
	}); err != nil {
		t.Fatalf("seed season aggregate: %v", err)
	}

	deleteCount := 0
	finalized := false
	discordSvc.SetEditPromptHookForTest(func(context.Context, string, string, string) error {
		finalized = true
		return nil
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && (r.URL.Path == "/Items/s9e1" || r.URL.Path == "/Items/s9e2") {
			deleteCount++
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	h := NewExecuteDeleteHandler(store, jellyfin.NewClient(server.URL, "api-key", server.Client()))
	h.SetDiscordService(discordSvc)
	if err := h.Handle(context.Background(), domain.JobRecord{JobID: "job-season", ItemID: "target:season:season-9", IdempotencyKey: "dedupe:season"}); err != nil {
		t.Fatalf("execute season delete: %v", err)
	}
	if deleteCount != 2 {
		t.Fatalf("expected 2 episode deletes, got %d", deleteCount)
	}
	if !finalized {
		t.Fatal("expected season delete completion to finalize discord message")
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		_, found, err := tx.GetFlow(context.Background(), "target:season:season-9")
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected season flow deleted")
		}
		_, found, err = tx.GetFlow(context.Background(), "target:item:s9e1")
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected episode flow s9e1 deleted")
		}
		_, found, err = tx.GetFlow(context.Background(), "target:item:s9e2")
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected episode flow s9e2 deleted")
		}
		return nil
	}); err != nil {
		t.Fatalf("verify season post-delete state: %v", err)
	}
}

func TestExecuteDeleteHandlerDeletesMovieProjectionAndSiblingFlows(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()
	pub, _, _ := ed25519.GenerateKey(nil)
	discordSvc, err := discord.NewService("", pub)
	if err != nil {
		t.Fatalf("discord service: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.UpsertFlowCAS(context.Background(), domain.Flow{FlowID: "flow:target:movie:mv-1", ItemID: "target:movie:mv-1", SubjectType: "movie", DisplayName: "Movie One", Discord: domain.DiscordContext{ChannelID: "ch-del", MessageID: "msg-del"}, State: domain.FlowStateDeleteQueued, Version: 0, CreatedAt: now, UpdatedAt: now}, 0); err != nil {
			return err
		}
		if err := tx.UpsertMedia(context.Background(), domain.MediaItem{ItemID: "mv-1", ItemType: "Movie", UpdatedAt: now}); err != nil {
			return err
		}
		if err := tx.UpsertFlowCAS(context.Background(), domain.Flow{FlowID: "flow:target:item:mv-1", ItemID: "target:item:mv-1", SubjectType: "item", State: domain.FlowStateActive, Version: 0, CreatedAt: now, UpdatedAt: now}, 0); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed movie target: %v", err)
	}

	deleteCalled := false
	finalized := false
	discordSvc.SetEditPromptHookForTest(func(context.Context, string, string, string) error {
		finalized = true
		return nil
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && r.URL.Path == "/Items/mv-1" {
			deleteCalled = true
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	h := NewExecuteDeleteHandler(store, jellyfin.NewClient(server.URL, "api-key", server.Client()))
	h.SetDiscordService(discordSvc)
	if err := h.Handle(context.Background(), domain.JobRecord{JobID: "job-movie", ItemID: "target:movie:mv-1", IdempotencyKey: "dedupe:movie"}); err != nil {
		t.Fatalf("execute movie delete: %v", err)
	}
	if !deleteCalled {
		t.Fatal("expected jellyfin movie delete call")
	}
	if !finalized {
		t.Fatal("expected delete completion to finalize discord message")
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		_, found, err := tx.GetFlow(context.Background(), "target:movie:mv-1")
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected movie projection flow deleted")
		}
		_, found, err = tx.GetFlow(context.Background(), "target:item:mv-1")
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected sibling item flow deleted")
		}
		_, found, err = tx.GetMedia(context.Background(), "mv-1")
		if err != nil {
			return err
		}
		if found {
			t.Fatal("expected movie media deleted")
		}
		return nil
	}); err != nil {
		t.Fatalf("verify movie post-delete state: %v", err)
	}
}

func TestSendHITLPromptHandlerAppliesMinimumResponseWindow(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:    "flow:item-min-window",
			ItemID:    "item-min-window",
			State:     domain.FlowStatePendingReview,
			Version:   0,
			CreatedAt: now,
			UpdatedAt: now,
		}, 0)
	}); err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	pub, _, _ := ed25519.GenerateKey(nil)
	discordSvc, err := discord.NewService("", pub)
	if err != nil {
		t.Fatalf("discord service: %v", err)
	}
	discordSvc.SetSendPromptHookForTest(func(context.Context, string, string, int64, string, string, string) (string, error) {
		return "msg-min-window", nil
	})

	h := NewSendHITLPromptHandler(store, nil, discordSvc, "channel-1", 25*time.Millisecond)
	if err := h.Handle(context.Background(), domain.JobRecord{JobID: "prompt-min-window", ItemID: "item-min-window"}); err != nil {
		t.Fatalf("handle prompt: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(context.Background(), "item-min-window")
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected flow")
		}
		if flow.DecisionDeadlineAt.Before(now.Add(23 * time.Hour)) {
			t.Fatalf("expected minimum response window, got deadline=%s", flow.DecisionDeadlineAt)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify flow: %v", err)
	}

	jobs, err := store.LeaseDueJobs(context.Background(), now.Add(2*time.Hour), 20, "test", time.Minute)
	if err != nil {
		t.Fatalf("lease jobs: %v", err)
	}
	for _, job := range jobs {
		if job.Kind == domain.JobKindExecuteDelete {
			t.Fatalf("unexpected execute delete job before minimum window: %#v", job)
		}
	}
}

func TestSendHITLPromptHandlerUsesPolicyTimeoutHoursForDeadline(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID: "flow:item-timeout-hours",
			ItemID: "item-timeout-hours",
			State:  domain.FlowStatePendingReview,
			PolicySnapshot: domain.PolicySnapshot{
				ExpireAfterDays: 30,
				HITLTimeoutHrs:  72,
				TimeoutAction:   "delete",
			},
			Version:   0,
			CreatedAt: now,
			UpdatedAt: now,
		}, 0)
	}); err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	pub, _, _ := ed25519.GenerateKey(nil)
	discordSvc, err := discord.NewService("", pub)
	if err != nil {
		t.Fatalf("discord service: %v", err)
	}
	discordSvc.SetSendPromptHookForTest(func(context.Context, string, string, int64, string, string, string) (string, error) {
		return "msg-timeout-hours", nil
	})

	h := NewSendHITLPromptHandler(store, nil, discordSvc, "channel-1", 24*time.Hour)
	if err := h.Handle(context.Background(), domain.JobRecord{JobID: "prompt-timeout-hours", ItemID: "item-timeout-hours"}); err != nil {
		t.Fatalf("handle prompt: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(context.Background(), "item-timeout-hours")
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected flow")
		}
		if flow.DecisionDeadlineAt.Before(now.Add(71 * time.Hour)) {
			t.Fatalf("expected deadline to honor policy timeout hours, got %s", flow.DecisionDeadlineAt)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify flow: %v", err)
	}
}

func TestSendHITLPromptHandlerIncludesLastPlayedStatusLine(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:    "flow:item-last-played",
			ItemID:    "item-last-played",
			State:     domain.FlowStatePendingReview,
			Version:   0,
			CreatedAt: now,
			UpdatedAt: now,
		}, 0); err != nil {
			return err
		}
		return tx.UpsertMedia(context.Background(), domain.MediaItem{ItemID: "item-last-played", LastPlayedAt: now.Add(-6 * time.Hour), UpdatedAt: now})
	}); err != nil {
		t.Fatalf("seed flow/media: %v", err)
	}

	pub, _, _ := ed25519.GenerateKey(nil)
	discordSvc, err := discord.NewService("", pub)
	if err != nil {
		t.Fatalf("discord service: %v", err)
	}
	receivedStatus := ""
	discordSvc.SetSendPromptHookForTest(func(_ context.Context, _ string, _ string, _ int64, _ string, _ string, status string) (string, error) {
		receivedStatus = status
		return "msg-status", nil
	})

	h := NewSendHITLPromptHandler(store, nil, discordSvc, "channel-1", 48*time.Hour)
	if err := h.Handle(context.Background(), domain.JobRecord{JobID: "prompt-last-played", ItemID: "item-last-played"}); err != nil {
		t.Fatalf("handle prompt: %v", err)
	}
	if receivedStatus == "" {
		t.Fatal("expected status line argument to be provided for prompt send")
	}
}

func TestSendHITLPromptHandlerUsesCurrentFlowVersionInCustomID(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:    "flow:item-version-match",
			ItemID:    "item-version-match",
			State:     domain.FlowStatePendingReview,
			Version:   0,
			CreatedAt: now,
			UpdatedAt: now,
		}, 0)
	}); err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	pub, _, _ := ed25519.GenerateKey(nil)
	discordSvc, err := discord.NewService("", pub)
	if err != nil {
		t.Fatalf("discord service: %v", err)
	}
	recordedVersion := int64(-1)
	discordSvc.SetSendPromptHookForTest(func(_ context.Context, _ string, _ string, version int64, _ string, _ string, _ string) (string, error) {
		recordedVersion = version
		return "msg-version", nil
	})

	h := NewSendHITLPromptHandler(store, nil, discordSvc, "channel-1", 48*time.Hour)
	if err := h.Handle(context.Background(), domain.JobRecord{JobID: "prompt-version", ItemID: "item-version-match"}); err != nil {
		t.Fatalf("handle prompt: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(context.Background(), "item-version-match")
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected flow")
		}
		if recordedVersion != flow.Version {
			t.Fatalf("expected custom id version to match flow version, got=%d want=%d", recordedVersion, flow.Version)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify flow: %v", err)
	}
}

func TestSendHITLPromptHandlerSendsNewMessageWhenStoredMessageMissing(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:item-stale-msg",
			ItemID:      "item-stale-msg",
			SubjectType: "item",
			DisplayName: "Stale Message Item",
			State:       domain.FlowStatePendingReview,
			Discord:     domain.DiscordContext{ChannelID: "channel-1", MessageID: "msg-stale"},
			Version:     0,
			CreatedAt:   now,
			UpdatedAt:   now,
		}, 0)
	}); err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	pub, _, _ := ed25519.GenerateKey(nil)
	discordSvc, err := discord.NewService("", pub)
	if err != nil {
		t.Fatalf("discord service: %v", err)
	}
	sendCount := 0
	discordSvc.SetSendPromptHookForTest(func(context.Context, string, string, int64, string, string, string) (string, error) {
		sendCount++
		return "msg-new", nil
	})

	h := NewSendHITLPromptHandler(store, nil, discordSvc, "channel-1", 48*time.Hour)
	if err := h.Handle(context.Background(), domain.JobRecord{JobID: "prompt-stale-msg", ItemID: "item-stale-msg"}); err != nil {
		t.Fatalf("handle prompt: %v", err)
	}
	if sendCount != 1 {
		t.Fatalf("expected one prompt send, got %d", sendCount)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(context.Background(), "item-stale-msg")
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected flow")
		}
		if flow.Discord.MessageID != "msg-new" {
			t.Fatalf("expected new discord message id, got %q", flow.Discord.MessageID)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify flow: %v", err)
	}
}
