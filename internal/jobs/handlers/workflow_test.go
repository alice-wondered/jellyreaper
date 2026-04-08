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
		flow, found, err := tx.GetFlow(context.Background(), "item1")
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected flow")
		}
		if flow.State != domain.FlowStateDeleted {
			t.Fatalf("unexpected flow state: %s", flow.State)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify flow: %v", err)
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

	h := NewHITLTimeoutHandler(store)
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

	h := NewHITLTimeoutHandler(store)
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
		if err := tx.UpsertMedia(context.Background(), domain.MediaItem{ItemID: "ep-1", SeriesID: "series-1", UpdatedAt: now}); err != nil {
			return err
		}
		return tx.UpsertMedia(context.Background(), domain.MediaItem{ItemID: "ep-2", SeriesID: "series-1", UpdatedAt: now})
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
	discordSvc.SetSendPromptHookForTest(func(context.Context, string, string, int64, string, string) (string, error) {
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
