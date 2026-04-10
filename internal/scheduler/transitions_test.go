package scheduler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	bbolt "go.etcd.io/bbolt"

	"jellyreaper/internal/domain"
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

func seedFlow(t *testing.T, store *bboltrepo.Store, flow domain.Flow) {
	t.Helper()
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(context.Background(), flow, 0)
	})
	if err != nil {
		t.Fatalf("seed flow: %v", err)
	}
}

func getFlow(t *testing.T, store *bboltrepo.Store, itemID string) domain.Flow {
	t.Helper()
	var flow domain.Flow
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		f, ok, err := tx.GetFlow(context.Background(), itemID)
		if err != nil {
			return err
		}
		if !ok {
			t.Fatalf("flow %s not found", itemID)
		}
		flow = f
		return nil
	})
	if err != nil {
		t.Fatalf("get flow: %v", err)
	}
	return flow
}

func newTestFlowManager(store *bboltrepo.Store) *FlowManager {
	return NewFlowManager(store, NewScheduler(nil, nil), nil)
}

func baseFlow(itemID string, state domain.FlowState) domain.Flow {
	now := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	return domain.Flow{
		FlowID:      "flow:" + itemID,
		ItemID:      itemID,
		SubjectType: "movie",
		DisplayName: "Test Movie",
		State:       state,
		Version:     0,
		PolicySnapshot: domain.PolicySnapshot{
			ExpireAfterDays: 30,
			HITLTimeoutHrs:  48,
			TimeoutAction:   "delete",
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
}

var testSrc = TransitionSource{Source: "test", Reason: "unit_test"}

func TestArchive(t *testing.T) {
	store := newTestStore(t)
	mgr := newTestFlowManager(store)

	f := baseFlow("target:movie:arch", domain.FlowStateActive)
	seedFlow(t, store, f)

	result, err := mgr.Archive(context.Background(), f.ItemID, testSrc)
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if result.Flow.State != domain.FlowStateArchived {
		t.Fatalf("expected archived, got %s", result.Flow.State)
	}

	got := getFlow(t, store, f.ItemID)
	if got.State != domain.FlowStateArchived {
		t.Fatalf("expected archived in store, got %s", got.State)
	}
}

func TestArchive_RejectsDeleteQueued(t *testing.T) {
	store := newTestStore(t)
	mgr := newTestFlowManager(store)

	f := baseFlow("target:movie:dq", domain.FlowStateDeleteQueued)
	seedFlow(t, store, f)

	_, err := mgr.Archive(context.Background(), f.ItemID, testSrc)
	if err == nil {
		t.Fatal("expected error archiving delete_queued flow")
	}
}

func TestDelete(t *testing.T) {
	store := newTestStore(t)
	mgr := newTestFlowManager(store)

	f := baseFlow("target:movie:del", domain.FlowStatePendingReview)
	f.Discord = domain.DiscordContext{ChannelID: "ch1", MessageID: "msg1"}
	seedFlow(t, store, f)

	result, err := mgr.Delete(context.Background(), f.ItemID, testSrc)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if result.Flow.State != domain.FlowStateDeleteQueued {
		t.Fatalf("expected delete_queued, got %s", result.Flow.State)
	}
	if result.FinalizePrompt == nil {
		t.Fatal("expected finalization for pending_review flow")
	}
	if result.FinalizePrompt.ChannelID != "ch1" {
		t.Fatalf("expected ch1, got %s", result.FinalizePrompt.ChannelID)
	}
}

func TestDelay(t *testing.T) {
	store := newTestStore(t)
	mgr := newTestFlowManager(store)

	f := baseFlow("target:movie:dly", domain.FlowStatePendingReview)
	seedFlow(t, store, f)

	result, err := mgr.Delay(context.Background(), f.ItemID, DelayRequest{
		Days:             30,
		TransitionSource: testSrc,
	})
	if err != nil {
		t.Fatalf("delay: %v", err)
	}
	if result.Flow.State != domain.FlowStateActive {
		t.Fatalf("expected active, got %s", result.Flow.State)
	}

	got := getFlow(t, store, f.ItemID)
	if got.HITLOutcome != "delay" {
		t.Fatalf("expected delay outcome, got %s", got.HITLOutcome)
	}
}

func TestPlayed(t *testing.T) {
	store := newTestStore(t)
	mgr := newTestFlowManager(store)
	nextEval := time.Now().UTC().Add(30 * 24 * time.Hour)

	f := baseFlow("target:movie:ply", domain.FlowStatePendingReview)
	f.Discord = domain.DiscordContext{ChannelID: "ch2", MessageID: "msg2"}
	seedFlow(t, store, f)

	result, err := mgr.Played(context.Background(), f.ItemID, PlayedRequest{
		NextEvalAt:       nextEval,
		TransitionSource: testSrc,
	})
	if err != nil {
		t.Fatalf("played: %v", err)
	}
	if result.Flow.State != domain.FlowStateActive {
		t.Fatalf("expected active, got %s", result.Flow.State)
	}
	if result.FinalizePrompt == nil {
		t.Fatal("expected finalization for played recovery")
	}
}

func TestRequestReview(t *testing.T) {
	store := newTestStore(t)
	mgr := newTestFlowManager(store)

	f := baseFlow("target:movie:rev", domain.FlowStateActive)
	seedFlow(t, store, f)

	result, err := mgr.RequestReview(context.Background(), f.ItemID, testSrc)
	if err != nil {
		t.Fatalf("request review: %v", err)
	}
	if result.Flow.State != domain.FlowStatePendingReview {
		t.Fatalf("expected pending_review, got %s", result.Flow.State)
	}
}

func TestRollbackToActive(t *testing.T) {
	store := newTestStore(t)
	mgr := newTestFlowManager(store)

	f := baseFlow("target:movie:rb", domain.FlowStatePendingReview)
	seedFlow(t, store, f)

	result, err := mgr.RollbackToActive(context.Background(), f.ItemID, 10*time.Minute, testSrc)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if result.Flow.State != domain.FlowStateActive {
		t.Fatalf("expected active, got %s", result.Flow.State)
	}
}

func TestUnarchive(t *testing.T) {
	store := newTestStore(t)
	mgr := newTestFlowManager(store)

	f := baseFlow("target:movie:ua", domain.FlowStateArchived)
	seedFlow(t, store, f)

	result, err := mgr.Unarchive(context.Background(), f.ItemID, testSrc)
	if err != nil {
		t.Fatalf("unarchive: %v", err)
	}
	if result.Flow.State != domain.FlowStateActive {
		t.Fatalf("expected active, got %s", result.Flow.State)
	}
}

func TestFlowNotFound(t *testing.T) {
	store := newTestStore(t)
	mgr := newTestFlowManager(store)

	_, err := mgr.Archive(context.Background(), "target:movie:nonexistent", testSrc)
	if err == nil {
		t.Fatal("expected error for nonexistent flow")
	}
}
