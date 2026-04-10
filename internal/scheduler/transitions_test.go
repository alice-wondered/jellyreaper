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

func testStore(t *testing.T) *bboltrepo.Store {
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

func newTestFlowManager() *FlowManager {
	return NewFlowManager(NewScheduler(nil, nil), nil)
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

func TestArchive(t *testing.T) {
	store := testStore(t)
	mgr := newTestFlowManager()
	now := time.Date(2026, 4, 10, 14, 0, 0, 0, time.UTC)

	f := baseFlow("target:movie:arch", domain.FlowStateActive)
	seedFlow(t, store, f)

	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow, _, _ := tx.GetFlow(context.Background(), f.ItemID)
		_, err := mgr.Archive(context.Background(), tx, &flow, now)
		return err
	})
	if err != nil {
		t.Fatalf("archive: %v", err)
	}

	got := getFlow(t, store, f.ItemID)
	if got.State != domain.FlowStateArchived {
		t.Fatalf("expected archived, got %s", got.State)
	}
	if got.HITLOutcome != "archive" {
		t.Fatalf("expected archive outcome, got %s", got.HITLOutcome)
	}
}

func TestArchive_RejectsDeleteQueued(t *testing.T) {
	store := testStore(t)
	mgr := newTestFlowManager()
	now := time.Date(2026, 4, 10, 14, 0, 0, 0, time.UTC)

	f := baseFlow("target:movie:dq", domain.FlowStateDeleteQueued)
	seedFlow(t, store, f)

	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow, _, _ := tx.GetFlow(context.Background(), f.ItemID)
		_, err := mgr.Archive(context.Background(), tx, &flow, now)
		return err
	})
	if err == nil {
		t.Fatal("expected error archiving delete_queued flow")
	}
}

func TestDelete(t *testing.T) {
	store := testStore(t)
	mgr := newTestFlowManager()
	now := time.Date(2026, 4, 10, 14, 0, 0, 0, time.UTC)

	f := baseFlow("target:movie:del", domain.FlowStatePendingReview)
	f.Discord = domain.DiscordContext{ChannelID: "ch1", MessageID: "msg1"}
	seedFlow(t, store, f)

	var result *TransitionResult
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow, _, _ := tx.GetFlow(context.Background(), f.ItemID)
		var err error
		result, err = mgr.Delete(context.Background(), tx, &flow, now, "test")
		return err
	})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	got := getFlow(t, store, f.ItemID)
	if got.State != domain.FlowStateDeleteQueued {
		t.Fatalf("expected delete_queued, got %s", got.State)
	}
	if result.FinalizePrompt == nil {
		t.Fatal("expected finalization for pending_review flow")
	}
	if result.FinalizePrompt.ChannelID != "ch1" {
		t.Fatalf("expected ch1, got %s", result.FinalizePrompt.ChannelID)
	}
}

func TestDelay(t *testing.T) {
	store := testStore(t)
	mgr := newTestFlowManager()
	now := time.Date(2026, 4, 10, 14, 0, 0, 0, time.UTC)
	delayUntil := now.Add(30 * 24 * time.Hour)

	f := baseFlow("target:movie:dly", domain.FlowStatePendingReview)
	seedFlow(t, store, f)

	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow, _, _ := tx.GetFlow(context.Background(), f.ItemID)
		_, err := mgr.Delay(context.Background(), tx, &flow, now, delayUntil, "test_delay")
		return err
	})
	if err != nil {
		t.Fatalf("delay: %v", err)
	}

	got := getFlow(t, store, f.ItemID)
	if got.State != domain.FlowStateActive {
		t.Fatalf("expected active, got %s", got.State)
	}
	if got.HITLOutcome != "delay" {
		t.Fatalf("expected delay outcome, got %s", got.HITLOutcome)
	}
	if !got.NextActionAt.Equal(delayUntil) {
		t.Fatalf("expected next_action_at %s, got %s", delayUntil, got.NextActionAt)
	}
}

func TestPlayed(t *testing.T) {
	store := testStore(t)
	mgr := newTestFlowManager()
	now := time.Date(2026, 4, 10, 14, 0, 0, 0, time.UTC)
	nextEval := now.Add(30 * 24 * time.Hour)

	f := baseFlow("target:movie:ply", domain.FlowStatePendingReview)
	f.Discord = domain.DiscordContext{ChannelID: "ch2", MessageID: "msg2"}
	seedFlow(t, store, f)

	var result *TransitionResult
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow, _, _ := tx.GetFlow(context.Background(), f.ItemID)
		var err error
		result, err = mgr.Played(context.Background(), tx, &flow, now, nextEval, "test:eval")
		return err
	})
	if err != nil {
		t.Fatalf("played: %v", err)
	}

	got := getFlow(t, store, f.ItemID)
	if got.State != domain.FlowStateActive {
		t.Fatalf("expected active, got %s", got.State)
	}
	if got.HITLOutcome != "played" {
		t.Fatalf("expected played outcome, got %s", got.HITLOutcome)
	}
	if result.FinalizePrompt == nil {
		t.Fatal("expected finalization for played recovery")
	}
}

func TestRequestReview(t *testing.T) {
	store := testStore(t)
	mgr := newTestFlowManager()
	now := time.Date(2026, 4, 10, 14, 0, 0, 0, time.UTC)

	f := baseFlow("target:movie:rev", domain.FlowStateActive)
	seedFlow(t, store, f)

	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow, _, _ := tx.GetFlow(context.Background(), f.ItemID)
		_, err := mgr.RequestReview(context.Background(), tx, &flow, now)
		return err
	})
	if err != nil {
		t.Fatalf("request review: %v", err)
	}

	got := getFlow(t, store, f.ItemID)
	if got.State != domain.FlowStatePendingReview {
		t.Fatalf("expected pending_review, got %s", got.State)
	}
}

func TestRollbackToActive(t *testing.T) {
	store := testStore(t)
	mgr := newTestFlowManager()
	now := time.Date(2026, 4, 10, 14, 0, 0, 0, time.UTC)
	retryAt := now.Add(10 * time.Minute)

	f := baseFlow("target:movie:rb", domain.FlowStatePendingReview)
	seedFlow(t, store, f)

	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow, _, _ := tx.GetFlow(context.Background(), f.ItemID)
		_, err := mgr.RollbackToActive(context.Background(), tx, &flow, now, retryAt, "recovery")
		return err
	})
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}

	got := getFlow(t, store, f.ItemID)
	if got.State != domain.FlowStateActive {
		t.Fatalf("expected active, got %s", got.State)
	}
	if !got.NextActionAt.Equal(retryAt) {
		t.Fatalf("expected next_action_at %s, got %s", retryAt, got.NextActionAt)
	}
}

func TestUnarchive(t *testing.T) {
	store := testStore(t)
	mgr := newTestFlowManager()
	now := time.Date(2026, 4, 10, 14, 0, 0, 0, time.UTC)

	f := baseFlow("target:movie:ua", domain.FlowStateArchived)
	seedFlow(t, store, f)

	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow, _, _ := tx.GetFlow(context.Background(), f.ItemID)
		_, err := mgr.Unarchive(context.Background(), tx, &flow, now, "test_unarchive")
		return err
	})
	if err != nil {
		t.Fatalf("unarchive: %v", err)
	}

	got := getFlow(t, store, f.ItemID)
	if got.State != domain.FlowStateActive {
		t.Fatalf("expected active, got %s", got.State)
	}
}
