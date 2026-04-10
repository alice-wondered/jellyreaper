package scheduler

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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

func seedMedia(t *testing.T, store *bboltrepo.Store, item domain.MediaItem) {
	t.Helper()
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.UpsertMedia(context.Background(), item)
	})
	if err != nil {
		t.Fatalf("seed media: %v", err)
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

func getNextJob(t *testing.T, store *bboltrepo.Store) (domain.JobRecord, bool) {
	t.Helper()
	job, found, err := store.GetNextQueuedJob(context.Background())
	if err != nil {
		t.Fatalf("get next queued job: %v", err)
	}
	return job, found
}

func newMgr(t *testing.T, store *bboltrepo.Store) *FlowManager {
	t.Helper()
	mgr := NewFlowManager(store, NewScheduler(nil, nil), nil)
	mgr.SetNowFunc(func() time.Time {
		return time.Date(2026, 4, 10, 14, 0, 0, 0, time.UTC)
	})
	return mgr
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

// --- Archive ---

func TestArchive_TransitionsAndBumpsVersion(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)

	f := baseFlow("target:movie:arch", domain.FlowStateActive)
	seedFlow(t, store, f)

	result, err := mgr.Archive(context.Background(), f.ItemID, testSrc)
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if result.Flow.State != domain.FlowStateArchived {
		t.Fatalf("expected archived in result, got %s", result.Flow.State)
	}
	if result.Flow.Version != 1 {
		t.Fatalf("expected version 1, got %d", result.Flow.Version)
	}

	got := getFlow(t, store, f.ItemID)
	if got.State != domain.FlowStateArchived {
		t.Fatalf("expected archived in store, got %s", got.State)
	}
	if got.HITLOutcome != "archive" {
		t.Fatalf("expected archive outcome, got %s", got.HITLOutcome)
	}
	if !got.UpdatedAt.Equal(time.Date(2026, 4, 10, 14, 0, 0, 0, time.UTC)) {
		t.Fatalf("expected UpdatedAt from injected clock, got %s", got.UpdatedAt)
	}
}

func TestArchive_PendingReviewCapturesFinalization(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)

	f := baseFlow("target:movie:arch-pr", domain.FlowStatePendingReview)
	f.Discord = domain.DiscordContext{ChannelID: "ch-a", MessageID: "msg-a"}
	seedFlow(t, store, f)

	result, err := mgr.Archive(context.Background(), f.ItemID, TransitionSource{Source: "ai"})
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if result.FinalizePrompt == nil {
		t.Fatal("expected finalization for pending_review → archived")
	}
	if result.FinalizePrompt.ChannelID != "ch-a" {
		t.Fatalf("expected ch-a, got %s", result.FinalizePrompt.ChannelID)
	}
	if !strings.Contains(result.FinalizePrompt.Content, "ARCHIVED") {
		t.Fatalf("expected ARCHIVED in content, got %s", result.FinalizePrompt.Content)
	}
	if !strings.Contains(result.FinalizePrompt.Content, "(AI)") {
		t.Fatalf("expected (AI) suffix, got %s", result.FinalizePrompt.Content)
	}
}

func TestArchive_ActiveHasNoFinalization(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)

	f := baseFlow("target:movie:arch-act", domain.FlowStateActive)
	seedFlow(t, store, f)

	result, err := mgr.Archive(context.Background(), f.ItemID, testSrc)
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if result.FinalizePrompt != nil {
		t.Fatal("expected no finalization for active → archived")
	}
}

func TestArchive_RejectsDeleteQueued(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)

	f := baseFlow("target:movie:dq", domain.FlowStateDeleteQueued)
	seedFlow(t, store, f)

	_, err := mgr.Archive(context.Background(), f.ItemID, testSrc)
	if err == nil {
		t.Fatal("expected error archiving delete_queued flow")
	}
}

func TestArchive_IdempotentOnAlreadyArchived(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)

	f := baseFlow("target:movie:arch-idem", domain.FlowStateArchived)
	seedFlow(t, store, f)

	result, err := mgr.Archive(context.Background(), f.ItemID, testSrc)
	if err != nil {
		t.Fatalf("expected idempotent success, got %v", err)
	}
	if result.Flow.Version != 0 {
		t.Fatalf("expected no version bump for idempotent archive, got %d", result.Flow.Version)
	}
}

// --- Delete ---

func TestDelete_CreatesExecuteDeleteJob(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)

	f := baseFlow("target:movie:del-job", domain.FlowStateActive)
	seedFlow(t, store, f)

	result, err := mgr.Delete(context.Background(), f.ItemID, testSrc)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if result.Flow.State != domain.FlowStateDeleteQueued {
		t.Fatalf("expected delete_queued, got %s", result.Flow.State)
	}

	job, found := getNextJob(t, store)
	if !found {
		t.Fatal("expected execute_delete job to be enqueued")
	}
	if job.Kind != domain.JobKindExecuteDelete {
		t.Fatalf("expected execute_delete, got %s", job.Kind)
	}
	if job.ItemID != f.ItemID {
		t.Fatalf("expected item %s, got %s", f.ItemID, job.ItemID)
	}
}

func TestDelete_PendingReviewCapturesFinalization(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)

	f := baseFlow("target:movie:del-fin", domain.FlowStatePendingReview)
	f.Discord = domain.DiscordContext{ChannelID: "ch-d", MessageID: "msg-d"}
	seedFlow(t, store, f)

	result, err := mgr.Delete(context.Background(), f.ItemID, TransitionSource{Source: "discord"})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if result.FinalizePrompt == nil {
		t.Fatal("expected finalization for pending_review → delete_queued")
	}
	if !strings.Contains(result.FinalizePrompt.Content, "DELETE REQUESTED") {
		t.Fatalf("expected DELETE REQUESTED, got %s", result.FinalizePrompt.Content)
	}
}

// --- Delay ---

func TestDelay_TransitionsToActiveWithFutureEval(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)
	fixedNow := time.Date(2026, 4, 10, 14, 0, 0, 0, time.UTC)

	f := baseFlow("target:movie:dly", domain.FlowStatePendingReview)
	seedFlow(t, store, f)

	result, err := mgr.Delay(context.Background(), f.ItemID, DelayRequest{
		Days:             15,
		ExpireAfterDays:  15,
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
	expectedNext := fixedNow.Add(15 * 24 * time.Hour)
	if !got.NextActionAt.Equal(expectedNext) {
		t.Fatalf("expected next_action_at %s, got %s", expectedNext, got.NextActionAt)
	}
	if got.PolicySnapshot.ExpireAfterDays != 15 {
		t.Fatalf("expected policy days 15, got %d", got.PolicySnapshot.ExpireAfterDays)
	}

	// Eval job should be scheduled.
	job, found := getNextJob(t, store)
	if !found {
		t.Fatal("expected eval job after delay")
	}
	if job.Kind != domain.JobKindEvaluatePolicy {
		t.Fatalf("expected evaluate_policy, got %s", job.Kind)
	}
}

func TestDelay_FinalizationIncludesDays(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)

	f := baseFlow("target:movie:dly-fin", domain.FlowStatePendingReview)
	f.Discord = domain.DiscordContext{ChannelID: "ch-dl", MessageID: "msg-dl"}
	seedFlow(t, store, f)

	result, err := mgr.Delay(context.Background(), f.ItemID, DelayRequest{
		Days:             7,
		TransitionSource: TransitionSource{Source: "ai"},
	})
	if err != nil {
		t.Fatalf("delay: %v", err)
	}
	if result.FinalizePrompt == nil {
		t.Fatal("expected finalization")
	}
	if !strings.Contains(result.FinalizePrompt.Content, "DELAYED 7 days") {
		t.Fatalf("expected '7 days' in content, got %s", result.FinalizePrompt.Content)
	}
}

// --- RequestReview ---

func TestRequestReview_CreatesPromptJob(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)

	f := baseFlow("target:movie:rev-job", domain.FlowStateActive)
	seedFlow(t, store, f)

	result, err := mgr.RequestReview(context.Background(), f.ItemID, testSrc)
	if err != nil {
		t.Fatalf("request review: %v", err)
	}
	if result.Flow.State != domain.FlowStatePendingReview {
		t.Fatalf("expected pending_review, got %s", result.Flow.State)
	}

	job, found := getNextJob(t, store)
	if !found {
		t.Fatal("expected send_hitl_prompt job")
	}
	if job.Kind != domain.JobKindSendHITLPrompt {
		t.Fatalf("expected send_hitl_prompt, got %s", job.Kind)
	}
}

func TestRequestReview_IdempotentOnPendingReview(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)

	f := baseFlow("target:movie:rev-idem", domain.FlowStatePendingReview)
	seedFlow(t, store, f)

	result, err := mgr.RequestReview(context.Background(), f.ItemID, testSrc)
	if err != nil {
		t.Fatalf("expected idempotent success, got %v", err)
	}
	if result.Flow.Version != 0 {
		t.Fatalf("expected no version bump, got %d", result.Flow.Version)
	}
}

// --- Played ---

func TestPlayed_PurgesJobsAndReschedulesEval(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)

	f := baseFlow("target:movie:ply-jobs", domain.FlowStatePendingReview)
	f.Discord = domain.DiscordContext{ChannelID: "ch-p", MessageID: "msg-p"}
	seedFlow(t, store, f)

	// Seed a stale HITL timeout job that should be purged.
	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.EnqueueJob(context.Background(), domain.JobRecord{
			JobID:  "job:timeout:stale",
			ItemID: f.ItemID,
			Kind:   domain.JobKindHITLTimeout,
			Status: domain.JobStatusPending,
			RunAt:  time.Now().UTC(),
		})
	})
	if err != nil {
		t.Fatalf("seed stale job: %v", err)
	}

	result, err := mgr.Played(context.Background(), f.ItemID, PlayedRequest{
		TransitionSource: testSrc,
	})
	if err != nil {
		t.Fatalf("played: %v", err)
	}
	if result.Flow.State != domain.FlowStateActive {
		t.Fatalf("expected active, got %s", result.Flow.State)
	}
	if result.Flow.HITLOutcome != "played" {
		t.Fatalf("expected played outcome, got %s", result.Flow.HITLOutcome)
	}
	if result.FinalizePrompt == nil {
		t.Fatal("expected finalization")
	}

	// The stale timeout job should be purged; the eval singleton should be the only job.
	job, found := getNextJob(t, store)
	if !found {
		t.Fatal("expected eval job after played recovery")
	}
	if job.Kind != domain.JobKindEvaluatePolicy {
		t.Fatalf("expected evaluate_policy (stale timeout should be purged), got %s", job.Kind)
	}
}

func TestPlayed_AutoComputesNextEvalFromMedia(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)
	fixedNow := time.Date(2026, 4, 10, 14, 0, 0, 0, time.UTC)

	f := baseFlow("target:movie:ply-auto", domain.FlowStatePendingReview)
	f.PolicySnapshot.ExpireAfterDays = 60
	seedFlow(t, store, f)

	// Seed media with a recent play.
	playedAt := fixedNow.Add(-10 * 24 * time.Hour) // 10 days ago
	seedMedia(t, store, domain.MediaItem{
		ItemID:       "ply-auto",
		ItemType:     "Movie",
		LastPlayedAt: playedAt,
		CreatedAt:    fixedNow,
		UpdatedAt:    fixedNow,
	})

	result, err := mgr.Played(context.Background(), f.ItemID, PlayedRequest{
		TransitionSource: testSrc,
	})
	if err != nil {
		t.Fatalf("played: %v", err)
	}

	// nextEvalAt should be playedAt + 60 days = fixedNow + 50 days
	expectedNext := playedAt.Add(60 * 24 * time.Hour)
	got := getFlow(t, store, f.ItemID)
	if !got.NextActionAt.Equal(expectedNext) {
		t.Fatalf("expected auto-computed next_action_at %s, got %s", expectedNext, got.NextActionAt)
	}
	if !result.WakeAt.Equal(expectedNext) {
		t.Fatalf("expected WakeAt %s, got %s", expectedNext, result.WakeAt)
	}
}

// --- Unarchive ---

func TestUnarchive_SchedulesImmediateEval(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)

	f := baseFlow("target:movie:ua-eval", domain.FlowStateArchived)
	seedFlow(t, store, f)

	result, err := mgr.Unarchive(context.Background(), f.ItemID, testSrc)
	if err != nil {
		t.Fatalf("unarchive: %v", err)
	}
	if result.Flow.State != domain.FlowStateActive {
		t.Fatalf("expected active, got %s", result.Flow.State)
	}

	job, found := getNextJob(t, store)
	if !found {
		t.Fatal("expected eval job after unarchive")
	}
	if job.Kind != domain.JobKindEvaluatePolicy {
		t.Fatalf("expected evaluate_policy, got %s", job.Kind)
	}
}

func TestUnarchive_RejectsNonArchived(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)

	f := baseFlow("target:movie:ua-bad", domain.FlowStateActive)
	seedFlow(t, store, f)

	_, err := mgr.Unarchive(context.Background(), f.ItemID, testSrc)
	if err == nil {
		t.Fatal("expected error unarchiving non-archived flow")
	}
}

// --- RollbackToActive ---

func TestRollbackToActive_SchedulesCooldownEval(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)
	fixedNow := time.Date(2026, 4, 10, 14, 0, 0, 0, time.UTC)

	f := baseFlow("target:movie:rb-eval", domain.FlowStatePendingReview)
	seedFlow(t, store, f)

	result, err := mgr.RollbackToActive(context.Background(), f.ItemID, 10*time.Minute, testSrc)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if result.Flow.State != domain.FlowStateActive {
		t.Fatalf("expected active, got %s", result.Flow.State)
	}

	got := getFlow(t, store, f.ItemID)
	expectedRetry := fixedNow.Add(10 * time.Minute)
	if !got.NextActionAt.Equal(expectedRetry) {
		t.Fatalf("expected next_action_at %s, got %s", expectedRetry, got.NextActionAt)
	}
}

// --- Edge cases ---

func TestFlowNotFound(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)

	_, err := mgr.Archive(context.Background(), "target:movie:ghost", testSrc)
	if err == nil {
		t.Fatal("expected error for nonexistent flow")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected 'not found' in error, got %s", err.Error())
	}
}

func TestTransitionSourcePropagatedToFinalization(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)

	f := baseFlow("target:movie:src-prop", domain.FlowStatePendingReview)
	f.Discord = domain.DiscordContext{ChannelID: "ch", MessageID: "msg"}
	seedFlow(t, store, f)

	result, err := mgr.Delete(context.Background(), f.ItemID, TransitionSource{Source: "discord", Actor: "alice"})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if result.FinalizePrompt == nil {
		t.Fatal("expected finalization")
	}
	if !strings.Contains(result.FinalizePrompt.Content, "(DISCORD)") {
		t.Fatalf("expected source in finalization, got %s", result.FinalizePrompt.Content)
	}
}

// --- Full lifecycle ---

func TestFullLifecycle_ActiveToReviewToDelayToReviewToDelete(t *testing.T) {
	store := newTestStore(t)
	mgr := newMgr(t, store)

	f := baseFlow("target:movie:lifecycle", domain.FlowStateActive)
	seedFlow(t, store, f)

	// Step 1: Request review
	r1, err := mgr.RequestReview(context.Background(), f.ItemID, TransitionSource{Source: "scheduler"})
	if err != nil {
		t.Fatalf("request review: %v", err)
	}
	if r1.Flow.State != domain.FlowStatePendingReview {
		t.Fatalf("step 1: expected pending_review, got %s", r1.Flow.State)
	}

	// Step 2: Delay
	r2, err := mgr.Delay(context.Background(), f.ItemID, DelayRequest{
		Days:             7,
		TransitionSource: TransitionSource{Source: "discord"},
	})
	if err != nil {
		t.Fatalf("delay: %v", err)
	}
	if r2.Flow.State != domain.FlowStateActive {
		t.Fatalf("step 2: expected active, got %s", r2.Flow.State)
	}

	// Step 3: Request review again
	r3, err := mgr.RequestReview(context.Background(), f.ItemID, TransitionSource{Source: "scheduler"})
	if err != nil {
		t.Fatalf("request review 2: %v", err)
	}
	if r3.Flow.State != domain.FlowStatePendingReview {
		t.Fatalf("step 3: expected pending_review, got %s", r3.Flow.State)
	}

	// Step 4: Delete
	r4, err := mgr.Delete(context.Background(), f.ItemID, TransitionSource{Source: "discord"})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if r4.Flow.State != domain.FlowStateDeleteQueued {
		t.Fatalf("step 4: expected delete_queued, got %s", r4.Flow.State)
	}

	// Verify version was bumped through all transitions.
	got := getFlow(t, store, f.ItemID)
	if got.Version < 4 {
		t.Fatalf("expected version >= 4 after 4 transitions, got %d", got.Version)
	}
}
