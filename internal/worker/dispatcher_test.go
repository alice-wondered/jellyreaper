package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bbolt "go.etcd.io/bbolt"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/jobs"
	"jellyreaper/internal/repo"
	bboltrepo "jellyreaper/internal/repo/bbolt"
)

type fakeRepo struct {
	completeCalls int
	failCalls     int
	lastTerminal  bool
}

func (f *fakeRepo) WithTx(context.Context, func(repo.TxRepository) error) error { return nil }
func (f *fakeRepo) LeaseDueJobs(context.Context, time.Time, int, string, time.Duration) ([]domain.JobRecord, error) {
	return nil, nil
}
func (f *fakeRepo) GetNextDueAt(context.Context) (time.Time, bool, error) {
	return time.Time{}, false, nil
}
func (f *fakeRepo) GetNextQueuedJob(context.Context) (domain.JobRecord, bool, error) {
	return domain.JobRecord{}, false, nil
}
func (f *fakeRepo) CompleteJob(context.Context, string, time.Time) error {
	f.completeCalls++
	return nil
}
func (f *fakeRepo) FailJob(_ context.Context, _ string, _ string, _ time.Time, terminal bool) error {
	f.failCalls++
	f.lastTerminal = terminal
	return nil
}

type testHandler struct {
	kind domain.JobKind
	err  error
}

func (h testHandler) Kind() domain.JobKind { return h.kind }
func (h testHandler) Handle(context.Context, domain.JobRecord) error {
	return h.err
}

func TestDispatcher_CompletesSuccessfulJob(t *testing.T) {
	r := &fakeRepo{}
	reg, _ := jobs.NewRegistry(testHandler{kind: domain.JobKindEvaluatePolicy})
	d := NewDispatcher(r, reg, nil)

	err := d.Dispatch(context.Background(), domain.JobRecord{JobID: "j1", Kind: domain.JobKindEvaluatePolicy})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}
	if r.completeCalls != 1 || r.failCalls != 0 {
		t.Fatalf("unexpected completion/failure calls: complete=%d fail=%d", r.completeCalls, r.failCalls)
	}
}

func TestDispatcher_FailsUnknownKindTerminally(t *testing.T) {
	r := &fakeRepo{}
	reg, _ := jobs.NewRegistry()
	d := NewDispatcher(r, reg, nil)

	err := d.Dispatch(context.Background(), domain.JobRecord{JobID: "j1", Kind: domain.JobKindEvaluatePolicy})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}
	if r.failCalls != 1 || !r.lastTerminal {
		t.Fatalf("expected one terminal failure, got fail=%d terminal=%v", r.failCalls, r.lastTerminal)
	}
}

func TestDispatcher_RetryNonTerminalHandlerError(t *testing.T) {
	r := &fakeRepo{}
	reg, _ := jobs.NewRegistry(testHandler{kind: domain.JobKindEvaluatePolicy, err: errors.New("boom")})
	d := NewDispatcher(r, reg, nil)
	d.now = func() time.Time { return time.Unix(0, 0) }

	err := d.Dispatch(context.Background(), domain.JobRecord{JobID: "j1", Kind: domain.JobKindEvaluatePolicy, Attempts: 0, MaxAttempts: 3})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}
	if r.failCalls != 1 || r.lastTerminal {
		t.Fatalf("expected non-terminal failure, got fail=%d terminal=%v", r.failCalls, r.lastTerminal)
	}
}

func TestDispatcher_MarksDeleteFlowFailedOnTerminalDeleteError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dispatcher.db")
	store, err := bboltrepo.Open(path, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = os.Remove(path) })

	now := time.Now().UTC()
	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:target:movie:delete-fail",
			ItemID:      "target:movie:delete-fail",
			SubjectType: "movie",
			DisplayName: "Delete Fail",
			State:       domain.FlowStateDeleteQueued,
			Version:     1,
			CreatedAt:   now,
			UpdatedAt:   now,
		}, 0); err != nil {
			return err
		}
		return tx.EnqueueJob(context.Background(), domain.JobRecord{JobID: "job-delete-fail", ItemID: "target:movie:delete-fail", Kind: domain.JobKindExecuteDelete, Status: domain.JobStatusPending, RunAt: now, MaxAttempts: 1})
	}); err != nil {
		t.Fatalf("seed state: %v", err)
	}

	reg, _ := jobs.NewRegistry(testHandler{kind: domain.JobKindExecuteDelete, err: errors.New("boom")})
	d := NewDispatcher(store, reg, nil)
	d.now = func() time.Time { return now }

	if err := d.Dispatch(context.Background(), domain.JobRecord{JobID: "job-delete-fail", ItemID: "target:movie:delete-fail", Kind: domain.JobKindExecuteDelete, Attempts: 0, MaxAttempts: 1}); err != nil {
		t.Fatalf("dispatch terminal delete failure: %v", err)
	}

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(context.Background(), "target:movie:delete-fail")
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected flow to remain present after terminal delete failure")
		}
		if flow.State != domain.FlowStateDeleteFailed {
			t.Fatalf("expected delete_failed state, got %s", flow.State)
		}
		job, found, err := tx.GetJob(context.Background(), "job-delete-fail")
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected failed job to remain present")
		}
		if job.Attempts != 1 {
			t.Fatalf("expected attempts incremented to 1, got %d", job.Attempts)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify delete failure state: %v", err)
	}
}

func TestDispatcher_NotifierFiresOnTerminalDeleteFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dispatcher-notifier.db")
	store, err := bboltrepo.Open(path, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = os.Remove(path) })

	now := time.Now().UTC()
	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.UpsertFlowCAS(context.Background(), domain.Flow{
			FlowID:      "flow:target:movie:notify-fail",
			ItemID:      "target:movie:notify-fail",
			SubjectType: "movie",
			DisplayName: "Notify Fail",
			State:       domain.FlowStateDeleteQueued,
			Version:     1,
			CreatedAt:   now,
			UpdatedAt:   now,
		}, 0); err != nil {
			return err
		}
		return tx.EnqueueJob(context.Background(), domain.JobRecord{JobID: "job-notify-fail", ItemID: "target:movie:notify-fail", Kind: domain.JobKindExecuteDelete, Status: domain.JobStatusPending, RunAt: now, MaxAttempts: 1})
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	notifyCalls := 0
	var lastFlow domain.Flow
	var lastErr error
	reg, _ := jobs.NewRegistry(testHandler{kind: domain.JobKindExecuteDelete, err: errors.New("upstream gone")})
	d := NewDispatcher(store, reg, nil)
	d.now = func() time.Time { return now }
	d.SetDeleteFailedNotifier(func(_ context.Context, flow domain.Flow, err error) {
		notifyCalls++
		lastFlow = flow
		lastErr = err
	})

	if err := d.Dispatch(context.Background(), domain.JobRecord{JobID: "job-notify-fail", ItemID: "target:movie:notify-fail", Kind: domain.JobKindExecuteDelete, Attempts: 0, MaxAttempts: 1}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	if notifyCalls != 1 {
		t.Fatalf("expected notifier to fire exactly once, got %d", notifyCalls)
	}
	if lastFlow.ItemID != "target:movie:notify-fail" {
		t.Fatalf("expected notifier to receive failed flow, got %#v", lastFlow)
	}
	if lastFlow.State != domain.FlowStateDeleteFailed {
		t.Fatalf("expected notifier to see DeleteFailed state, got %s", lastFlow.State)
	}
	if lastErr == nil || !strings.Contains(lastErr.Error(), "upstream gone") {
		t.Fatalf("expected notifier error to carry handler error, got %v", lastErr)
	}
}

func TestDispatcher_RequeuesTerminalHITLPromptJob(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := bboltrepo.Open(path, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = os.Remove(path) })

	// Seed the job that will fail terminally.
	now := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	job := domain.JobRecord{
		JobID:       "job-hitl-fail",
		FlowID:      "flow:target:movie:stuck",
		ItemID:      "target:movie:stuck",
		Kind:        domain.JobKindSendHITLPrompt,
		Status:      domain.JobStatusPending,
		RunAt:       now,
		MaxAttempts: 1,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.EnqueueJob(context.Background(), job)
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}

	reg, _ := jobs.NewRegistry(testHandler{kind: domain.JobKindSendHITLPrompt, err: errors.New("discord unavailable")})
	d := NewDispatcher(store, reg, nil)
	d.now = func() time.Time { return now }

	if err := d.Dispatch(context.Background(), job); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	// Verify a recovery job was enqueued. The original job is now failed,
	// so GetNextQueuedJob should return the recovery job.
	nextJob, found, err := store.GetNextQueuedJob(context.Background())
	if err != nil {
		t.Fatalf("get next queued job: %v", err)
	}
	if !found {
		t.Fatal("expected a recovery job to be enqueued after terminal HITL prompt failure")
	}
	if !strings.HasPrefix(nextJob.JobID, "job:recovery:") {
		t.Fatalf("expected recovery job ID, got %s", nextJob.JobID)
	}
	if nextJob.ItemID != "target:movie:stuck" {
		t.Fatalf("expected recovery job for stuck item, got %s", nextJob.ItemID)
	}
	if nextJob.Status != domain.JobStatusPending {
		t.Fatalf("expected recovery job pending, got %s", nextJob.Status)
	}
	if !nextJob.RunAt.After(now) {
		t.Fatalf("expected recovery job scheduled in the future, got %s", nextJob.RunAt)
	}
}
