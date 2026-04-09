package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
			State:       domain.FlowStateDeleteInProgress,
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
