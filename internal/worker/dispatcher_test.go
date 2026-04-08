package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/jobs"
	"jellyreaper/internal/repo"
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
