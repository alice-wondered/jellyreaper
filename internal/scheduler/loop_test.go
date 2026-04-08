package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/repo"
)

type fakeRepo struct {
	mu      sync.Mutex
	jobs    []domain.JobRecord
	nextDue time.Time
	hasDue  bool
}

func (f *fakeRepo) WithTx(context.Context, func(repo.TxRepository) error) error { return nil }
func (f *fakeRepo) CompleteJob(context.Context, string, time.Time) error        { return nil }
func (f *fakeRepo) FailJob(context.Context, string, string, time.Time, bool) error {
	return nil
}

func (f *fakeRepo) LeaseDueJobs(_ context.Context, _ time.Time, _ int, _ string, _ time.Duration) ([]domain.JobRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]domain.JobRecord, len(f.jobs))
	copy(out, f.jobs)
	f.jobs = nil
	return out, nil
}

func (f *fakeRepo) GetNextDueAt(context.Context) (time.Time, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nextDue, f.hasDue, nil
}

func (f *fakeRepo) GetNextQueuedJob(context.Context) (domain.JobRecord, bool, error) {
	return domain.JobRecord{}, false, nil
}

func TestLoop_LeaseAndDispatchCallsDispatch(t *testing.T) {
	repo := &fakeRepo{jobs: []domain.JobRecord{{JobID: "job1"}, {JobID: "job2"}}}

	var got []string
	loop := NewLoop(repo, func(_ context.Context, job domain.JobRecord) error {
		got = append(got, job.JobID)
		return nil
	}, nil, Config{})

	if err := loop.leaseAndDispatch(context.Background(), time.Now()); err != nil {
		t.Fatalf("leaseAndDispatch error: %v", err)
	}

	if len(got) != 2 || got[0] != "job1" || got[1] != "job2" {
		t.Fatalf("unexpected dispatch order: %#v", got)
	}
}

func TestLoop_SleepUntilWakesOnSignal(t *testing.T) {
	signal := make(chan time.Time, 1)
	loop := NewLoop(&fakeRepo{}, func(context.Context, domain.JobRecord) error { return nil }, nil, Config{Signal: signal})

	start := time.Now()
	go func() {
		time.Sleep(20 * time.Millisecond)
		signal <- time.Now()
	}()

	if err := loop.sleepUntil(context.Background(), time.Now().Add(2*time.Second)); err != nil {
		t.Fatalf("sleepUntil error: %v", err)
	}

	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("expected early wake, elapsed=%s", elapsed)
	}
}

func TestLoop_ComputeNextWakePrefersSoonest(t *testing.T) {
	now := time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC)
	repo := &fakeRepo{nextDue: now.Add(2 * time.Second), hasDue: true}
	loop := NewLoop(repo, func(context.Context, domain.JobRecord) error { return nil }, nil, Config{IdlePoll: 5 * time.Second})
	loop.now = func() time.Time { return now }
	loop.wakeHints.Push(now.Add(1 * time.Second))

	next := loop.computeNextWake(context.Background(), now)
	if want := now.Add(1 * time.Second); !next.Equal(want) {
		t.Fatalf("unexpected next wake: got=%s want=%s", next, want)
	}
}
