package worker

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	bbolt "go.etcd.io/bbolt"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/jobs"
	"jellyreaper/internal/repo"
	bboltrepo "jellyreaper/internal/repo/bbolt"
)

type countingHandler struct {
	kind  domain.JobKind
	calls *int
}

func (h countingHandler) Kind() domain.JobKind { return h.kind }
func (h countingHandler) Handle(context.Context, domain.JobRecord) error {
	*h.calls++
	return nil
}

func TestDeleteGateDefersDeleteKindsWithoutBurningAttempts(t *testing.T) {
	store, err := bboltrepo.Open(filepath.Join(t.TempDir(), "gate.db"), 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	kinds := []domain.JobKind{domain.JobKindHITLTimeout, domain.JobKindExecuteDelete, domain.JobKindEvaluatePolicy}
	calls := map[domain.JobKind]*int{}
	handlers := make([]jobs.JobHandler, 0, len(kinds))
	if err := store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		for _, kind := range kinds {
			n := 0
			calls[kind] = &n
			handlers = append(handlers, countingHandler{kind: kind, calls: &n})
			if err := tx.EnqueueJob(ctx, domain.JobRecord{JobID: "job:" + string(kind), ItemID: "item", Kind: kind, Status: domain.JobStatusPending, RunAt: now.Add(-time.Hour), Attempts: 2, MaxAttempts: 5, IdempotencyKey: string(kind), CreatedAt: now}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed jobs: %v", err)
	}
	reg, err := jobs.NewRegistry(handlers...)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	d := NewDispatcher(store, reg, nil)
	d.now = func() time.Time { return now }
	d.SetDeleteGate(DeleteGate{Allowed: func() bool { return false }, RecheckIn: time.Minute})

	leased, err := store.LeaseDueJobs(context.Background(), now, 10, "w", time.Minute)
	if err != nil || len(leased) != len(kinds) {
		t.Fatalf("lease: got %d jobs, err %v", len(leased), err)
	}
	for _, job := range leased {
		if err := d.Dispatch(context.Background(), job); err != nil {
			t.Fatalf("dispatch %s: %v", job.Kind, err)
		}
	}

	for _, kind := range kinds[:2] {
		if *calls[kind] != 0 {
			t.Fatalf("%s ran through a closed gate", kind)
		}
		var job domain.JobRecord
		_ = store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
			job, _, err = tx.GetJob(ctx, "job:"+string(kind))
			return err
		})
		if job.Status != domain.JobStatusPending || job.Attempts != 2 || !job.RunAt.Equal(now.Add(time.Minute)) || job.LeaseOwner != "" {
			t.Fatalf("%s: want pending, attempts=2, run_at=now+1m, no lease; got %+v", kind, job)
		}
	}
	if *calls[domain.JobKindEvaluatePolicy] != 1 {
		t.Fatal("evaluate_policy must not be gated")
	}
	if next, ok, _ := store.GetNextDueAt(context.Background()); !ok || !next.Equal(now.Add(time.Minute)) {
		t.Fatalf("deferred jobs must be back in the due index at now+1m, got %v ok=%v", next, ok)
	}

	d.SetDeleteGate(DeleteGate{Allowed: func() bool { return true }, RecheckIn: time.Minute})
	leased, _ = store.LeaseDueJobs(context.Background(), now.Add(time.Minute), 10, "w", time.Minute)
	for _, job := range leased {
		_ = d.Dispatch(context.Background(), job)
	}
	if *calls[domain.JobKindHITLTimeout] != 1 || *calls[domain.JobKindExecuteDelete] != 1 {
		t.Fatal("open gate must run the previously deferred jobs")
	}
}
