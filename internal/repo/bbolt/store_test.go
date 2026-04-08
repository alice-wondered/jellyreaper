package bbolt

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	bboltlib "go.etcd.io/bbolt"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/repo"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store.db")
	store, err := Open(path, 0o600, &bboltlib.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = os.Remove(path) })
	return store
}

func TestLeaseDueJobsOnlyLeasesPendingDue(t *testing.T) {
	store := testStore(t)
	now := time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC)

	err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.EnqueueJob(context.Background(), domain.JobRecord{JobID: "due", ItemID: "i1", Kind: domain.JobKindEvaluatePolicy, Status: domain.JobStatusPending, RunAt: now.Add(-time.Minute)}); err != nil {
			return err
		}
		if err := tx.EnqueueJob(context.Background(), domain.JobRecord{JobID: "future", ItemID: "i2", Kind: domain.JobKindEvaluatePolicy, Status: domain.JobStatusPending, RunAt: now.Add(time.Hour)}); err != nil {
			return err
		}
		return tx.EnqueueJob(context.Background(), domain.JobRecord{JobID: "completed", ItemID: "i3", Kind: domain.JobKindEvaluatePolicy, Status: domain.JobStatusCompleted, RunAt: now.Add(-time.Minute)})
	})
	if err != nil {
		t.Fatalf("seed jobs: %v", err)
	}

	leased, err := store.LeaseDueJobs(context.Background(), now, 10, "w1", time.Minute)
	if err != nil {
		t.Fatalf("lease due jobs: %v", err)
	}
	if len(leased) != 1 || leased[0].JobID != "due" {
		t.Fatalf("unexpected leased jobs: %#v", leased)
	}
}

func TestFailJobRequeuesPending(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.EnqueueJob(context.Background(), domain.JobRecord{JobID: "job1", ItemID: "i1", Kind: domain.JobKindEvaluatePolicy, Status: domain.JobStatusPending, RunAt: now})
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}

	leased, err := store.LeaseDueJobs(context.Background(), now, 1, "worker", time.Minute)
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease failed: err=%v leased=%d", err, len(leased))
	}

	retryAt := now.Add(time.Minute)
	if err := store.FailJob(context.Background(), "job1", "boom", retryAt, false); err != nil {
		t.Fatalf("fail job: %v", err)
	}

	next, ok, err := store.GetNextDueAt(context.Background())
	if err != nil {
		t.Fatalf("get next due: %v", err)
	}
	if !ok || !next.Equal(retryAt) {
		t.Fatalf("unexpected next due: ok=%v due=%s retryAt=%s", ok, next, retryAt)
	}
}

func TestLeaseDueJobsReclaimsExpiredLeases(t *testing.T) {
	store := testStore(t)
	now := time.Now().UTC()

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.EnqueueJob(context.Background(), domain.JobRecord{JobID: "job-expired", ItemID: "i1", Kind: domain.JobKindEvaluatePolicy, Status: domain.JobStatusPending, RunAt: now})
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}

	leased, err := store.LeaseDueJobs(context.Background(), now, 1, "worker-a", 15*time.Millisecond)
	if err != nil {
		t.Fatalf("initial lease: %v", err)
	}
	if len(leased) != 1 {
		t.Fatalf("expected one leased job, got %d", len(leased))
	}

	time.Sleep(20 * time.Millisecond)

	reclaimed, err := store.LeaseDueJobs(context.Background(), time.Now().UTC(), 1, "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("reclaim lease: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].JobID != "job-expired" {
		t.Fatalf("expected reclaimed job, got %#v", reclaimed)
	}
}

func TestOpenReconcilesPersistedQueueAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.db")

	store, err := Open(path, 0o600, &bboltlib.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	now := time.Now().UTC()
	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.EnqueueJob(context.Background(), domain.JobRecord{JobID: "job-restart", ItemID: "i-restart", Kind: domain.JobKindEvaluatePolicy, Status: domain.JobStatusPending, RunAt: now}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed restart job: %v", err)
	}

	leased, err := store.LeaseDueJobs(context.Background(), now, 1, "worker-a", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("lease before restart: %v", err)
	}
	if len(leased) != 1 {
		t.Fatalf("expected one leased job before restart, got %d", len(leased))
	}

	_ = store.Close()
	time.Sleep(15 * time.Millisecond)

	reopened, err := Open(path, 0o600, &bboltlib.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	jobs, err := reopened.LeaseDueJobs(context.Background(), time.Now().UTC(), 10, "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("lease after restart: %v", err)
	}
	if len(jobs) != 1 || jobs[0].JobID != "job-restart" {
		t.Fatalf("expected restored job after restart, got %#v", jobs)
	}
}
