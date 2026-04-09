package bbolt

import (
	"context"
	"fmt"
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
	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		job, found, err := tx.GetJob(context.Background(), "job1")
		if err != nil {
			return err
		}
		if !found {
			t.Fatal("expected failed job to exist")
		}
		if job.Attempts != 1 {
			t.Fatalf("expected attempts incremented to 1, got %d", job.Attempts)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify failed job attempts: %v", err)
	}

	next, ok, err := store.GetNextDueAt(context.Background())
	if err != nil {
		t.Fatalf("get next due: %v", err)
	}
	if !ok || !next.Equal(retryAt) {
		t.Fatalf("unexpected next due: ok=%v due=%s retryAt=%s", ok, next, retryAt)
	}
}

func TestGetNextQueuedJobReturnsEarliestPendingJob(t *testing.T) {
	store := testStore(t)
	now := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		if err := tx.EnqueueJob(context.Background(), domain.JobRecord{JobID: "job-late", ItemID: "i-late", Kind: domain.JobKindEvaluatePolicy, Status: domain.JobStatusPending, RunAt: now.Add(2 * time.Hour)}); err != nil {
			return err
		}
		if err := tx.EnqueueJob(context.Background(), domain.JobRecord{JobID: "job-early", ItemID: "i-early", Kind: domain.JobKindEvaluatePolicy, Status: domain.JobStatusPending, RunAt: now.Add(time.Hour)}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed jobs: %v", err)
	}

	job, found, err := store.GetNextQueuedJob(context.Background())
	if err != nil {
		t.Fatalf("get next queued job: %v", err)
	}
	if !found {
		t.Fatal("expected next queued job")
	}
	if job.JobID != "job-early" {
		t.Fatalf("expected earliest job, got %s", job.JobID)
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

func TestLeaseDueJobsCleansStaleDueIndexEntries(t *testing.T) {
	store := testStore(t)
	now := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)

	if err := store.WithTx(context.Background(), func(tx repo.TxRepository) error {
		return tx.EnqueueJob(context.Background(), domain.JobRecord{
			JobID:     "job-real",
			ItemID:    "item-real",
			Kind:      domain.JobKindEvaluatePolicy,
			Status:    domain.JobStatusPending,
			RunAt:     now,
			FlowID:    "flow:item-real",
			CreatedAt: now,
			UpdatedAt: now,
		})
	}); err != nil {
		t.Fatalf("seed real job: %v", err)
	}

	if err := store.db.Update(func(tx *bboltlib.Tx) error {
		due, err := requireBucket(tx, bucketDueIndex)
		if err != nil {
			return err
		}
		return due.Put(dueIndexKeyBytes(now.Add(-time.Minute), "job-missing"), keyBytes("job-missing"))
	}); err != nil {
		t.Fatalf("insert stale due index entry: %v", err)
	}

	leased, err := store.LeaseDueJobs(context.Background(), now, 10, "worker", time.Minute)
	if err != nil {
		t.Fatalf("lease due jobs: %v", err)
	}
	if len(leased) != 1 || leased[0].JobID != "job-real" {
		t.Fatalf("expected only real job leased, got %#v", leased)
	}

	if err := store.db.View(func(tx *bboltlib.Tx) error {
		due, err := requireBucket(tx, bucketDueIndex)
		if err != nil {
			return err
		}
		k := dueIndexKeyBytes(now.Add(-time.Minute), "job-missing")
		if due.Get(k) != nil {
			t.Fatalf("expected stale due index key to be removed")
		}
		return nil
	}); err != nil {
		t.Fatalf("verify stale due index removal: %v", err)
	}
}

func TestSearchFlowsUsesTrigramIndex(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := store.WithTx(ctx, func(tx repo.TxRepository) error {
		if err := tx.UpsertFlowCAS(ctx, domain.Flow{FlowID: "flow:target:series:office", ItemID: "target:series:office", SubjectType: "series", DisplayName: "The Office", Version: 0, CreatedAt: now, UpdatedAt: now}, 0); err != nil {
			return err
		}
		if err := tx.UpsertFlowCAS(ctx, domain.Flow{FlowID: "flow:target:series:offgrid", ItemID: "target:series:offgrid", SubjectType: "series", DisplayName: "Off Grid", Version: 0, CreatedAt: now, UpdatedAt: now}, 0); err != nil {
			return err
		}
		return tx.UpsertFlowCAS(ctx, domain.Flow{FlowID: "flow:target:movie:office-space", ItemID: "target:movie:office-space", SubjectType: "movie", DisplayName: "Office Space", Version: 0, CreatedAt: now, UpdatedAt: now}, 0)
	}); err != nil {
		t.Fatalf("seed flows: %v", err)
	}

	var series []domain.Flow
	if err := store.WithTx(ctx, func(tx repo.TxRepository) error {
		var err error
		series, err = tx.SearchFlows(ctx, "office", "series", 10)
		return err
	}); err != nil {
		t.Fatalf("search series flows: %v", err)
	}
	if len(series) != 1 || series[0].ItemID != "target:series:office" {
		t.Fatalf("unexpected series search results: %#v", series)
	}

	var all []domain.Flow
	if err := store.WithTx(ctx, func(tx repo.TxRepository) error {
		var err error
		all, err = tx.SearchFlows(ctx, "off", "", 10)
		return err
	}); err != nil {
		t.Fatalf("search all flows: %v", err)
	}
	if len(all) < 2 {
		t.Fatalf("expected multiple fuzzy matches for off, got %#v", all)
	}
}

func TestSearchFlowsIndexUpdatesOnRenameAndDelete(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := store.WithTx(ctx, func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(ctx, domain.Flow{FlowID: "flow:target:series:office", ItemID: "target:series:office", SubjectType: "series", DisplayName: "Parks and Recreation", Version: 0, CreatedAt: now, UpdatedAt: now}, 0)
	}); err != nil {
		t.Fatalf("seed flow: %v", err)
	}

	if err := store.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, "target:series:office")
		if err != nil || !found {
			return fmt.Errorf("get flow before rename: found=%v err=%w", found, err)
		}
		flow.DisplayName = "The Workspace"
		flow.UpdatedAt = now.Add(time.Minute)
		flow.Version++
		return tx.UpsertFlowCAS(ctx, flow, flow.Version-1)
	}); err != nil {
		t.Fatalf("rename flow: %v", err)
	}

	if err := store.WithTx(ctx, func(tx repo.TxRepository) error {
		results, err := tx.SearchFlows(ctx, "parks", "", 10)
		if err != nil {
			return err
		}
		if len(results) != 0 {
			return fmt.Errorf("expected parks search to be empty after rename, got %#v", results)
		}
		results, err = tx.SearchFlows(ctx, "workspace", "", 10)
		if err != nil {
			return err
		}
		if len(results) != 1 || results[0].ItemID != "target:series:office" {
			return fmt.Errorf("unexpected workspace search results: %#v", results)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify search after rename: %v", err)
	}

	if err := store.WithTx(ctx, func(tx repo.TxRepository) error {
		return tx.DeleteFlow(ctx, "target:series:office")
	}); err != nil {
		t.Fatalf("delete flow: %v", err)
	}

	if err := store.WithTx(ctx, func(tx repo.TxRepository) error {
		results, err := tx.SearchFlows(ctx, "workspace", "", 10)
		if err != nil {
			return err
		}
		if len(results) != 0 {
			return fmt.Errorf("expected no results after delete, got %#v", results)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify search after delete: %v", err)
	}
}

func TestSearchFlowsIndexReflectsNewAdditions(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := store.WithTx(ctx, func(tx repo.TxRepository) error {
		results, err := tx.SearchFlows(ctx, "severance", "series", 10)
		if err != nil {
			return err
		}
		if len(results) != 0 {
			return fmt.Errorf("expected empty initial search, got %#v", results)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify initial empty search: %v", err)
	}

	if err := store.WithTx(ctx, func(tx repo.TxRepository) error {
		return tx.UpsertFlowCAS(ctx, domain.Flow{
			FlowID:      "flow:target:series:severance",
			ItemID:      "target:series:severance",
			SubjectType: "series",
			DisplayName: "Severance",
			Version:     0,
			CreatedAt:   now,
			UpdatedAt:   now,
		}, 0)
	}); err != nil {
		t.Fatalf("insert new flow: %v", err)
	}

	if err := store.WithTx(ctx, func(tx repo.TxRepository) error {
		results, err := tx.SearchFlows(ctx, "severance", "series", 10)
		if err != nil {
			return err
		}
		if len(results) != 1 || results[0].ItemID != "target:series:severance" {
			return fmt.Errorf("expected newly added flow to be searchable, got %#v", results)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify search after insert: %v", err)
	}
}

func TestSearchFlowsIndexDeletionKeepsRemainingMatches(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := store.WithTx(ctx, func(tx repo.TxRepository) error {
		if err := tx.UpsertFlowCAS(ctx, domain.Flow{FlowID: "flow:target:series:star-trek", ItemID: "target:series:star-trek", SubjectType: "series", DisplayName: "Star Trek", Version: 0, CreatedAt: now, UpdatedAt: now}, 0); err != nil {
			return err
		}
		return tx.UpsertFlowCAS(ctx, domain.Flow{FlowID: "flow:target:series:star-gate", ItemID: "target:series:star-gate", SubjectType: "series", DisplayName: "Stargate SG-1", Version: 0, CreatedAt: now, UpdatedAt: now}, 0)
	}); err != nil {
		t.Fatalf("seed star flows: %v", err)
	}

	if err := store.WithTx(ctx, func(tx repo.TxRepository) error {
		return tx.DeleteFlow(ctx, "target:series:star-trek")
	}); err != nil {
		t.Fatalf("delete one star flow: %v", err)
	}

	if err := store.WithTx(ctx, func(tx repo.TxRepository) error {
		results, err := tx.SearchFlows(ctx, "star", "series", 10)
		if err != nil {
			return err
		}
		if len(results) != 1 || results[0].ItemID != "target:series:star-gate" {
			return fmt.Errorf("expected remaining star match only, got %#v", results)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify search after partial delete: %v", err)
	}
}
