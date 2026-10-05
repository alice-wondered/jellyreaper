package scheduler

import (
	"context"
	"testing"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/repo"
)

func TestRequestEvalIsANoOpWhenAlreadyScheduledIdentically(t *testing.T) {
	store := newTestStore(t)
	sched := NewScheduler(nil, nil)
	flow := domain.Flow{FlowID: "flow:target:season:s1", ItemID: "target:season:s1", Version: 4}
	due := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	request := func(now time.Time, runAt time.Time, version int64) domain.JobRecord {
		var job domain.JobRecord
		if err := store.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
			if err := sched.RequestEval(ctx, tx, flow, now, runAt, "test", "k", version); err != nil {
				return err
			}
			job, _, _ = tx.GetJob(ctx, "job:eval:scheduled:"+flow.ItemID)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return job
	}
	first := request(time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC), due, 4)
	same := request(time.Date(2026, 10, 5, 0, 0, 1, 0, time.UTC), due, 4)
	if !same.UpdatedAt.Equal(first.UpdatedAt) {
		t.Fatalf("identical request rewrote the job: %v → %v", first.UpdatedAt, same.UpdatedAt)
	}
	moved := request(time.Date(2026, 10, 5, 0, 0, 2, 0, time.UTC), due.Add(24*time.Hour), 5)
	if !moved.RunAt.Equal(due.Add(24 * time.Hour)) {
		t.Fatalf("a real change must still be written: %v", moved.RunAt)
	}
}
