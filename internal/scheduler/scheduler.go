package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/jobs"
	"jellyreaper/internal/repo"
)

// EvalRequester schedules evaluate_policy jobs on behalf of callers.
// Implementations must be safe to call from within an open transaction.
//
// flowVersion is the flow.Version observed at the moment the caller decided
// to schedule this eval — typically the post-CAS version from the write the
// caller just made. It is stamped on the eval payload; when the eval handler
// runs it verifies flow.Version still matches and bails (INFO log, no error)
// if not. Pass 0 to skip the version check (used by tests and self-defer
// paths that intentionally accept any version).
type EvalRequester interface {
	RequestEval(ctx context.Context, tx repo.TxRepository, flow domain.Flow, now, runAt time.Time, reason, idempotencyKey string, flowVersion int64) error
}

// Scheduler wraps Loop and provides the sanctioned API for upserting the
// singleton evaluate_policy job on a flow. External systems (webhooks,
// discord interactions, ai decisions, backfill) call RequestEval instead of
// touching job records directly so that rescheduling semantics live in one
// place.
type Scheduler struct {
	*Loop
	wake func(time.Time)
}

// NewScheduler creates a Scheduler from an existing Loop. wake is called
// after a job record is written so the loop wakes promptly at the right
// time; pass nil to skip the signal (useful in tests without a running
// loop).
func NewScheduler(loop *Loop, wake func(time.Time)) *Scheduler {
	if wake == nil {
		wake = func(time.Time) {}
	}
	return &Scheduler{Loop: loop, wake: wake}
}

// RequestEval upserts the singleton evaluate_policy job for the given flow
// to run at runAt. If the job is currently leased by the scheduler (i.e.
// the handler is actively running) only RunAt and PayloadJSON are updated;
// the lease is left intact and CompleteJob will observe the advanced RunAt
// and reschedule the job to Pending rather than marking it Completed. Any
// non-leased existing job is fully reset to Pending.
//
// flowVersion is stamped on the payload as the staleness checkpoint. See the
// EvalRequester interface for semantics.
func (s *Scheduler) RequestEval(ctx context.Context, tx repo.TxRepository, flow domain.Flow, now, runAt time.Time, reason, idempotencyKey string, flowVersion int64) error {
	payload, err := json.Marshal(jobs.EvaluatePolicyPayload{Reason: reason, FlowVersion: flowVersion})
	if err != nil {
		return fmt.Errorf("marshal eval payload: %w", err)
	}

	jobID := "job:eval:scheduled:" + flow.ItemID
	job, found, err := tx.GetJob(ctx, jobID)
	if err != nil {
		return err
	}

	if found {
		activelyLeased := job.Status == domain.JobStatusLeased && now.Before(job.LeaseUntil)

		job.FlowID = flow.FlowID
		job.ItemID = flow.ItemID
		job.Kind = domain.JobKindEvaluatePolicy
		job.RunAt = runAt
		job.MaxAttempts = 5
		job.IdempotencyKey = idempotencyKey + ":" + flow.ItemID
		job.PayloadJSON = payload
		job.LastError = ""
		job.UpdatedAt = now
		if job.CreatedAt.IsZero() {
			job.CreatedAt = now
		}

		if !activelyLeased {
			// Not executing right now — safe to fully reset.
			job.Status = domain.JobStatusPending
			job.LeaseOwner = ""
			job.LeaseUntil = time.Time{}
		}
		// If actively leased: leave Status/LeaseOwner/LeaseUntil as-is.
		// CompleteJob will observe the updated RunAt and reschedule the job
		// to Pending instead of marking it Completed.

		if err := tx.UpdateJob(ctx, job); err != nil {
			return err
		}
	} else {
		if err := tx.EnqueueJob(ctx, domain.JobRecord{
			JobID:          jobID,
			FlowID:         flow.FlowID,
			ItemID:         flow.ItemID,
			Kind:           domain.JobKindEvaluatePolicy,
			Status:         domain.JobStatusPending,
			RunAt:          runAt,
			MaxAttempts:    5,
			IdempotencyKey: idempotencyKey + ":" + flow.ItemID,
			PayloadJSON:    payload,
			CreatedAt:      now,
			UpdatedAt:      now,
		}); err != nil {
			return err
		}
	}

	s.wake(runAt)
	return nil
}
