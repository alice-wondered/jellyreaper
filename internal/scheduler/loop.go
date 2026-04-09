package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/repo"
)

type DispatchFunc func(context.Context, domain.JobRecord) error

type Config struct {
	LeaseOwner string
	LeaseLimit int
	LeaseTTL   time.Duration
	IdlePoll   time.Duration
	Signal     <-chan time.Time
}

type Loop struct {
	repository repo.Repository
	dispatch   DispatchFunc
	logger     *slog.Logger

	leaseOwner string
	leaseLimit int
	leaseTTL   time.Duration
	idlePoll   time.Duration
	signal     <-chan time.Time
	now        func() time.Time

	wakeHints *WakeHeap
}

func NewLoop(repository repo.Repository, dispatch DispatchFunc, logger *slog.Logger, cfg Config) *Loop {
	if logger == nil {
		logger = slog.Default()
	}
	if dispatch == nil {
		dispatch = func(context.Context, domain.JobRecord) error { return nil }
	}
	if cfg.LeaseOwner == "" {
		cfg.LeaseOwner = "scheduler"
	}
	if cfg.LeaseLimit <= 0 {
		cfg.LeaseLimit = 32
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 30 * time.Second
	}
	if cfg.IdlePoll <= 0 {
		cfg.IdlePoll = 5 * time.Second
	}

	return &Loop{
		repository: repository,
		dispatch:   dispatch,
		logger:     logger,
		leaseOwner: cfg.LeaseOwner,
		leaseLimit: cfg.LeaseLimit,
		leaseTTL:   cfg.LeaseTTL,
		idlePoll:   cfg.IdlePoll,
		signal:     cfg.Signal,
		now:        time.Now,
		wakeHints:  NewWakeHeap(),
	}
}

// I would have expected to see a select block here to determine our wakeups, especially because that allows us to
// open a channel and wake early if we get a signal
func (l *Loop) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		now := l.now()
		if err := l.leaseAndDispatch(ctx, now); err != nil {
			l.logger.Error("lease/dispatch cycle failed", "lex", "SCHEDULER", "error", err)
		}

		nextWake := l.computeNextWake(ctx, now)
		if err := l.sleepUntil(ctx, nextWake); err != nil {
			return nil
		}
	}
}

func (l *Loop) leaseAndDispatch(ctx context.Context, now time.Time) error {
	// let's verify the leasing system behavior is working the way that we intend for it to
	// essentially no writes can happen to a job that's leased
	// which prevents a write to a pending deletion, evaluation, or pending_review
	// but when not leased (because not due) we CAN write to those jobs, allowing for
	// a playback event, for instance, to be processed and update any scheduled job that isn't due yet
	// this means that every job handler should have a "verification" stage where it checks to see if the job
	// that's scheduled should still be valid or if we should state change based on new information
	// for instance, HITL review should not schedule a deletion if a play came in
	// but frankly, I find this to be a little clunky, because I would really expect for something affecting the current job to
	// immediately update the job state
	// however I want job state to be owned by the scheduler/job handler not webhooks or external systems or backfill or whatever else
	// in which case, I think our scheduler should probably expose some sort of API that allows us to encapsulate job update and schedule update
	// semantics for those external systems rather than doing it directly via the repository
	// then if we need to deal with leasing or whatever else at least it's all in one place
	// and we can handle complexity like "waiting for the lease to expire and resolve next action"
	jobs, err := l.repository.LeaseDueJobs(ctx, now, l.leaseLimit, l.leaseOwner, l.leaseTTL)
	if err != nil {
		return fmt.Errorf("lease due jobs: %w", err)
	}

	for _, job := range jobs {
		if err := l.dispatch(ctx, job); err != nil {
			l.logger.Error("dispatch failed", "lex", "SCHEDULER-DISPATCH", "job_id", job.JobID, "kind", job.Kind, "error", err)
		}
	}

	return nil
}

func (l *Loop) computeNextWake(ctx context.Context, now time.Time) time.Time {
	nextWake := now.Add(l.idlePoll)

	dueAt, ok, err := l.repository.GetNextDueAt(ctx)
	if err != nil {
		l.logger.Error("get next due job failed", "lex", "SCHEDULER-QUEUE", "error", err)
	} else if ok && dueAt.Before(nextWake) {
		nextWake = dueAt
	}

	if hinted, ok := l.wakeHints.Peek(); ok && hinted.Before(nextWake) {
		nextWake = hinted
	}

	if nextWake.Before(now) {
		return now
	}
	return nextWake
}

// Oh ok we have our select thing here, it's just a bit buried in indirection
func (l *Loop) sleepUntil(ctx context.Context, wakeAt time.Time) error {
	wait := wakeAt.Sub(l.now())
	if wait < 0 {
		wait = 0
	}

	timer := time.NewTimer(wait)
	defer stopTimer(timer)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case hintedAt, ok := <-l.signal:
		if !ok {
			l.signal = nil
			return nil
		}
		l.wakeHints.Push(hintedAt)
		l.wakeHints.DiscardThrough(l.now())
		return nil
	case <-timer.C:
		l.wakeHints.DiscardThrough(l.now())
		return nil
	}
}

func stopTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
