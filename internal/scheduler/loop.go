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

func (l *Loop) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		now := l.now()
		if err := l.leaseAndDispatch(ctx, now); err != nil {
			l.logger.Error("lease/dispatch cycle failed", "error", err)
		}

		nextWake := l.computeNextWake(ctx, now)
		if err := l.sleepUntil(ctx, nextWake); err != nil {
			return nil
		}
	}
}

func (l *Loop) leaseAndDispatch(ctx context.Context, now time.Time) error {
	jobs, err := l.repository.LeaseDueJobs(ctx, now, l.leaseLimit, l.leaseOwner, l.leaseTTL)
	if err != nil {
		return fmt.Errorf("lease due jobs: %w", err)
	}

	for _, job := range jobs {
		if err := l.dispatch(ctx, job); err != nil {
			l.logger.Error("dispatch failed", "job_id", job.JobID, "kind", job.Kind, "error", err)
		}
	}

	return nil
}

func (l *Loop) computeNextWake(ctx context.Context, now time.Time) time.Time {
	nextWake := now.Add(l.idlePoll)

	dueAt, ok, err := l.repository.GetNextDueAt(ctx)
	if err != nil {
		l.logger.Error("get next due job failed", "error", err)
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
