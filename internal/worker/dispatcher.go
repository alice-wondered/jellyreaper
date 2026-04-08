package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/jobs"
	"jellyreaper/internal/repo"
)

const (
	defaultBackoffBase      = 2 * time.Second
	defaultBackoffMax       = 5 * time.Minute
	defaultBackoffJitterPct = 0.20
)

type Dispatcher struct {
	repository repo.Repository
	registry   *jobs.Registry
	logger     *slog.Logger
	now        func() time.Time
}

func NewDispatcher(repository repo.Repository, registry *jobs.Registry, logger *slog.Logger) *Dispatcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Dispatcher{
		repository: repository,
		registry:   registry,
		logger:     logger,
		now:        time.Now,
	}
}

func (d *Dispatcher) Dispatch(ctx context.Context, job domain.JobRecord) error {
	handler, ok := d.registry.Get(job.Kind)
	if !ok {
		err := fmt.Errorf("unknown job kind %q", job.Kind)
		d.logger.Warn("job failed: unknown kind", "job_id", job.JobID, "kind", job.Kind)
		return d.repository.FailJob(ctx, job.JobID, err.Error(), time.Time{}, true)
	}

	if err := handler.Handle(ctx, job); err != nil {
		attempt := job.Attempts + 1
		terminal := isTerminalAttempt(attempt, job.MaxAttempts)
		retryAt := time.Time{}
		if !terminal {
			retryAt = d.now().Add(RetryBackoff(attempt))
		}

		d.logger.Warn(
			"job handler failed",
			"job_id", job.JobID,
			"kind", job.Kind,
			"attempt", attempt,
			"terminal", terminal,
			"error", err,
		)

		if failErr := d.repository.FailJob(ctx, job.JobID, err.Error(), retryAt, terminal); failErr != nil {
			return fmt.Errorf("mark job failed %s: %w", job.JobID, failErr)
		}
		return nil
	}

	if err := d.repository.CompleteJob(ctx, job.JobID, d.now()); err != nil {
		return fmt.Errorf("mark job complete %s: %w", job.JobID, err)
	}

	d.logger.Info("job completed", "job_id", job.JobID, "kind", job.Kind)
	return nil
}

func RetryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	multiplier := math.Pow(2, float64(attempt-1))
	backoff := time.Duration(float64(defaultBackoffBase) * multiplier)
	if backoff > defaultBackoffMax {
		backoff = defaultBackoffMax
	}

	jitterFactor := 1 + ((rand.Float64()*2 - 1) * defaultBackoffJitterPct)
	withJitter := time.Duration(float64(backoff) * jitterFactor)
	if withJitter < time.Second {
		return time.Second
	}
	return withJitter
}

func isTerminalAttempt(attempt int, maxAttempts int) bool {
	if maxAttempts <= 0 {
		return true
	}
	return attempt >= maxAttempts
}
