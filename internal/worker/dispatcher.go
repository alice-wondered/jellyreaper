package worker

import (
	"context"
	"errors"
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

// DeleteFailedNotifier is invoked when an ExecuteDelete job exhausts its
// retries and the dispatcher transitions the flow to DeleteFailed. The hook
// is the seam for surfacing the failure to humans (Discord notification,
// pager, etc.). It runs on a best-effort basis; errors inside the notifier
// must not be returned to the dispatcher.
type DeleteFailedNotifier func(ctx context.Context, flow domain.Flow, err error)

type Dispatcher struct {
	repository         repo.Repository
	registry           *jobs.Registry
	logger             *slog.Logger
	now                func() time.Time
	notifyDeleteFailed DeleteFailedNotifier
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

// SetDeleteFailedNotifier installs a hook that fires after a terminal
// ExecuteDelete failure has been recorded as DeleteFailed on the flow.
func (d *Dispatcher) SetDeleteFailedNotifier(notifier DeleteFailedNotifier) {
	d.notifyDeleteFailed = notifier
}

func (d *Dispatcher) Dispatch(ctx context.Context, job domain.JobRecord) error {
	handler, ok := d.registry.Get(job.Kind)
	if !ok {
		err := fmt.Errorf("unknown job kind %q", job.Kind)
		d.logger.Warn("job failed: unknown kind", "lex", "JOB-ERROR", "job_id", job.JobID, "kind", job.Kind)
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
			"lex", jobLogLexicon(job.Kind),
			"kind", job.Kind,
			"job_id", job.JobID,
			"flow_id", job.FlowID,
			"item_id", job.ItemID,
			"attempt", attempt,
			"terminal", terminal,
			"error", err,
		)

		if failErr := d.repository.FailJob(ctx, job.JobID, err.Error(), retryAt, terminal); failErr != nil {
			return fmt.Errorf("mark job failed %s: %w", job.JobID, failErr)
		}
		if terminal {
			if job.Kind == domain.JobKindExecuteDelete {
				if markErr := d.markDeleteFlowFailed(ctx, job, err); markErr != nil {
					d.logger.Warn("failed to mark delete flow failed", "lex", "DELETION", "job_id", job.JobID, "item_id", job.ItemID, "error", markErr)
				}
			}
			// Let the handler roll the flow back to a recoverable state
			// and re-schedule the singleton eval so the state machine
			// continues from a known point.
			if recoverer, ok := handler.(jobs.TerminalFailureRecoverer); ok {
				if recoverErr := recoverer.OnTerminalFailure(ctx, job); recoverErr != nil {
					d.logger.Warn("terminal failure recovery failed", "lex", jobLogLexicon(job.Kind), "job_id", job.JobID, "item_id", job.ItemID, "error", recoverErr)
				}
			}
		}
		return nil
	}

	if err := d.repository.CompleteJob(ctx, job.JobID, d.now()); err != nil {
		return fmt.Errorf("mark job complete %s: %w", job.JobID, err)
	}

	fields := []any{"lex", jobLogLexicon(job.Kind), "kind", job.Kind, "job_id", job.JobID, "flow_id", job.FlowID, "item_id", job.ItemID}
	fields = append(fields, d.jobOutcomeFields(ctx, job)...)
	d.logger.Info("job completed", fields...)
	return nil
}

func (d *Dispatcher) markDeleteFlowFailed(ctx context.Context, job domain.JobRecord, deleteErr error) error {
	now := d.now().UTC()
	notifyFlow := domain.Flow{}
	notify := false
	if err := d.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, job.ItemID)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if flow.State != domain.FlowStateDeleteQueued {
			return nil
		}
		expected := flow.Version
		flow.State = domain.FlowStateDeleteFailed
		flow.UpdatedAt = now
		flow.Version = expected + 1
		if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
			return err
		}
		notifyFlow = flow
		notify = true
		return nil
	}); err != nil {
		return err
	}
	// Best-effort: notify whoever's listening (e.g. Discord) so the failure
	// is visible. Hook fires outside the tx so its (potentially slow) I/O
	// doesn't hold the bbolt write lock.
	if notify && d.notifyDeleteFailed != nil {
		if deleteErr == nil {
			deleteErr = errors.New("execute delete failed")
		}
		d.notifyDeleteFailed(ctx, notifyFlow, deleteErr)
	}
	return nil
}

func (d *Dispatcher) jobOutcomeFields(ctx context.Context, job domain.JobRecord) []any {
	fields := make([]any, 0, 12)

	switch job.Kind {
	case domain.JobKindEvaluatePolicy:
		if payload, err := jobs.DecodePayload[jobs.EvaluatePolicyPayload](job); err == nil && payload.Reason != "" {
			fields = append(fields, "reason", payload.Reason)
		}
	case domain.JobKindSendHITLPrompt:
		if payload, err := jobs.DecodePayload[jobs.SendHITLPromptPayload](job); err == nil && payload.ChannelID != "" {
			fields = append(fields, "channel_id", payload.ChannelID)
		}
	case domain.JobKindHITLTimeout:
		if payload, err := jobs.DecodePayload[jobs.HITLTimeoutPayload](job); err == nil && payload.DefaultAction != "" {
			fields = append(fields, "default_action", payload.DefaultAction)
		}
	case domain.JobKindExecuteDelete:
		if payload, err := jobs.DecodePayload[jobs.ExecuteDeletePayload](job); err == nil && payload.RequestedBy != "" {
			fields = append(fields, "requested_by", payload.RequestedBy)
		}
	}

	_ = d.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, job.ItemID)
		if err != nil {
			return nil
		}
		if !found {
			fields = append(fields, "flow_state", "missing")
			return nil
		}
		fields = append(fields,
			"flow_state", flow.State,
			"title", flow.DisplayName,
			"hitl_outcome", flow.HITLOutcome,
		)
		if !flow.NextActionAt.IsZero() {
			fields = append(fields, "next_action_at", flow.NextActionAt)
		}
		return nil
	})

	return fields
}

func jobLogLexicon(kind domain.JobKind) string {
	switch kind {
	case domain.JobKindEvaluatePolicy:
		return "POLICY-EVAL"
	case domain.JobKindSendHITLPrompt:
		return "HITL-PROMPT"
	case domain.JobKindHITLTimeout:
		return "HITL-TIMEOUT"
	case domain.JobKindExecuteDelete:
		return "DELETION"
	case domain.JobKindVerifyDelete:
		return "VERIFY-DELETION"
	case domain.JobKindReconcileItem:
		return "RECONCILE"
	default:
		return "JOB"
	}
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
