package handlers

import (
	"context"
	"log/slog"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/jobs"
)

type NoopHandler struct {
	kind   domain.JobKind
	logger *slog.Logger
}

func NewNoopHandler(kind domain.JobKind, logger *slog.Logger) *NoopHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &NoopHandler{kind: kind, logger: logger}
}

func (h *NoopHandler) Kind() domain.JobKind {
	return h.kind
}

func (h *NoopHandler) Handle(ctx context.Context, job domain.JobRecord) error {
	h.logger.InfoContext(ctx, "noop job handler executed", "job_id", job.JobID, "kind", job.Kind)
	return nil
}

func DefaultHandlers(logger *slog.Logger) []jobs.JobHandler {
	return []jobs.JobHandler{
		NewNoopHandler(domain.JobKindEvaluatePolicy, logger),
		NewNoopHandler(domain.JobKindSendHITLPrompt, logger),
		NewNoopHandler(domain.JobKindHITLTimeout, logger),
		NewNoopHandler(domain.JobKindExecuteDelete, logger),
		NewNoopHandler(domain.JobKindVerifyDelete, logger),
		NewNoopHandler(domain.JobKindReconcileItem, logger),
	}
}
