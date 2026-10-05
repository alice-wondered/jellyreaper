package handlers

import (
	"context"
	"log/slog"

	"jellyreaper/internal/domain"
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
