package repo

import (
	"context"
	"time"

	"jellyreaper/internal/domain"
)

type TxRepository interface {
	GetFlow(ctx context.Context, itemID string) (domain.Flow, bool, error)
	UpsertFlowCAS(ctx context.Context, flow domain.Flow, expectedVersion int64) error
	DeleteFlow(ctx context.Context, itemID string) error

	GetMedia(ctx context.Context, itemID string) (domain.MediaItem, bool, error)
	UpsertMedia(ctx context.Context, media domain.MediaItem) error
	DeleteMedia(ctx context.Context, itemID string) error
	ListMediaBySubject(ctx context.Context, subjectType string, subjectID string) ([]domain.MediaItem, error)

	AppendEvent(ctx context.Context, event domain.Event) error

	EnqueueJob(ctx context.Context, job domain.JobRecord) error
	GetJob(ctx context.Context, jobID string) (domain.JobRecord, bool, error)
	UpdateJob(ctx context.Context, job domain.JobRecord) error

	IsProcessed(ctx context.Context, key string) (bool, error)
	MarkProcessed(ctx context.Context, key string, at time.Time) error

	GetMeta(ctx context.Context, key string) (string, bool, error)
	SetMeta(ctx context.Context, key string, value string) error
}

type Repository interface {
	WithTx(ctx context.Context, fn func(tx TxRepository) error) error

	LeaseDueJobs(ctx context.Context, now time.Time, limit int, leaseOwner string, leaseTTL time.Duration) ([]domain.JobRecord, error)
	GetNextDueAt(ctx context.Context) (time.Time, bool, error)
	GetNextQueuedJob(ctx context.Context) (domain.JobRecord, bool, error)

	CompleteJob(ctx context.Context, jobID string, completedAt time.Time) error
	FailJob(ctx context.Context, jobID string, errMsg string, retryAt time.Time, markTerminal bool) error
}
