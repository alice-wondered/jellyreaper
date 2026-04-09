package repo

import (
	"context"
	"time"

	"jellyreaper/internal/domain"
)

// This is a really interesting pattern but I feel like we might be misusing it a bit causing our domain model to leak
// and become rather hard to understand
// now, instead of having a repository interface and building first class "functions as features" into the service implementation
// we instead have arbitrary tx repository calls all over our code to compose behaviors.
// could we not instead expose the transactional behaviors directly on the repository and not have a bunch of freeform
// functions that show up in random places like our scheduler?
type TxRepository interface {
	GetFlow(ctx context.Context, itemID string) (domain.Flow, bool, error)
	ListFlows(ctx context.Context) ([]domain.Flow, error)
	SearchFlows(ctx context.Context, query string, subjectType string, limit int) ([]domain.Flow, error)
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
	// DeleteJobsForItem removes every job record (any status, any kind) whose
	// ItemID matches itemID. Used by ExecuteDelete to purge stale eval/prompt/
	// timeout jobs after a successful destruction, and by webhook playback
	// recovery to wipe stale HITL chain jobs when a play resurrects an item.
	// Returns the number of records deleted.
	DeleteJobsForItem(ctx context.Context, itemID string) (int, error)

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
