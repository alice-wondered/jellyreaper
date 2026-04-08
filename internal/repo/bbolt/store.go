package bbolt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/bbolt"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/repo"
)

var _ repo.Repository = (*Store)(nil)
var _ repo.TxRepository = (*txRepo)(nil)

var (
	bucketMedia    = []byte("media")
	bucketFlows    = []byte("flows")
	bucketEvents   = []byte("events")
	bucketJobs     = []byte("jobs")
	bucketDueIndex = []byte("due_index")
	bucketDedupe   = []byte("dedupe")
	bucketMeta     = []byte("meta")
)

type Store struct {
	db *bbolt.DB
}

type txRepo struct {
	tx *bbolt.Tx
}

func Open(path string, mode os.FileMode, options *bbolt.Options) (*Store, error) {
	db, err := bbolt.Open(path, mode, options)
	if err != nil {
		return nil, fmt.Errorf("open bbolt: %w", err)
	}

	store := &Store{db: db}
	if err := store.db.Update(func(tx *bbolt.Tx) error {
		for _, name := range [][]byte{bucketMedia, bucketFlows, bucketEvents, bucketJobs, bucketDueIndex, bucketDedupe, bucketMeta} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("create bucket %s: %w", string(name), err)
			}
		}

		jobs, err := requireBucket(tx, bucketJobs)
		if err != nil {
			return err
		}
		dueIndex, err := requireBucket(tx, bucketDueIndex)
		if err != nil {
			return err
		}
		if err := syncJobsToDueIndex(jobs, dueIndex, time.Now().UTC(), true); err != nil {
			return fmt.Errorf("reconcile persisted job queue: %w", err)
		}

		return nil
	}); err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) WithTx(ctx context.Context, fn func(tx repo.TxRepository) error) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		if err := checkContext(ctx); err != nil {
			return err
		}
		return fn(&txRepo{tx: tx})
	})
}

func (s *Store) LeaseDueJobs(ctx context.Context, now time.Time, limit int, leaseOwner string, leaseTTL time.Duration) ([]domain.JobRecord, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	if leaseOwner == "" {
		return nil, fmt.Errorf("lease due jobs: lease owner required: %w", ErrInvalidInput)
	}
	if leaseTTL <= 0 {
		return nil, fmt.Errorf("lease due jobs: lease ttl must be > 0: %w", ErrInvalidInput)
	}

	leased := make([]domain.JobRecord, 0, limit)
	err := s.db.Update(func(tx *bbolt.Tx) error {
		jobs, err := requireBucket(tx, bucketJobs)
		if err != nil {
			return err
		}
		dueIndex, err := requireBucket(tx, bucketDueIndex)
		if err != nil {
			return err
		}
		if err := syncJobsToDueIndex(jobs, dueIndex, now, false); err != nil {
			return fmt.Errorf("sync due index: %w", err)
		}

		maxDue := dueIndexKeyBytes(now, "\xff")
		for len(leased) < limit {
			cursor := dueIndex.Cursor()
			k, v := cursor.First()
			if k == nil {
				break
			}
			if err := checkContext(ctx); err != nil {
				return err
			}
			if bytes.Compare(k, maxDue) > 0 {
				break
			}

			jobID := string(v)
			var job domain.JobRecord
			found, err := bucketGetJSON(jobs, jobID, &job)
			if err != nil {
				return fmt.Errorf("decode job %s: %w", jobID, err)
			}
			if !found {
				if err := dueIndex.Delete(k); err != nil {
					return fmt.Errorf("delete stale due index key %q: %w", string(k), err)
				}
				continue
			}

			if job.Status != domain.JobStatusPending || job.RunAt.After(now) {
				if err := dueIndex.Delete(k); err != nil {
					return fmt.Errorf("delete stale due index key %q: %w", string(k), err)
				}
				continue
			}

			if err := dueIndex.Delete(k); err != nil {
				return fmt.Errorf("delete due index key for leased job %s: %w", job.JobID, err)
			}

			job.Status = domain.JobStatusLeased
			job.LeaseOwner = leaseOwner
			job.LeaseUntil = now.Add(leaseTTL)
			job.UpdatedAt = now

			if err := bucketPutJSON(jobs, job.JobID, job); err != nil {
				return fmt.Errorf("persist leased job %s: %w", job.JobID, err)
			}

			leased = append(leased, job)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return leased, nil
}

func (s *Store) GetNextDueAt(ctx context.Context) (time.Time, bool, error) {
	if err := checkContext(ctx); err != nil {
		return time.Time{}, false, err
	}

	var out time.Time
	var ok bool
	err := s.db.View(func(tx *bbolt.Tx) error {
		dueIndex, err := requireBucket(tx, bucketDueIndex)
		if err != nil {
			return err
		}
		k, _ := dueIndex.Cursor().First()
		if k == nil {
			return nil
		}
		runAt, err := parseDueIndexKey(string(k))
		if err != nil {
			return err
		}
		out = runAt
		ok = true
		return nil
	})
	if err != nil {
		return time.Time{}, false, err
	}
	return out, ok, nil
}

func (s *Store) GetNextQueuedJob(ctx context.Context) (domain.JobRecord, bool, error) {
	if err := checkContext(ctx); err != nil {
		return domain.JobRecord{}, false, err
	}

	var out domain.JobRecord
	var ok bool
	err := s.db.View(func(tx *bbolt.Tx) error {
		dueIndex, err := requireBucket(tx, bucketDueIndex)
		if err != nil {
			return err
		}
		jobs, err := requireBucket(tx, bucketJobs)
		if err != nil {
			return err
		}

		_, v := dueIndex.Cursor().First()
		if v == nil {
			return nil
		}

		jobID := string(v)
		if jobID == "" {
			return nil
		}

		var job domain.JobRecord
		found, err := bucketGetJSON(jobs, jobID, &job)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		out = job
		ok = true
		return nil
	})
	if err != nil {
		return domain.JobRecord{}, false, err
	}
	return out, ok, nil
}

func (s *Store) CompleteJob(ctx context.Context, jobID string, completedAt time.Time) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if jobID == "" {
		return fmt.Errorf("complete job: job id required: %w", ErrInvalidInput)
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		repoTx := &txRepo{tx: tx}
		job, found, err := repoTx.GetJob(ctx, jobID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("complete job %s: %w", jobID, ErrNotFound)
		}

		job.Status = domain.JobStatusCompleted
		job.LeaseOwner = ""
		job.LeaseUntil = time.Time{}
		job.LastError = ""
		job.UpdatedAt = completedAt

		return repoTx.UpdateJob(ctx, job)
	})
}

func (s *Store) FailJob(ctx context.Context, jobID string, errMsg string, retryAt time.Time, markTerminal bool) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if jobID == "" {
		return fmt.Errorf("fail job: job id required: %w", ErrInvalidInput)
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		repoTx := &txRepo{tx: tx}
		job, found, err := repoTx.GetJob(ctx, jobID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("fail job %s: %w", jobID, ErrNotFound)
		}

		job.LastError = errMsg
		job.LeaseOwner = ""
		job.LeaseUntil = time.Time{}
		job.UpdatedAt = time.Now().UTC()

		if markTerminal {
			job.Status = domain.JobStatusFailed
		} else {
			job.Status = domain.JobStatusPending
			job.RunAt = retryAt
		}

		return repoTx.UpdateJob(ctx, job)
	})
}

func (t *txRepo) GetFlow(ctx context.Context, itemID string) (domain.Flow, bool, error) {
	if err := checkContext(ctx); err != nil {
		return domain.Flow{}, false, err
	}
	if itemID == "" {
		return domain.Flow{}, false, fmt.Errorf("get flow: item id required: %w", ErrInvalidInput)
	}
	b, err := requireBucket(t.tx, bucketFlows)
	if err != nil {
		return domain.Flow{}, false, err
	}
	var flow domain.Flow
	found, err := bucketGetJSON(b, itemID, &flow)
	if err != nil {
		return domain.Flow{}, false, fmt.Errorf("decode flow %s: %w", itemID, err)
	}
	if !found {
		return domain.Flow{}, false, nil
	}
	return flow, true, nil
}

func (t *txRepo) UpsertFlowCAS(ctx context.Context, flow domain.Flow, expectedVersion int64) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if flow.ItemID == "" {
		return fmt.Errorf("upsert flow: item id required: %w", ErrInvalidInput)
	}
	b, err := requireBucket(t.tx, bucketFlows)
	if err != nil {
		return err
	}

	var current domain.Flow
	found, err := bucketGetJSON(b, flow.ItemID, &current)
	if err != nil {
		return fmt.Errorf("decode existing flow %s: %w", flow.ItemID, err)
	}
	if !found {
		if expectedVersion != 0 {
			return fmt.Errorf("upsert flow %s expected version %d but missing: %w", flow.ItemID, expectedVersion, ErrConflict)
		}
	} else {
		if current.Version != expectedVersion {
			return fmt.Errorf("upsert flow %s expected version %d got %d: %w", flow.ItemID, expectedVersion, current.Version, ErrConflict)
		}
	}

	if err := bucketPutJSON(b, flow.ItemID, flow); err != nil {
		return fmt.Errorf("persist flow %s: %w", flow.ItemID, err)
	}
	return nil
}

func (t *txRepo) DeleteFlow(ctx context.Context, itemID string) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if itemID == "" {
		return fmt.Errorf("delete flow: item id required: %w", ErrInvalidInput)
	}
	b, err := requireBucket(t.tx, bucketFlows)
	if err != nil {
		return err
	}
	return b.Delete(keyBytes(itemID))
}

func (t *txRepo) GetMedia(ctx context.Context, itemID string) (domain.MediaItem, bool, error) {
	if err := checkContext(ctx); err != nil {
		return domain.MediaItem{}, false, err
	}
	if itemID == "" {
		return domain.MediaItem{}, false, fmt.Errorf("get media: item id required: %w", ErrInvalidInput)
	}
	b, err := requireBucket(t.tx, bucketMedia)
	if err != nil {
		return domain.MediaItem{}, false, err
	}
	var media domain.MediaItem
	found, err := bucketGetJSON(b, itemID, &media)
	if err != nil {
		return domain.MediaItem{}, false, fmt.Errorf("decode media %s: %w", itemID, err)
	}
	if !found {
		return domain.MediaItem{}, false, nil
	}
	return media, true, nil
}

func (t *txRepo) UpsertMedia(ctx context.Context, media domain.MediaItem) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if media.ItemID == "" {
		return fmt.Errorf("upsert media: item id required: %w", ErrInvalidInput)
	}
	b, err := requireBucket(t.tx, bucketMedia)
	if err != nil {
		return err
	}
	if err := bucketPutJSON(b, media.ItemID, media); err != nil {
		return fmt.Errorf("persist media %s: %w", media.ItemID, err)
	}
	return nil
}

func (t *txRepo) DeleteMedia(ctx context.Context, itemID string) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if itemID == "" {
		return fmt.Errorf("delete media: item id required: %w", ErrInvalidInput)
	}
	b, err := requireBucket(t.tx, bucketMedia)
	if err != nil {
		return err
	}
	return b.Delete(keyBytes(itemID))
}

func (t *txRepo) ListMediaBySubject(ctx context.Context, subjectType string, subjectID string) ([]domain.MediaItem, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if subjectType == "" || subjectID == "" {
		return nil, fmt.Errorf("list media by subject: subject type and id required: %w", ErrInvalidInput)
	}
	b, err := requireBucket(t.tx, bucketMedia)
	if err != nil {
		return nil, err
	}

	out := make([]domain.MediaItem, 0)
	err = b.ForEach(func(_, v []byte) error {
		if err := checkContext(ctx); err != nil {
			return err
		}
		var media domain.MediaItem
		if err := json.Unmarshal(v, &media); err != nil {
			return err
		}
		switch subjectType {
		case "season":
			if media.SeasonID == subjectID {
				out = append(out, media)
			}
		case "series":
			if media.SeriesID == subjectID {
				out = append(out, media)
			}
		case "movie", "item":
			if media.ItemID == subjectID {
				out = append(out, media)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list media by subject: %w", err)
	}
	return out, nil
}

func (t *txRepo) AppendEvent(ctx context.Context, event domain.Event) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if event.EventID == "" {
		return fmt.Errorf("append event: event id required: %w", ErrInvalidInput)
	}
	b, err := requireBucket(t.tx, bucketEvents)
	if err != nil {
		return err
	}
	if b.Get(keyBytes(event.EventID)) != nil {
		return fmt.Errorf("append event %s: %w", event.EventID, ErrAlreadyExists)
	}
	if err := bucketPutJSON(b, event.EventID, event); err != nil {
		return fmt.Errorf("persist event %s: %w", event.EventID, err)
	}
	return nil
}

func (t *txRepo) EnqueueJob(ctx context.Context, job domain.JobRecord) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if job.JobID == "" {
		return fmt.Errorf("enqueue job: job id required: %w", ErrInvalidInput)
	}
	if job.Status == "" {
		job.Status = domain.JobStatusPending
	}

	jobsBucket, err := requireBucket(t.tx, bucketJobs)
	if err != nil {
		return err
	}
	if jobsBucket.Get(keyBytes(job.JobID)) != nil {
		return fmt.Errorf("enqueue job %s: %w", job.JobID, ErrAlreadyExists)
	}

	if err := bucketPutJSON(jobsBucket, job.JobID, job); err != nil {
		return fmt.Errorf("persist job %s: %w", job.JobID, err)
	}

	if job.Status == domain.JobStatusPending {
		if err := putDueIndex(t.tx, job); err != nil {
			return err
		}
	}

	return nil
}

func (t *txRepo) GetJob(ctx context.Context, jobID string) (domain.JobRecord, bool, error) {
	if err := checkContext(ctx); err != nil {
		return domain.JobRecord{}, false, err
	}
	if jobID == "" {
		return domain.JobRecord{}, false, fmt.Errorf("get job: job id required: %w", ErrInvalidInput)
	}
	b, err := requireBucket(t.tx, bucketJobs)
	if err != nil {
		return domain.JobRecord{}, false, err
	}
	var job domain.JobRecord
	found, err := bucketGetJSON(b, jobID, &job)
	if err != nil {
		return domain.JobRecord{}, false, fmt.Errorf("decode job %s: %w", jobID, err)
	}
	if !found {
		return domain.JobRecord{}, false, nil
	}
	return job, true, nil
}

func (t *txRepo) UpdateJob(ctx context.Context, job domain.JobRecord) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if job.JobID == "" {
		return fmt.Errorf("update job: job id required: %w", ErrInvalidInput)
	}

	jobsBucket, err := requireBucket(t.tx, bucketJobs)
	if err != nil {
		return err
	}
	dueIndex, err := requireBucket(t.tx, bucketDueIndex)
	if err != nil {
		return err
	}

	var existing domain.JobRecord
	found, err := bucketGetJSON(jobsBucket, job.JobID, &existing)
	if err != nil {
		return fmt.Errorf("decode existing job %s: %w", job.JobID, err)
	}
	if !found {
		return fmt.Errorf("update job %s: %w", job.JobID, ErrNotFound)
	}

	if existing.Status == domain.JobStatusPending {
		if err := dueIndex.Delete(dueIndexKeyBytes(existing.RunAt, existing.JobID)); err != nil {
			return fmt.Errorf("remove prior due index for %s: %w", existing.JobID, err)
		}
	}

	if err := bucketPutJSON(jobsBucket, job.JobID, job); err != nil {
		return fmt.Errorf("persist updated job %s: %w", job.JobID, err)
	}

	if job.Status == domain.JobStatusPending {
		if err := putDueIndexBucket(dueIndex, job); err != nil {
			return fmt.Errorf("insert due index for %s: %w", job.JobID, err)
		}
	}

	return nil
}

func (t *txRepo) IsProcessed(ctx context.Context, key string) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, err
	}
	if key == "" {
		return false, fmt.Errorf("is processed: key required: %w", ErrInvalidInput)
	}
	b, err := requireBucket(t.tx, bucketDedupe)
	if err != nil {
		return false, err
	}
	return b.Get(keyBytes(key)) != nil, nil
}

func (t *txRepo) MarkProcessed(ctx context.Context, key string, at time.Time) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("mark processed: key required: %w", ErrInvalidInput)
	}
	b, err := requireBucket(t.tx, bucketDedupe)
	if err != nil {
		return err
	}
	if err := b.Put(keyBytes(key), keyBytes(at.UTC().Format(time.RFC3339Nano))); err != nil {
		return fmt.Errorf("mark processed %s: %w", key, err)
	}
	return nil
}

func (t *txRepo) GetMeta(ctx context.Context, key string) (string, bool, error) {
	if err := checkContext(ctx); err != nil {
		return "", false, err
	}
	if key == "" {
		return "", false, fmt.Errorf("get meta: key required: %w", ErrInvalidInput)
	}
	b, err := requireBucket(t.tx, bucketMeta)
	if err != nil {
		return "", false, err
	}
	raw := b.Get(keyBytes(key))
	if raw == nil {
		return "", false, nil
	}
	return string(raw), true, nil
}

func (t *txRepo) SetMeta(ctx context.Context, key string, value string) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("set meta: key required: %w", ErrInvalidInput)
	}
	b, err := requireBucket(t.tx, bucketMeta)
	if err != nil {
		return err
	}
	if err := b.Put(keyBytes(key), keyBytes(value)); err != nil {
		return fmt.Errorf("persist meta %s: %w", key, err)
	}
	return nil
}

func putDueIndex(tx *bbolt.Tx, job domain.JobRecord) error {
	dueIndex, err := requireBucket(tx, bucketDueIndex)
	if err != nil {
		return err
	}
	if err := putDueIndexBucket(dueIndex, job); err != nil {
		return fmt.Errorf("insert due index for %s: %w", job.JobID, err)
	}
	return nil
}

func putDueIndexBucket(dueIndex *bbolt.Bucket, job domain.JobRecord) error {
	return dueIndex.Put(dueIndexKeyBytes(job.RunAt, job.JobID), keyBytes(job.JobID))
}

func syncJobsToDueIndex(jobs *bbolt.Bucket, dueIndex *bbolt.Bucket, now time.Time, clearExisting bool) error {
	if clearExisting {
		if err := clearBucket(dueIndex); err != nil {
			return err
		}
	}

	return jobs.ForEach(func(_, value []byte) error {
		var job domain.JobRecord
		if err := json.Unmarshal(value, &job); err != nil {
			return err
		}

		updated := false
		if job.Status == domain.JobStatusLeased && !job.LeaseUntil.After(now) {
			job.Status = domain.JobStatusPending
			job.LeaseOwner = ""
			job.LeaseUntil = time.Time{}
			job.UpdatedAt = now
			updated = true
		}
		if job.Status == domain.JobStatusPending && job.RunAt.IsZero() {
			job.RunAt = now
			updated = true
		}

		if updated {
			if err := bucketPutJSON(jobs, job.JobID, job); err != nil {
				return fmt.Errorf("persist reconciled job %s: %w", job.JobID, err)
			}
		}

		if job.Status == domain.JobStatusPending {
			if err := putDueIndexBucket(dueIndex, job); err != nil {
				return fmt.Errorf("insert due index for %s: %w", job.JobID, err)
			}
		}
		return nil
	})
}

func clearBucket(bucket *bbolt.Bucket) error {
	cursor := bucket.Cursor()
	for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
		if err := bucket.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

func dueIndexKey(runAt time.Time, jobID string) string {
	return fmt.Sprintf("%020d|%s", runAt.UnixNano(), jobID)
}

func dueIndexKeyBytes(runAt time.Time, jobID string) []byte {
	return keyBytes(dueIndexKey(runAt, jobID))
}

func keyBytes(key string) []byte {
	return []byte(key)
}

func bucketGetJSON[T any](bucket *bbolt.Bucket, key string, out *T) (bool, error) {
	raw := bucket.Get(keyBytes(key))
	if raw == nil {
		return false, nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return false, err
	}
	return true, nil
}

func bucketPutJSON[T any](bucket *bbolt.Bucket, key string, value T) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return bucket.Put(keyBytes(key), encoded)
}

func parseDueIndexKey(key string) (time.Time, error) {
	parts := strings.SplitN(key, "|", 2)
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("parse due index key %q: malformed", key)
	}
	ns, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse due index key %q: %w", key, err)
	}
	return time.Unix(0, ns).UTC(), nil
}

func requireBucket(tx *bbolt.Tx, name []byte) (*bbolt.Bucket, error) {
	b := tx.Bucket(name)
	if b == nil {
		return nil, fmt.Errorf("missing bucket %s", string(name))
	}
	return b, nil
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
