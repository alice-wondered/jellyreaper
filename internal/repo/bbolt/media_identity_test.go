package bbolt

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	bbolt "go.etcd.io/bbolt"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/repo"
)

func openIdentityStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "identity.db"), 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func inTx(t *testing.T, s *Store, fn func(context.Context, repo.TxRepository) error) error {
	t.Helper()
	return s.WithTx(context.Background(), fn)
}

var episode = domain.MediaItem{ItemID: "ep1", ItemType: "Episode", SeriesID: "series1", SeasonID: "season1", SeasonNumber: intp(1), Name: "Pilot"}

func intp(n int) *int { return &n }

func TestCreateMediaRefusesIncompleteIdentityAndDuplicates(t *testing.T) {
	s := openIdentityStore(t)
	noSeason := episode
	noSeason.SeasonID = ""
	if err := inTx(t, s, func(ctx context.Context, tx repo.TxRepository) error { return tx.CreateMedia(ctx, noSeason) }); !errors.Is(err, domain.ErrIncompleteIdentity) {
		t.Fatalf("episode without season: got %v, want ErrIncompleteIdentity", err)
	}
	if err := inTx(t, s, func(ctx context.Context, tx repo.TxRepository) error { return tx.CreateMedia(ctx, episode) }); err != nil {
		t.Fatalf("complete episode: %v", err)
	}
	if err := inTx(t, s, func(ctx context.Context, tx repo.TxRepository) error { return tx.CreateMedia(ctx, episode) }); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("second create: got %v, want ErrAlreadyExists", err)
	}
}

func TestPatchMediaKeepsIdentity(t *testing.T) {
	s := openIdentityStore(t)
	_ = inTx(t, s, func(ctx context.Context, tx repo.TxRepository) error { return tx.CreateMedia(ctx, episode) })

	for name, mutate := range map[string]func(*domain.MediaItem){
		"blank season": func(m *domain.MediaItem) { m.SeasonID = "" },
		"move season":  func(m *domain.MediaItem) { m.SeasonID = "season2" },
		"blank series": func(m *domain.MediaItem) { m.SeriesID = "" },
		"retype":       func(m *domain.MediaItem) { m.ItemType = "Movie" },
		"renumber":     func(m *domain.MediaItem) { m.SeasonNumber = intp(0) },
		"unplace":      func(m *domain.MediaItem) { m.SeasonNumber = nil },
	} {
		t.Run(name, func(t *testing.T) {
			err := inTx(t, s, func(ctx context.Context, tx repo.TxRepository) error { return tx.PatchMedia(ctx, "ep1", mutate) })
			if !errors.Is(err, domain.ErrIdentityImmutable) {
				t.Fatalf("got %v, want ErrIdentityImmutable", err)
			}
		})
	}

	played := time.Date(2026, 10, 5, 0, 1, 18, 0, time.UTC)
	if err := inTx(t, s, func(ctx context.Context, tx repo.TxRepository) error {
		return tx.PatchMedia(ctx, "ep1", func(m *domain.MediaItem) { m.LastPlayedAt = played; m.Name = "Renamed" })
	}); err != nil {
		t.Fatalf("mutable patch: %v", err)
	}
	_ = inTx(t, s, func(ctx context.Context, tx repo.TxRepository) error {
		got, _, _ := tx.GetMedia(ctx, "ep1")
		if got.Identity() != episode.Identity() || !got.LastPlayedAt.Equal(played) || got.Name != "Renamed" {
			t.Fatalf("after patch: %+v", got)
		}
		return nil
	})
	if err := inTx(t, s, func(ctx context.Context, tx repo.TxRepository) error {
		return tx.PatchMedia(ctx, "missing", func(*domain.MediaItem) {})
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("patch missing row: got %v, want ErrNotFound", err)
	}
}

// Reconcile is the one path allowed to change identity: Jellyfin is the
// authority, so a legacy blank is filled and an upstream renumbering moves the
// episode. It still refuses to blank a row or park it in "Season Unknown".
func TestReconcileMediaIdentityFollowsJellyfin(t *testing.T) {
	s := openIdentityStore(t)
	legacy := []byte(`{"item_id":"legacy","item_type":"Episode","series_id":"series1","season_id":"","season_name":"Season Unknown"}`)
	if err := s.db.Update(func(tx *bbolt.Tx) error { return tx.Bucket(bucketMedia).Put([]byte("legacy"), legacy) }); err != nil {
		t.Fatalf("seed legacy: %v", err)
	}
	reconcile := func(id domain.MediaIdentity, name string) (domain.MediaIdentity, bool, error) {
		var before domain.MediaIdentity
		var changed bool
		err := inTx(t, s, func(ctx context.Context, tx repo.TxRepository) error {
			var err error
			before, changed, err = tx.ReconcileMediaIdentity(ctx, "legacy", id, name)
			return err
		})
		return before, changed, err
	}
	s1 := domain.MediaIdentity{ItemType: "Episode", SeriesID: "series1", SeasonID: "season1", SeasonNumber: 1, Placed: true}

	if _, changed, err := reconcile(s1, "Season 1"); err != nil || !changed {
		t.Fatalf("fill legacy: changed=%v err=%v", changed, err)
	}
	if _, changed, _ := reconcile(s1, "Season 1"); changed {
		t.Fatal("reconciling the same identity must report no change")
	}
	moved := domain.MediaIdentity{ItemType: "Episode", SeriesID: "series1", SeasonID: "season2", SeasonNumber: 2, Placed: true}
	before, changed, err := reconcile(moved, "Season 2")
	if err != nil || !changed || before != s1 {
		t.Fatalf("upstream renumber: before=%+v changed=%v err=%v", before, changed, err)
	}
	unplaced := domain.MediaIdentity{ItemType: "Episode", SeriesID: "series1", SeasonID: "season2"}
	if _, _, err := reconcile(unplaced, "Season Unknown"); !errors.Is(err, domain.ErrIncompleteIdentity) {
		t.Fatalf("parking in Season Unknown: got %v, want ErrIncompleteIdentity", err)
	}
	_ = inTx(t, s, func(ctx context.Context, tx repo.TxRepository) error {
		got, _, _ := tx.GetMedia(ctx, "legacy")
		if got.Identity() != moved || got.SeasonName != "Season 2" {
			t.Fatalf("final row: %+v %q", got.Identity(), got.SeasonName)
		}
		return nil
	})
}

func TestPruneDedupeRemovesOnlyStaleRecordsInBatches(t *testing.T) {
	s := openIdentityStore(t)
	cutoff := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	if err := inTx(t, s, func(ctx context.Context, tx repo.TxRepository) error {
		for i := 0; i < 25; i++ {
			if err := tx.MarkProcessed(ctx, "old:"+strconv.Itoa(i), cutoff.Add(-time.Hour)); err != nil {
				return err
			}
		}
		return tx.MarkProcessed(ctx, "fresh", cutoff.Add(time.Hour))
	}); err != nil {
		t.Fatal(err)
	}
	var batches []int
	for {
		var n int
		_ = inTx(t, s, func(ctx context.Context, tx repo.TxRepository) error {
			var err error
			n, err = tx.PruneDedupe(ctx, cutoff, 10)
			return err
		})
		batches = append(batches, n)
		if n < 10 {
			break
		}
	}
	if len(batches) != 3 || batches[0] != 10 || batches[1] != 10 || batches[2] != 5 {
		t.Fatalf("batches %v, want [10 10 5]", batches)
	}
	_ = inTx(t, s, func(ctx context.Context, tx repo.TxRepository) error {
		if ok, _ := tx.IsProcessed(ctx, "fresh"); !ok {
			t.Fatal("a record newer than the cutoff was pruned")
		}
		return nil
	})
}

func TestScheduleOnlyUpsertKeepsSearchIndexAndRenameRebuildsIt(t *testing.T) {
	s := openIdentityStore(t)
	flow := domain.Flow{FlowID: "flow:target:season:s1", ItemID: "target:season:s1", SubjectType: "season", DisplayName: "Season 1 of Invincible", State: domain.FlowStateActive}
	_ = inTx(t, s, func(ctx context.Context, tx repo.TxRepository) error { return tx.UpsertFlowCAS(ctx, flow, 0) })
	triKeys := func() int {
		n := 0
		_ = s.db.View(func(tx *bbolt.Tx) error { n = tx.Bucket(bucketFlowSearchTri).Stats().KeyN; return nil })
		return n
	}
	before := triKeys()
	flow.NextActionAt = time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	flow.Version = 1
	_ = inTx(t, s, func(ctx context.Context, tx repo.TxRepository) error { return tx.UpsertFlowCAS(ctx, flow, 0) })
	if triKeys() != before {
		t.Fatalf("schedule-only upsert changed the trigram index: %d → %d", before, triKeys())
	}
	flow.DisplayName = "Season 1 of Severance"
	flow.Version = 2
	_ = inTx(t, s, func(ctx context.Context, tx repo.TxRepository) error { return tx.UpsertFlowCAS(ctx, flow, 1) })
	_ = inTx(t, s, func(ctx context.Context, tx repo.TxRepository) error {
		got, err := tx.SearchFlows(ctx, "severance", "season", 5)
		if err != nil || len(got) != 1 {
			t.Fatalf("renamed flow not searchable: %v %v", got, err)
		}
		if old, _ := tx.SearchFlows(ctx, "invincible", "season", 5); len(old) != 0 {
			t.Fatalf("stale name still indexed: %v", old)
		}
		return nil
	})
}
