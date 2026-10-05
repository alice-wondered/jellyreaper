package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	bbolt "go.etcd.io/bbolt"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/jellyfin"
	"jellyreaper/internal/repo"
	bboltrepo "jellyreaper/internal/repo/bbolt"
)

const (
	idEpisode = "40a902ca34838d111ba4845e90c39d40"
	idSeason  = "c9f8920dfd52412251ea49ff7f0b366d"
	idSeries  = "9e2015f5b82644f498db4fb0af83afc6"
)

type jfEpisode struct {
	id, season, seasonName string
	number                 *int // nil: Jellyfin has not placed it ("Season Unknown")
}

func num(n int) *int { return &n }

// fakeJellyfin serves Jellyfin's real shape for the identity lookup and the
// paginated episode listing from a mutable catalog, and counts lookups.
func fakeJellyfin(t *testing.T, lookups *int32, catalog *[]jfEpisode) *jellyfin.Client {
	t.Helper()
	dto := func(e jfEpisode) map[string]any {
		d := map[string]any{"Id": e.id, "Type": "Episode", "SeriesId": idSeries, "SeasonId": e.season, "SeasonName": e.seasonName, "ProviderIds": map[string]string{"Tvdb": "368207"}}
		if e.number != nil {
			d["ParentIndexNumber"] = *e.number
		}
		return d
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		items := []any{}
		switch q := r.URL.Query(); {
		case q.Get("Ids") != "":
			if q.Get("Fields") != "ProviderIds" { // count identity lookups only
				atomic.AddInt32(lookups, 1)
			}
			for _, e := range *catalog {
				if e.id == q.Get("Ids") {
					items = append(items, dto(e))
				}
			}
		case q.Get("StartIndex") == "0":
			for _, e := range *catalog {
				items = append(items, dto(e))
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"Items": items})
	}))
	t.Cleanup(srv.Close)
	return jellyfin.NewClient(srv.URL, "k", srv.Client())
}

// identityService opens a store (optionally pre-seeded with raw legacy rows
// that no current API could write) behind a Service talking to fakeJellyfin.
func identityService(t *testing.T, legacyRows map[string]string, catalog *[]jfEpisode) (*Service, *int32, *time.Time) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "identity.db")
	if len(legacyRows) > 0 {
		raw, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: time.Second})
		if err != nil {
			t.Fatalf("open raw: %v", err)
		}
		if err := raw.Update(func(tx *bbolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("media"))
			if err != nil {
				return err
			}
			for id, row := range legacyRows {
				if err := b.Put([]byte(id), []byte(row)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("seed legacy rows: %v", err)
		}
		_ = raw.Close()
	}
	store, err := bboltrepo.Open(path, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	svc := NewService(store, nil, nil)
	clock := time.Date(2026, 10, 3, 17, 41, 38, 0, time.UTC)
	svc.now = func() time.Time { return clock }
	svc.SyncFlowManagerClock()
	var lookups int32
	svc.SetJellyfinClient(fakeJellyfin(t, &lookups, catalog))
	return svc, &lookups, &clock
}

func season1Catalog() *[]jfEpisode {
	return &[]jfEpisode{{id: idEpisode, season: idSeason, seasonName: "Season 1", number: num(1)}}
}

func episodeEvent(eventType, seasonID, dedupe string) jellyfin.WebhookEvent {
	return jellyfin.WebhookEvent{
		Payload:   jellyfin.WebhookPayload{ItemID: idEpisode, ItemType: "Episode", Name: "IT'S ABOUT TIME", SeasonID: seasonID, NotificationType: eventType, SeasonNumber: seasonNo(1)},
		ItemID:    idEpisode,
		EventType: eventType,
		DedupeKey: "jellyfin:" + eventType + ":" + dedupe,
	}
}

func getMediaFor(t *testing.T, svc *Service, id string) (domain.MediaItem, bool) {
	t.Helper()
	var m domain.MediaItem
	var found bool
	_ = svc.repository.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		var err error
		m, found, err = tx.GetMedia(ctx, id)
		return err
	})
	return m, found
}

func getFlowFor(t *testing.T, svc *Service, id string) (domain.Flow, bool) {
	t.Helper()
	var f domain.Flow
	var found bool
	_ = svc.repository.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		var err error
		f, found, err = tx.GetFlow(ctx, id)
		return err
	})
	return f, found
}

func TestItemAddedWithoutSeasonIDResolvesFromJellyfin(t *testing.T) {
	svc, lookups, _ := identityService(t, nil, season1Catalog())
	ctx := context.Background()

	if err := svc.HandleJellyfinWebhook(ctx, episodeEvent("ItemAdded", "", "added")); err != nil {
		t.Fatalf("itemadded: %v", err)
	}
	media, _ := getMediaFor(t, svc, idEpisode)
	if id := media.Identity(); id.SeasonID != idSeason || id.SeriesID != idSeries || !id.Placed || id.SeasonNumber != 1 {
		t.Fatalf("identity not resolved at ingest: %+v", id)
	}
	if _, found := getFlowFor(t, svc, "target:season:"+idSeason); !found {
		t.Fatal("season flow must exist from the ItemAdded itself")
	}
	before := atomic.LoadInt32(lookups)
	for _, d := range []string{"a", "b", "c"} {
		if err := svc.HandleJellyfinWebhook(ctx, episodeEvent("PlaybackProgress", "", d)); err != nil {
			t.Fatalf("progress: %v", err)
		}
	}
	if extra := atomic.LoadInt32(lookups) - before; extra != 0 {
		t.Fatalf("a resolved identity is cached; made %d extra lookups", extra)
	}
}

func TestLaterEventCannotMoveEpisodeSeason(t *testing.T) {
	svc, _, _ := identityService(t, nil, season1Catalog())
	ctx := context.Background()
	if err := svc.HandleJellyfinWebhook(ctx, episodeEvent("ItemAdded", "", "added")); err != nil {
		t.Fatalf("itemadded: %v", err)
	}
	_ = svc.HandleJellyfinWebhook(ctx, episodeEvent("ItemUpdated", "0000000000000000000000000000beef", "conflict"))
	if media, _ := getMediaFor(t, svc, idEpisode); media.SeasonID != idSeason {
		t.Fatalf("an ingest event moved the season to %q", media.SeasonID)
	}
}

func TestSpecialsIndexedAsSeasonZero(t *testing.T) {
	catalog := &[]jfEpisode{{id: idEpisode, season: idSeason, seasonName: "Specials", number: num(0)}}
	svc, _, _ := identityService(t, nil, catalog)
	if err := svc.HandleJellyfinWebhook(context.Background(), episodeEvent("ItemAdded", "", "added")); err != nil {
		t.Fatalf("itemadded: %v", err)
	}
	media, found := getMediaFor(t, svc, idEpisode)
	if !found || !media.Identity().IsSpecials() || media.SeasonName != "Specials" {
		t.Fatalf("specials must index as season 0: found=%v %+v %q", found, media.Identity(), media.SeasonName)
	}
}

func TestUnplacedEpisodeIsNeitherIndexedNorCached(t *testing.T) {
	catalog := &[]jfEpisode{{id: idEpisode, season: "virtual", seasonName: "Season Unknown"}}
	svc, lookups, _ := identityService(t, nil, catalog)
	ctx := context.Background()
	if err := svc.HandleJellyfinWebhook(ctx, episodeEvent("ItemAdded", "", "added")); err != nil {
		t.Fatalf("itemadded: %v", err)
	}
	if _, found := getMediaFor(t, svc, idEpisode); found {
		t.Fatal("a Season Unknown episode must not be indexed into a placeholder season")
	}
	(*catalog)[0] = jfEpisode{id: idEpisode, season: idSeason, seasonName: "Season 1", number: num(1)} // Jellyfin finishes placing it
	if err := svc.HandleJellyfinWebhook(ctx, episodeEvent("PlaybackStart", "", "start")); err != nil {
		t.Fatalf("playback: %v", err)
	}
	if media, found := getMediaFor(t, svc, idEpisode); !found || media.SeasonID != idSeason {
		t.Fatalf("placed episode must index on the next event: found=%v season=%q", found, media.SeasonID)
	}
	if n := atomic.LoadInt32(lookups); n != 2 {
		t.Fatalf("the unplaced answer must not be cached: %d lookups, want 2", n)
	}
}

func TestReconcileMediaIdentitiesFollowsJellyfin(t *testing.T) {
	const (
		idEp2     = "aaaa0000aaaa0000aaaa0000aaaa0002"
		idSeason2 = "bbbb0000bbbb0000bbbb0000bbbb0002"
		idUnplace = "cccc0000cccc0000cccc0000cccc0003"
	)
	legacy := func(id string) string {
		return `{"item_id":"` + id + `","item_type":"Episode","series_id":"","season_id":"","season_name":"Season Unknown"}`
	}
	catalog := &[]jfEpisode{
		{id: idEpisode, season: idSeason, seasonName: "Season 1", number: num(1)},
		{id: idEp2, season: idSeason, seasonName: "Season 1", number: num(1)},
		{id: idUnplace, season: "dddd0000dddd0000dddd0000dddd0004", seasonName: "Season Unknown"},
	}
	svc, _, clock := identityService(t, map[string]string{idEpisode: legacy(idEpisode), idEp2: legacy(idEp2), idUnplace: legacy(idUnplace)}, catalog)
	ctx := context.Background()
	for _, id := range []string{idSeason, idSeason2} {
		if err := svc.repository.WithTx(ctx, func(ctx context.Context, tx repo.TxRepository) error {
			return tx.UpsertFlowCAS(ctx, domain.Flow{FlowID: "flow:target:season:" + id, ItemID: "target:season:" + id, SubjectType: "season", State: domain.FlowStateActive, Version: 3}, 0)
		}); err != nil {
			t.Fatalf("seed season flow: %v", err)
		}
	}
	count := func(id string) int {
		f, _ := getFlowFor(t, svc, "target:season:"+id)
		return f.EpisodeCount
	}

	res, err := svc.ReconcileMediaIdentities(ctx, 24*time.Hour)
	if err != nil || res.Moved != 2 || res.Unplaced != 1 || count(idSeason) != 2 {
		t.Fatalf("legacy fill: %+v err=%v season1 count=%d", res, err, count(idSeason))
	}
	if media, _ := getMediaFor(t, svc, idUnplace); media.SeasonID != "" {
		t.Fatalf("an unplaced episode must stay unlinked, got %q", media.SeasonID)
	}

	if res, _ := svc.ReconcileMediaIdentities(ctx, 24*time.Hour); !res.Skipped {
		t.Fatalf("a second run inside the interval must skip: %+v", res)
	}

	(*catalog)[1] = jfEpisode{id: idEp2, season: idSeason2, seasonName: "Season 2", number: num(2)} // upstream renumber
	*clock = clock.Add(25 * time.Hour)
	res, err = svc.ReconcileMediaIdentities(ctx, 24*time.Hour)
	if err != nil || res.Moved != 1 || res.Recounted != 2 {
		t.Fatalf("renumber: %+v err=%v", res, err)
	}
	if count(idSeason) != 1 || count(idSeason2) != 1 {
		t.Fatalf("counts after move: s1=%d s2=%d, want 1/1", count(idSeason), count(idSeason2))
	}
	if media, _ := getMediaFor(t, svc, idEp2); media.SeasonID != idSeason2 || media.SeasonName != "Season 2" {
		t.Fatalf("moved episode: season %q name %q", media.SeasonID, media.SeasonName)
	}
	if f, _ := getFlowFor(t, svc, "target:season:"+idSeason); f.Version != 3 {
		t.Fatalf("recount bumped the version to %d; queued jobs would go stale", f.Version)
	}
}
