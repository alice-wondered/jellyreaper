package jellyfin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

func TestFetchIdentity(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("Ids") == "" {
			t.Fatalf("missing Ids: %s", r.URL)
		}
		if r.URL.Query().Get("Ids") == "gone" {
			_ = json.NewEncoder(w).Encode(map[string]any{"Items": []any{}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"Items": []map[string]any{{"Id": "40a902ca-3483-8d11-1ba4-845e90c39d40", "Type": "Episode", "SeriesId": "9E2015F5-B826-44F4-98DB-4FB0AF83AFC6", "SeasonId": "c9f8920d-fd52-4122-51ea-49ff7f0b366d", "SeasonName": "Season 1", "ParentIndexNumber": 1}}})
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "k", srv.Client())

	res, found, err := c.FetchIdentity(context.Background(), "40a902ca34838d111ba4845e90c39d40")
	if err != nil || !found {
		t.Fatalf("fetch: found=%v err=%v", found, err)
	}
	id := res.Identity
	if !id.Placed || id.SeasonNumber != 1 || res.SeasonName != "Season 1" {
		t.Fatalf("season placement not read: %+v %q", id, res.SeasonName)
	}
	if id.SeasonID != "c9f8920dfd52412251ea49ff7f0b366d" || id.SeriesID != "9e2015f5b82644f498db4fb0af83afc6" || id.ItemType != "Episode" {
		t.Fatalf("identity not normalized: %+v", id)
	}
	if _, found, err := c.FetchIdentity(context.Background(), "gone"); found || err != nil {
		t.Fatalf("missing item: found=%v err=%v", found, err)
	}
}

func TestListEpisodeIdentitiesPages(t *testing.T) {
	const total = 5
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, _ := strconv.Atoi(r.URL.Query().Get("StartIndex"))
		limit, _ := strconv.Atoi(r.URL.Query().Get("Limit"))
		items := []map[string]any{}
		for i := start; i < total && i < start+limit; i++ {
			items = append(items, map[string]any{"Id": "ep" + strconv.Itoa(i), "Type": "Episode", "SeriesId": "s", "SeasonId": "se"})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"Items": items})
	}))
	defer srv.Close()

	for _, pageSize := range []int{1, 2, total, total + 1} {
		got, err := NewClient(srv.URL, "k", srv.Client()).ListEpisodeIdentities(context.Background(), pageSize)
		if err != nil || len(got) != total {
			t.Fatalf("pageSize=%d: got %d identities, err %v", pageSize, len(got), err)
		}
	}
}
