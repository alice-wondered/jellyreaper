package sonarr

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func seriesDetail() map[string]any {
	return map[string]any{
		"id":               77,
		"title":            "Sample Series",
		"qualityProfileId": 4,
		"seasons": []map[string]any{
			{"seasonNumber": 1, "monitored": true},
			{"seasonNumber": 3, "monitored": true},
		},
	}
}

func TestRemoveSeasonByProviderIDsUnmonitorsSeasonFlag(t *testing.T) {
	var sawBulkDelete bool
	var put map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/series":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 77, "tvdbId": 73244, "imdbId": "tt0386676", "title": "Sample Series"}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/series/77":
			_ = json.NewEncoder(w).Encode(seriesDetail())
		case r.Method == http.MethodPut && r.URL.Path == "/api/v3/series/77":
			if err := json.NewDecoder(r.Body).Decode(&put); err != nil {
				t.Fatalf("decode series put body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/episode":
			if got := r.URL.Query().Get("seriesId"); got != "77" {
				t.Fatalf("expected seriesId=77, got %q", got)
			}
			if got := r.URL.Query().Get("seasonNumber"); got != "3" {
				t.Fatalf("expected seasonNumber=3, got %q", got)
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 1001, "episodeFileId": 501}, {"id": 1002, "episodeFileId": 502}})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v3/episodefile/bulk":
			sawBulkDelete = true
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected sonarr call %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	svc := NewService(server.URL, "k")
	if err := svc.RemoveSeasonByProviderIDs(context.Background(), map[string]string{"tvdb": "73244"}, 3); err != nil {
		t.Fatalf("remove season by provider ids: %v", err)
	}
	if !sawBulkDelete {
		t.Fatal("expected matched season episode files to be deleted")
	}
	if put == nil {
		t.Fatal("expected series PUT flipping the season monitored flag")
	}
	monitored := map[float64]any{}
	for _, raw := range put["seasons"].([]any) {
		season := raw.(map[string]any)
		monitored[season["seasonNumber"].(float64)] = season["monitored"]
	}
	if monitored[3] != false {
		t.Fatalf("expected season 3 monitored=false, got %#v", monitored[3])
	}
	if monitored[1] != true {
		t.Fatalf("expected season 1 untouched, got %#v", monitored[1])
	}
	if put["qualityProfileId"] != float64(4) || put["title"] != "Sample Series" {
		t.Fatalf("expected unmodeled series fields to round-trip, got %#v", put)
	}
}

func TestRemoveSeasonByProviderIDsSeasonMissingFromSeriesIsNotManaged(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/series":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 77, "tvdbId": 73244}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/episode":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 1001, "episodeFileId": 0}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/series/77":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 77, "seasons": []map[string]any{{"seasonNumber": 1, "monitored": true}}})
		case r.Method == http.MethodPut:
			t.Fatal("did not expect a series PUT when the season is absent")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	err := NewService(server.URL, "k").RemoveSeasonByProviderIDs(context.Background(), map[string]string{"tvdb": "73244"}, 3)
	if !errors.Is(err, ErrNotManaged) {
		t.Fatalf("expected ErrNotManaged, got %v", err)
	}
}

func TestRemoveSeasonByProviderIDsMonitor404IsIdempotent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/series":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 77, "tvdbId": 73244, "imdbId": "tt0386676"}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/episode":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 1001, "episodeFileId": 0}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/series/77":
			_ = json.NewEncoder(w).Encode(seriesDetail())
		case r.Method == http.MethodPut && r.URL.Path == "/api/v3/series/77":
			// Series disappeared between our GET and our PUT.
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	svc := NewService(server.URL, "k")
	if err := svc.RemoveSeasonByProviderIDs(context.Background(), map[string]string{"tvdb": "73244"}, 3); err != nil {
		t.Fatalf("expected 404 to be treated as success, got %v", err)
	}
}

func TestRemoveSeasonByProviderIDsNoMatchReturnsError(t *testing.T) {
	monitorCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/series":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 2, "tvdbId": 2, "imdbId": "tt0000002"}})
		case r.Method == http.MethodPut:
			monitorCalls++
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	svc := NewService(server.URL, "k")
	err := svc.RemoveSeasonByProviderIDs(context.Background(), map[string]string{"tvdb": "999999"}, 3)
	if err == nil {
		t.Fatal("expected not-found error for unmatched provider ids")
	}
	if !errors.Is(err, ErrNotManaged) {
		t.Fatalf("expected ErrNotManaged, got %v", err)
	}
	if monitorCalls != 0 {
		t.Fatalf("expected no monitor update call for unmatched provider ids, got %d", monitorCalls)
	}
}
