package sonarr

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRemoveSeasonByProviderIDsUpdatesEpisodeMonitorState(t *testing.T) {
	var sawBulkDelete bool
	var sawMonitor bool
	var sawSeriesDelete bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/series":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 77, "tvdbId": 73244, "imdbId": "tt0386676", "title": "Sample Series"}})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v3/series"):
			sawSeriesDelete = true
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/episode":
			if got := r.URL.Query().Get("seriesId"); got != "77" {
				t.Fatalf("expected seriesId=77, got %q", got)
			}
			if got := r.URL.Query().Get("seasonNumber"); got != "3" {
				t.Fatalf("expected seasonNumber=3, got %q", got)
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 1001, "episodeFileId": 501}, {"id": 1002, "episodeFileId": 502}})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v3/episodefile/bulk":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode bulk delete body: %v", err)
			}
			sawBulkDelete = true
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut && r.URL.Path == "/api/v3/episode/monitor":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode monitor body: %v", err)
			}
			if body["monitored"] != false {
				t.Fatalf("expected monitored=false, got %#v", body["monitored"])
			}
			sawMonitor = true
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	svc := NewService(server.URL, "k")
	if err := svc.RemoveSeasonByProviderIDs(context.Background(), map[string]string{"tvdb": "73244"}, 3); err != nil {
		t.Fatalf("remove season by provider ids: %v", err)
	}
	if !sawMonitor {
		t.Fatal("expected matched season episodes to be unmonitored")
	}
	if !sawBulkDelete {
		t.Fatal("expected matched season episode files to be deleted")
	}
	if sawSeriesDelete {
		t.Fatal("did not expect any sonarr series delete endpoint call during season operation")
	}
}

func TestRemoveSeasonByProviderIDsNoMatchReturnsError(t *testing.T) {
	monitorCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/series":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 2, "tvdbId": 2, "imdbId": "tt0000002"}})
		case r.Method == http.MethodPut && r.URL.Path == "/api/v3/episode/monitor":
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
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not-found error, got %v", err)
	}
	if monitorCalls != 0 {
		t.Fatalf("expected no monitor update call for unmatched provider ids, got %d", monitorCalls)
	}
}
