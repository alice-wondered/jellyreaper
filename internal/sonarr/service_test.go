package sonarr

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRemoveByProviderIDsDeletesMatchedSeries(t *testing.T) {
	var sawDelete bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/series":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 77, "tvdbId": 73244, "imdbId": "tt0386676", "title": "Sample Series"}})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v3/series/77":
			if got := r.URL.Query().Get("deleteFiles"); got != "false" {
				t.Fatalf("expected deleteFiles=false, got %q", got)
			}
			if got := r.URL.Query().Get("addImportListExclusion"); got != "false" {
				t.Fatalf("expected addImportListExclusion=false, got %q", got)
			}
			if got := r.Header.Get("X-Api-Key"); got != "k" {
				t.Fatalf("expected api key header, got %q", got)
			}
			sawDelete = true
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	svc := NewService(server.URL, "k")
	if err := svc.RemoveByProviderIDs(context.Background(), map[string]string{"tvdb": "73244"}); err != nil {
		t.Fatalf("remove by provider ids: %v", err)
	}
	if !sawDelete {
		t.Fatal("expected matched series to be deleted")
	}
}

func TestRemoveByProviderIDsNoMatchSkipsDelete(t *testing.T) {
	deleteCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/series":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 2, "tvdbId": 2, "imdbId": "tt0000002"}})
		case r.Method == http.MethodDelete:
			deleteCalls++
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	svc := NewService(server.URL, "k")
	err := svc.RemoveByProviderIDs(context.Background(), map[string]string{"tvdb": "999999"})
	if err == nil {
		t.Fatal("expected not-found error for unmatched provider ids")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not-found error, got %v", err)
	}
	if deleteCalls != 0 {
		t.Fatalf("expected no delete call for unmatched provider ids, got %d", deleteCalls)
	}
}
