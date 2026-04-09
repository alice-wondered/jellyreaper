package radarr

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRemoveByProviderIDsDeletesMatchedMovie(t *testing.T) {
	var sawDelete bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/movie":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 42, "tmdbId": 603, "imdbId": "tt0133093", "title": "Sample Movie"}})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v3/movie/42":
			if got := r.URL.Query().Get("deleteFiles"); got != "true" {
				t.Fatalf("expected deleteFiles=true, got %q", got)
			}
			if got := r.URL.Query().Get("addImportExclusion"); got != "false" {
				t.Fatalf("expected addImportExclusion=false, got %q", got)
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
	if err := svc.RemoveByProviderIDs(context.Background(), map[string]string{"tmdb": "603"}); err != nil {
		t.Fatalf("remove by provider ids: %v", err)
	}
	if !sawDelete {
		t.Fatal("expected matched movie to be deleted")
	}
}

func TestRemoveByProviderIDs404IsIdempotent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/movie":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 7, "tmdbId": 603, "imdbId": "tt0133093"}})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v3/movie/7":
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	svc := NewService(server.URL, "k")
	if err := svc.RemoveByProviderIDs(context.Background(), map[string]string{"tmdb": "603"}); err != nil {
		t.Fatalf("expected 404 to be treated as success, got %v", err)
	}
}

func TestRemoveByProviderIDsNoMatchSkipsDelete(t *testing.T) {
	deleteCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/movie":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 1, "tmdbId": 1, "imdbId": "tt0000001"}})
		case r.Method == http.MethodDelete:
			deleteCalls++
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	svc := NewService(server.URL, "k")
	err := svc.RemoveByProviderIDs(context.Background(), map[string]string{"tmdb": "999999"})
	if err == nil {
		t.Fatal("expected not-found error for unmatched provider ids")
	}
	if !errors.Is(err, ErrNotManaged) {
		t.Fatalf("expected ErrNotManaged, got %v", err)
	}
	if deleteCalls != 0 {
		t.Fatalf("expected no delete call for unmatched provider ids, got %d", deleteCalls)
	}
}
