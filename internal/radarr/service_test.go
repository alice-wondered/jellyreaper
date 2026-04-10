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
	var sawFileList, sawFileBulkDelete, sawMovieDelete bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/movie":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 42, "tmdbId": 603, "imdbId": "tt0133093", "title": "Sample Movie"}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/moviefile":
			if got := r.URL.Query().Get("movieId"); got != "42" {
				t.Fatalf("expected movieId=42, got %q", got)
			}
			sawFileList = true
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 101}, {"id": 102}})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v3/moviefile/bulk":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode bulk delete body: %v", err)
			}
			ids, ok := body["movieFileIds"].([]any)
			if !ok || len(ids) != 2 {
				t.Fatalf("expected 2 movieFileIds, got %#v", body["movieFileIds"])
			}
			sawFileBulkDelete = true
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v3/movie/42":
			if got := r.URL.Query().Get("deleteFiles"); got != "false" {
				t.Fatalf("expected deleteFiles=false (files already removed), got %q", got)
			}
			if got := r.Header.Get("X-Api-Key"); got != "k" {
				t.Fatalf("expected api key header, got %q", got)
			}
			sawMovieDelete = true
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
	if !sawFileList {
		t.Fatal("expected moviefile list call")
	}
	if !sawFileBulkDelete {
		t.Fatal("expected moviefile bulk delete call")
	}
	if !sawMovieDelete {
		t.Fatal("expected movie entry delete call")
	}
}

func TestRemoveByProviderIDs404IsIdempotent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/movie":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 7, "tmdbId": 603, "imdbId": "tt0133093"}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/moviefile":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
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
