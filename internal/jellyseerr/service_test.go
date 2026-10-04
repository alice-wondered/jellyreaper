package jellyseerr

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClearMediaDeletesRecordForKind(t *testing.T) {
	for _, tc := range []struct {
		kind   MediaKind
		lookup string
	}{
		{MediaKindMovie, "/api/v1/movie/603"},
		{MediaKindTV, "/api/v1/tv/603"},
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			deleted := ""
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Api-Key") != "k" {
					t.Fatalf("missing api key header")
				}
				switch {
				case r.Method == http.MethodGet && r.URL.Path == tc.lookup:
					_ = json.NewEncoder(w).Encode(map[string]any{"id": 603, "mediaInfo": map[string]any{"id": 42, "status": 5}})
				case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/media/42":
					deleted = r.URL.Path
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Fatalf("unexpected call %s %s", r.Method, r.URL.Path)
				}
			}))
			defer server.Close()

			if err := NewService(server.URL, "k").ClearMedia(context.Background(), tc.kind, map[string]string{"tmdb": "603"}); err != nil {
				t.Fatalf("clear media: %v", err)
			}
			if deleted == "" {
				t.Fatal("expected media record delete")
			}
		})
	}
}

func TestClearMediaNoRecordIsNoop(t *testing.T) {
	for name, handler := range map[string]http.HandlerFunc{
		"lookup 404":   func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) },
		"no mediaInfo": func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(map[string]any{"id": 603}) },
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete {
					t.Fatal("did not expect a delete without a media record")
				}
				handler(w, r)
			}))
			defer server.Close()
			if err := NewService(server.URL, "k").ClearMedia(context.Background(), MediaKindMovie, map[string]string{"tmdb": "603"}); err != nil {
				t.Fatalf("expected no-op, got %v", err)
			}
		})
	}
}

func TestClearMediaRequiresTMDB(t *testing.T) {
	for _, ids := range []map[string]string{nil, {"imdb": "tt0133093"}, {"tmdb": "abc"}, {"tmdb": "0"}} {
		if err := NewService("http://unused", "k").ClearMedia(context.Background(), MediaKindMovie, ids); !errors.Is(err, ErrNoTMDB) {
			t.Fatalf("ids %v: expected ErrNoTMDB, got %v", ids, err)
		}
	}
}

func TestClearMediaSurfacesServerErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"mediaInfo": map[string]any{"id": 42}})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	if err := NewService(server.URL, "k").ClearMedia(context.Background(), MediaKindMovie, map[string]string{"tmdb": "603"}); err == nil {
		t.Fatal("expected 500 on delete to surface as an error")
	}
}
