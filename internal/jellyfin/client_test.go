package jellyfin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchProviderIDsFallsBackToAlternateIDForm(t *testing.T) {
	const dashed = "bda444ed-c4e7-4bbb-6677-3cbe94938d10"
	const nodash = "bda444edc4e74bbb66773cbe94938d10"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/Items/" + dashed:
			w.WriteHeader(http.StatusBadRequest)
			return
		case "/Items/" + nodash:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ProviderIds":{"Tvdb":"73244","Imdb":"tt0386676"}}`))
			return
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "api-key", server.Client())
	ids, err := client.FetchProviderIDs(context.Background(), dashed)
	if err != nil {
		t.Fatalf("fetch provider ids: %v", err)
	}
	if ids["tvdb"] != "73244" {
		t.Fatalf("expected tvdb provider id from alternate id form, got %q", ids["tvdb"])
	}
	if ids["imdb"] != "tt0386676" {
		t.Fatalf("expected imdb provider id from alternate id form, got %q", ids["imdb"])
	}
}
