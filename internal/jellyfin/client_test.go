package jellyfin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchProviderIDsPrefersNoDashUUIDForm(t *testing.T) {
	const dashed = "bda444ed-c4e7-4bbb-6677-3cbe94938d10"
	const nodash = "bda444edc4e74bbb66773cbe94938d10"
	dashedCalls := 0
	nodashCalls := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/Items/" + dashed:
			dashedCalls++
			w.WriteHeader(http.StatusBadRequest)
			return
		case "/Items/" + nodash:
			nodashCalls++
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
	if nodashCalls != 1 {
		t.Fatalf("expected nodash form to be queried once, got %d", nodashCalls)
	}
	if dashedCalls != 0 {
		t.Fatalf("did not expect dashed fallback when nodash succeeds, got %d dashed calls", dashedCalls)
	}
}

func TestFetchProviderIDsFallsBackToDashedFormWhenNeeded(t *testing.T) {
	const dashed = "bda444ed-c4e7-4bbb-6677-3cbe94938d10"
	const nodash = "bda444edc4e74bbb66773cbe94938d10"
	dashedCalls := 0
	nodashCalls := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/Items/" + nodash:
			nodashCalls++
			w.WriteHeader(http.StatusBadRequest)
			return
		case "/Items/" + dashed:
			dashedCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ProviderIds":{"Tvdb":"111","Imdb":"tt0111"}}`))
			return
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "api-key", server.Client())
	ids, err := client.FetchProviderIDs(context.Background(), dashed)
	if err != nil {
		t.Fatalf("fetch provider ids with fallback: %v", err)
	}
	if ids["tvdb"] != "111" {
		t.Fatalf("expected tvdb provider id from dashed fallback, got %q", ids["tvdb"])
	}
	if nodashCalls != 1 || dashedCalls != 1 {
		t.Fatalf("expected one nodash attempt and one dashed fallback, got nodash=%d dashed=%d", nodashCalls, dashedCalls)
	}
}
