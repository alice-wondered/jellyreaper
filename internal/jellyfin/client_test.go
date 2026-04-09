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

func TestProviderIDCandidateStripsDashedHexIDsEvenIfNonRFCUUID(t *testing.T) {
	in := "f8f13c13-eae5-0047-57eb-3308105503b9"
	want := "f8f13c13eae5004757eb3308105503b9"
	if got := providerIDCandidate(in); got != want {
		t.Fatalf("providerIDCandidate(%q)=%q want=%q", in, got, want)
	}
}

func TestProviderIDCandidatePreservesNonHexIDs(t *testing.T) {
	in := "series-provider-1"
	if got := providerIDCandidate(in); got != in {
		t.Fatalf("providerIDCandidate(%q)=%q want=%q", in, got, in)
	}
}
