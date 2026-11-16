package discord

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchImageAuthenticatesWithoutLegacyAuthorization(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Emby-Token") != "" || r.Header.Get("Authorization") != `MediaBrowser Token="k3y"` {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte("png"))
	}))
	defer srv.Close()

	s, err := NewService("", nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	s.SetJellyfinImageSource(srv.URL, "k3y")
	if _, _, err := s.fetchImage(context.Background(), srv.URL+"/Items/abc/Images/Primary"); err != nil {
		t.Fatalf("fetch image: %v", err)
	}
}
