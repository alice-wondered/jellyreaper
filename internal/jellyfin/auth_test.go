package jellyfin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const testAPIKey = "k3y"

func modernAuthServer(t *testing.T, body string) *httptest.Server {
	return modernAuthServerRoutes(t, map[string]string{"": body})
}

// modernAuthServerRoutes mimics Jellyfin 12 with EnableLegacyAuthorization=false:
// only the Authorization header authenticates, and a request that also
// carries a legacy credential is treated as legacy and rejected. Bodies are
// keyed by exact path, falling back to routes[""].
func modernAuthServerRoutes(t *testing.T, routes map[string]string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		legacy := r.Header.Get("X-Emby-Token") != "" || r.Header.Get("X-MediaBrowser-Token") != "" || r.URL.Query().Get("api_key") != ""
		if legacy || r.Header.Get("Authorization") != `MediaBrowser Token="`+testAPIKey+`"` {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		body, ok := routes[r.URL.Path]
		if !ok {
			body = routes[""]
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestSetAuthUsesMediaBrowserAuthorizationHeader(t *testing.T) {
	h := http.Header{}
	SetAuth(h, testAPIKey)
	if got, want := h.Get("Authorization"), `MediaBrowser Token="k3y"`; got != want {
		t.Fatalf("Authorization = %q, want %q", got, want)
	}
	if h.Get("X-Emby-Token") != "" {
		t.Fatal("legacy X-Emby-Token header set")
	}
}

func TestClientAuthenticatesWithoutLegacyAuthorization(t *testing.T) {
	ctx := context.Background()

	t.Run("Ping", func(t *testing.T) {
		c := NewClient(modernAuthServer(t, `{}`).URL, testAPIKey, nil)
		if err := c.Ping(ctx); err != nil {
			t.Fatalf("ping: %v", err)
		}
	})
	t.Run("DeleteItem", func(t *testing.T) {
		c := NewClient(modernAuthServer(t, ``).URL, testAPIKey, nil)
		if err := c.DeleteItem(ctx, "abc"); err != nil {
			t.Fatalf("delete: %v", err)
		}
	})
	t.Run("FetchProviderIDs", func(t *testing.T) {
		c := NewClient(modernAuthServer(t, `{"Items":[]}`).URL, testAPIKey, nil)
		if _, err := c.FetchProviderIDs(ctx, "abc"); err != nil {
			t.Fatalf("fetch provider ids: %v", err)
		}
	})
	t.Run("CheckItemsExist", func(t *testing.T) {
		c := NewClient(modernAuthServer(t, `{"Items":[]}`).URL, testAPIKey, nil)
		if _, err := c.CheckItemsExist(ctx, []string{"abc"}); err != nil {
			t.Fatalf("check items exist: %v", err)
		}
	})
	t.Run("FetchIdentity", func(t *testing.T) {
		c := NewClient(modernAuthServer(t, `{"Items":[]}`).URL, testAPIKey, nil)
		if _, _, err := c.FetchIdentity(ctx, "abc"); err != nil {
			t.Fatalf("fetch identity: %v", err)
		}
	})
}

func TestBackfillAuthenticatesWithoutLegacyAuthorization(t *testing.T) {
	ctx := context.Background()

	t.Run("generated client", func(t *testing.T) {
		srv := modernAuthServerRoutes(t, map[string]string{"": `{"Items":[],"TotalRecordCount":0}`, "/Users": `[]`})
		b, err := NewBackfillService(srv.URL, testAPIKey, srv.Client())
		if err != nil {
			t.Fatalf("new backfill service: %v", err)
		}
		if _, err := b.FetchChangedItemsPage(ctx, time.Now().Add(-time.Hour), 0, 10); err != nil {
			t.Fatalf("fetch changed items page: %v", err)
		}
	})
	t.Run("getJSONWithRetry", func(t *testing.T) {
		srv := modernAuthServer(t, `[]`)
		b, err := NewBackfillService(srv.URL, testAPIKey, srv.Client())
		if err != nil {
			t.Fatalf("new backfill service: %v", err)
		}
		var out []any
		if err := b.getJSONWithRetry(ctx, srv.URL+"/Users", "fetch users", 1<<20, &out); err != nil {
			t.Fatalf("get json: %v", err)
		}
	})
}
