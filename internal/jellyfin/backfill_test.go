package jellyfin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"

	gen "jellyreaper/internal/jellyfin/gen"
)

func TestBackfillFetchPlaybackEventsSince(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/System/ActivityLog/Entries" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		res := gen.ActivityLogEntryQueryResult{Items: &[]gen.ActivityLogEntry{
			{Type: strPtr("PlaybackStart"), ItemId: strPtr("item-1"), Name: strPtr("Movie A")},
			{Type: strPtr("ItemAdded"), ItemId: strPtr("item-2"), Name: strPtr("Movie B")},
		}}
		_ = json.NewEncoder(w).Encode(res)
	}))
	defer server.Close()

	b, err := NewBackfillService(server.URL, "token", server.Client())
	if err != nil {
		t.Fatalf("new backfill service: %v", err)
	}

	events, err := b.FetchPlaybackEventsSince(context.Background(), time.Now().Add(-time.Hour), 100)
	if err != nil {
		t.Fatalf("fetch playback events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("unexpected event count: %d", len(events))
	}
	if events[0].Type != "PlaybackStart" || events[0].ItemID != "item-1" {
		t.Fatalf("unexpected first event: %#v", events[0])
	}
}

func TestBackfillFetchChangedItemsSince(t *testing.T) {
	id := uuid.New()
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/Items" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		enableUserData := r.URL.Query().Get("EnableUserData")
		if enableUserData == "" {
			enableUserData = r.URL.Query().Get("enableUserData")
		}
		if enableUserData != "true" {
			t.Fatalf("expected EnableUserData=true query parameter")
		}
		playCount := int32(7)
		userData := gen.UserItemDataDto{LastPlayedDate: &now, PlayCount: &playCount}
		res := gen.BaseItemDtoQueryResult{Items: &[]gen.BaseItemDto{{Id: &id, Name: strPtr("Movie C"), DateCreated: &now, DateLastMediaAdded: &now, UserData: &userData}}}
		_ = json.NewEncoder(w).Encode(res)
	}))
	defer server.Close()

	b, err := NewBackfillService(server.URL, "token", server.Client())
	if err != nil {
		t.Fatalf("new backfill service: %v", err)
	}

	items, err := b.FetchChangedItemsSince(context.Background(), time.Now().Add(-24*time.Hour), 100)
	if err != nil {
		t.Fatalf("fetch changed items: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("unexpected changed item count: %d", len(items))
	}
	if items[0].ItemID != id.String() || items[0].Name != "Movie C" {
		t.Fatalf("unexpected changed item: %#v", items[0])
	}
	if items[0].PlayCount != 7 {
		t.Fatalf("unexpected play count: %d", items[0].PlayCount)
	}
	if items[0].LastPlayedAt.IsZero() {
		t.Fatal("expected last played timestamp")
	}
}

func strPtr(v string) *string { return &v }

func TestBackfillFetchChangedItemsSincePaginatesAllResults(t *testing.T) {
	now := time.Now().UTC()
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/Items" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		start, _ := strconv.Atoi(r.URL.Query().Get("startIndex"))
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 {
			limit = 500
		}

		if start >= len(ids) {
			_ = json.NewEncoder(w).Encode(gen.BaseItemDtoQueryResult{Items: &[]gen.BaseItemDto{}})
			return
		}
		end := start + limit
		if end > len(ids) {
			end = len(ids)
		}

		out := make([]gen.BaseItemDto, 0, end-start)
		for i := start; i < end; i++ {
			name := fmt.Sprintf("Item %d", i+1)
			out = append(out, gen.BaseItemDto{Id: &ids[i], Name: &name, DateCreated: &now, DateLastMediaAdded: &now})
		}
		_ = json.NewEncoder(w).Encode(gen.BaseItemDtoQueryResult{Items: &out})
	}))
	defer server.Close()

	b, err := NewBackfillService(server.URL, "token", server.Client())
	if err != nil {
		t.Fatalf("new backfill service: %v", err)
	}

	items, err := b.FetchChangedItemsSince(context.Background(), time.Now().Add(-24*time.Hour), 2)
	if err != nil {
		t.Fatalf("fetch changed items: %v", err)
	}
	if len(items) != len(ids) {
		t.Fatalf("expected %d items, got %d", len(ids), len(items))
	}
}

func TestBackfillFetchPlaybackEventsSincePaginatesAllResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/System/ActivityLog/Entries" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		start, _ := strconv.Atoi(r.URL.Query().Get("startIndex"))
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 {
			limit = 500
		}

		total := 5
		if start >= total {
			_ = json.NewEncoder(w).Encode(gen.ActivityLogEntryQueryResult{Items: &[]gen.ActivityLogEntry{}})
			return
		}
		end := start + limit
		if end > total {
			end = total
		}

		out := make([]gen.ActivityLogEntry, 0, end-start)
		for i := start; i < end; i++ {
			typ := "PlaybackStart"
			itemID := fmt.Sprintf("item-%d", i+1)
			name := fmt.Sprintf("Item %d", i+1)
			out = append(out, gen.ActivityLogEntry{Type: &typ, ItemId: &itemID, Name: &name})
		}
		_ = json.NewEncoder(w).Encode(gen.ActivityLogEntryQueryResult{Items: &out})
	}))
	defer server.Close()

	b, err := NewBackfillService(server.URL, "token", server.Client())
	if err != nil {
		t.Fatalf("new backfill service: %v", err)
	}

	events, err := b.FetchPlaybackEventsSince(context.Background(), time.Now().Add(-time.Hour), 2)
	if err != nil {
		t.Fatalf("fetch playback events: %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("expected 5 events, got %d", len(events))
	}
}
