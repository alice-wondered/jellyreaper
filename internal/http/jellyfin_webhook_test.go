package http

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"jellyreaper/internal/jellyfin"
)

func TestJellyfinWebhookHandlerAccepted(t *testing.T) {
	called := false
	h := NewJellyfinWebhookHandler(func(_ context.Context, event jellyfin.WebhookEvent) error {
		called = true
		if event.ItemID != "item1" {
			t.Fatalf("unexpected item id: %s", event.ItemID)
		}
		if event.Payload.ItemID != "item1" {
			t.Fatalf("unexpected payload item id: %s", event.Payload.ItemID)
		}
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/webhooks/jellyfin", bytes.NewBufferString(`{"ItemId":"item1","EventId":"evt1"}`))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("unexpected status: %d", rr.Code)
	}
	if !called {
		t.Fatal("expected callback to be called")
	}
}

func TestJellyfinWebhookHandlerRejectsInvalidJSON(t *testing.T) {
	h := NewJellyfinWebhookHandler(func(context.Context, jellyfin.WebhookEvent) error { return nil })

	req := httptest.NewRequest(http.MethodPost, "/webhooks/jellyfin", bytes.NewBufferString(`{"ItemId":`))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", rr.Code)
	}
}

func TestJellyfinWebhookHandlerRejectsMultipleJSONValues(t *testing.T) {
	h := NewJellyfinWebhookHandler(func(context.Context, jellyfin.WebhookEvent) error { return nil })

	req := httptest.NewRequest(http.MethodPost, "/webhooks/jellyfin", bytes.NewBufferString(`{} {}`))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", rr.Code)
	}
}

func TestJellyfinWebhookHandlerHandlesUnexpectedFieldType(t *testing.T) {
	called := false
	h := NewJellyfinWebhookHandler(func(_ context.Context, event jellyfin.WebhookEvent) error {
		called = true
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/webhooks/jellyfin", bytes.NewBufferString(`{"ItemId":123,"EventId":"evt1"}`))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", rr.Code)
	}
	if called {
		t.Fatal("callback should not be called for invalid provider payload type")
	}
}
