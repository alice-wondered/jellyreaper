package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	stdhttp "net/http"

	"jellyreaper/internal/jellyfin"
)

type JellyfinWebhookHandler struct {
	handle func(context.Context, jellyfin.WebhookEvent) error
}

func NewJellyfinWebhookHandler(handle func(context.Context, jellyfin.WebhookEvent) error) *JellyfinWebhookHandler {
	return &JellyfinWebhookHandler{handle: handle}
}

func (h *JellyfinWebhookHandler) ServeHTTP(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if r.Method != stdhttp.MethodPost {
		w.Header().Set("Allow", stdhttp.MethodPost)
		stdhttp.Error(w, "method not allowed", stdhttp.StatusMethodNotAllowed)
		return
	}
	if h.handle == nil {
		stdhttp.Error(w, "jellyfin callback is not configured", stdhttp.StatusInternalServerError)
		return
	}

	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		stdhttp.Error(w, "failed to read body", stdhttp.StatusBadRequest)
		return
	}

	dec := json.NewDecoder(bytes.NewReader(body))

	var payload jellyfin.WebhookPayload
	if err := dec.Decode(&payload); err != nil {
		stdhttp.Error(w, fmt.Sprintf("invalid json body: %v", err), stdhttp.StatusBadRequest)
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		stdhttp.Error(w, "invalid json body: multiple json values", stdhttp.StatusBadRequest)
		return
	}

	raw := make(map[string]any)
	if err := json.Unmarshal(body, &raw); err != nil {
		stdhttp.Error(w, "invalid json payload", stdhttp.StatusBadRequest)
		return
	}

	event := jellyfin.BuildWebhookEvent(payload, raw)
	if err := h.handle(r.Context(), event); err != nil {
		stdhttp.Error(w, err.Error(), stdhttp.StatusInternalServerError)
		return
	}

	w.WriteHeader(stdhttp.StatusAccepted)
}
