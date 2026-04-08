package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	stdhttp "net/http"

	"github.com/bwmarrin/discordgo"

	"jellyreaper/internal/discord"
)

type DiscordInteractionsHandler struct {
	service *discord.Service
	handle  func(context.Context, discord.IncomingInteraction) (*discordgo.InteractionResponse, error)
}

func NewDiscordInteractionsHandler(service *discord.Service, handle func(context.Context, discord.IncomingInteraction) (*discordgo.InteractionResponse, error)) *DiscordInteractionsHandler {
	return &DiscordInteractionsHandler{service: service, handle: handle}
}

func (h *DiscordInteractionsHandler) ServeHTTP(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if r.Method != stdhttp.MethodPost {
		w.Header().Set("Allow", stdhttp.MethodPost)
		stdhttp.Error(w, "method not allowed", stdhttp.StatusMethodNotAllowed)
		return
	}
	if h.service == nil {
		stdhttp.Error(w, "discord service is not configured", stdhttp.StatusInternalServerError)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		stdhttp.Error(w, "failed to read request body", stdhttp.StatusBadRequest)
		return
	}

	sig := r.Header.Get("X-Signature-Ed25519")
	ts := r.Header.Get("X-Signature-Timestamp")
	if ok, reason := h.service.VerifyInteractionPayload(sig, ts, body); !ok {
		slog.Warn("discord interaction signature verification failed",
			"reason", reason,
			"has_signature_header", sig != "",
			"has_timestamp_header", ts != "",
			"body_len", len(body),
		)
		stdhttp.Error(w, "invalid request signature", stdhttp.StatusUnauthorized)
		return
	}

	incoming, err := discord.ParseIncomingInteraction(body)
	if err != nil {
		stdhttp.Error(w, fmt.Sprintf("invalid json body: %v", err), stdhttp.StatusBadRequest)
		return
	}

	response, status, err := h.service.HandleIncomingInteraction(r.Context(), incoming, h.handle)
	if err != nil {
		stdhttp.Error(w, err.Error(), status)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response)
}
