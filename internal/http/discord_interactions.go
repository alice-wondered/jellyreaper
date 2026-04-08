package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

	if !h.service.VerifyRequest(r) {
		stdhttp.Error(w, "invalid request signature", stdhttp.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		stdhttp.Error(w, "failed to read request body", stdhttp.StatusBadRequest)
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
