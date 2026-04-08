package http

import (
	"context"
	"errors"
	stdhttp "net/http"

	"github.com/bwmarrin/discordgo"

	"jellyreaper/internal/discord"
	"jellyreaper/internal/jellyfin"
)

type Config struct {
	Addr                     string
	Jellyfin                 func(context.Context, jellyfin.WebhookEvent) error
	JellyfinWebhookToken     string
	Discord                  *discord.Service
	HandleDiscordInteraction func(context.Context, discord.IncomingInteraction) (*discordgo.InteractionResponse, error)
}

func NewMux(cfg Config) (stdhttp.Handler, error) {
	if cfg.Jellyfin == nil {
		return nil, errors.New("http: jellyfin callback is required")
	}
	if cfg.Discord == nil {
		return nil, errors.New("http: discord service is required")
	}
	if cfg.HandleDiscordInteraction == nil {
		return nil, errors.New("http: discord interaction handler is required")
	}

	mux := stdhttp.NewServeMux()
	mux.HandleFunc("/healthz", healthzHandler)
	mux.Handle("/webhooks/jellyfin", NewJellyfinWebhookHandler(cfg.Jellyfin, cfg.JellyfinWebhookToken))
	mux.Handle("/discord/interactions", NewDiscordInteractionsHandler(cfg.Discord, cfg.HandleDiscordInteraction))
	return mux, nil
}

func NewServer(cfg Config) (*stdhttp.Server, error) {
	h, err := NewMux(cfg)
	if err != nil {
		return nil, err
	}
	return &stdhttp.Server{
		Addr:    cfg.Addr,
		Handler: h,
	}, nil
}

func healthzHandler(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if r.Method != stdhttp.MethodGet {
		w.Header().Set("Allow", stdhttp.MethodGet)
		stdhttp.Error(w, "method not allowed", stdhttp.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(stdhttp.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
