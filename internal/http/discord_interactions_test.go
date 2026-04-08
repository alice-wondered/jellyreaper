package http

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bwmarrin/discordgo"

	"jellyreaper/internal/discord"
)

func TestDiscordInteractionsHandlerPing(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	dsvc, err := discord.NewService("", pub)
	if err != nil {
		t.Fatalf("new discord service: %v", err)
	}
	h := NewDiscordInteractionsHandler(dsvc, func(context.Context, discord.IncomingInteraction) (*discordgo.InteractionResponse, error) {
		return nil, nil
	})

	body := []byte(`{"type":1}`)
	req := httptest.NewRequest(http.MethodPost, "/discord/interactions", bytes.NewReader(body))
	signDiscordRequest(req, priv, body)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rr.Code)
	}
}

func TestDiscordInteractionsHandlerComponentDispatch(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	dsvc, _ := discord.NewService("", pub)

	called := false
	h := NewDiscordInteractionsHandler(dsvc, func(_ context.Context, i discord.IncomingInteraction) (*discordgo.InteractionResponse, error) {
		called = true
		if i.CustomID != "jr:v1:archive:item1:0" {
			t.Fatalf("unexpected custom id")
		}
		return &discordgo.InteractionResponse{Type: discordgo.InteractionResponseChannelMessageWithSource}, nil
	})

	body := []byte(`{"id":"1","token":"tok","type":3,"data":{"custom_id":"jr:v1:archive:item1:0"}}`)
	req := httptest.NewRequest(http.MethodPost, "/discord/interactions", bytes.NewReader(body))
	signDiscordRequest(req, priv, body)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rr.Code)
	}
	if !called {
		t.Fatal("expected callback to be called")
	}
}

func TestDiscordInteractionsHandlerRejectsBadSignature(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(nil)
	dsvc, _ := discord.NewService("", pub)
	h := NewDiscordInteractionsHandler(dsvc, func(context.Context, discord.IncomingInteraction) (*discordgo.InteractionResponse, error) {
		return &discordgo.InteractionResponse{Type: discordgo.InteractionResponsePong}, nil
	})

	body := []byte(`{"type":1}`)
	req := httptest.NewRequest(http.MethodPost, "/discord/interactions", bytes.NewReader(body))
	req.Header.Set("X-Signature-Timestamp", "1")
	req.Header.Set("X-Signature-Ed25519", "deadbeef")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", rr.Code)
	}
}

func TestDiscordInteractionsHandlerRejectsInvalidPayloadTypes(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	dsvc, _ := discord.NewService("", pub)
	h := NewDiscordInteractionsHandler(dsvc, func(context.Context, discord.IncomingInteraction) (*discordgo.InteractionResponse, error) {
		return &discordgo.InteractionResponse{Type: discordgo.InteractionResponsePong}, nil
	})

	body := []byte(`{"id":"1","token":"tok","type":3,"data":{"custom_id":123}}`)
	req := httptest.NewRequest(http.MethodPost, "/discord/interactions", bytes.NewReader(body))
	signDiscordRequest(req, priv, body)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", rr.Code)
	}
}

func signDiscordRequest(req *http.Request, priv ed25519.PrivateKey, body []byte) {
	timestamp := "1700000000"
	msg := append([]byte(timestamp), body...)
	sig := ed25519.Sign(priv, msg)
	req.Header.Set("X-Signature-Timestamp", timestamp)
	req.Header.Set("X-Signature-Ed25519", hex.EncodeToString(sig))
}
