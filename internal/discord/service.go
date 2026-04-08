package discord

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
)

type InteractionCallback func(context.Context, IncomingInteraction) (*discordgo.InteractionResponse, error)

type Service struct {
	publicKey ed25519.PublicKey
	session   *discordgo.Session
	sendHook  func(context.Context, string, string, int64, string, string) (string, error)
	embedDir  string
}

func NewService(botToken string, publicKey ed25519.PublicKey) (*Service, error) {
	service := &Service{publicKey: publicKey}
	if botToken == "" {
		return service, nil
	}

	session, err := discordgo.New("Bot " + botToken)
	if err != nil {
		return nil, fmt.Errorf("create discord session: %w", err)
	}
	session.Identify.Intents = 0
	service.session = session

	return service, nil
}

func (s *Service) VerifyRequest(r *http.Request) bool {
	if len(s.publicKey) != ed25519.PublicKeySize {
		return false
	}
	return discordgo.VerifyInteraction(r, s.publicKey)
}

func (s *Service) HandleIncomingInteraction(ctx context.Context, interaction IncomingInteraction, handle InteractionCallback) (*discordgo.InteractionResponse, int, error) {
	switch interaction.Type {
	case discordgo.InteractionPing:
		return &discordgo.InteractionResponse{Type: discordgo.InteractionResponsePong}, http.StatusOK, nil
	case discordgo.InteractionMessageComponent:
		if handle == nil {
			return nil, http.StatusInternalServerError, fmt.Errorf("discord interaction callback is not configured")
		}
		resp, err := handle(ctx, interaction)
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}
		return resp, http.StatusOK, nil
	default:
		return nil, http.StatusBadRequest, fmt.Errorf("unsupported interaction type: %d", interaction.Type)
	}
}

func (s *Service) SendHITLPrompt(ctx context.Context, channelID, itemID string, version int64, displayName string, imageURL string) (string, error) {
	if s.sendHook != nil {
		return s.sendHook(ctx, channelID, itemID, version, displayName, imageURL)
	}

	if s.session == nil {
		return "", fmt.Errorf("discord bot token is not configured")
	}
	if channelID == "" {
		return "", fmt.Errorf("discord channel id is required")
	}

	contentName := displayName
	if contentName == "" {
		contentName = itemID
	}

	send := &discordgo.MessageSend{
		Content: fmt.Sprintf("Review needed: **%s**\nChoose an action for this media group.", contentName),
		Components: []discordgo.MessageComponent{
			discordgo.ActionsRow{Components: []discordgo.MessageComponent{
				discordgo.Button{Label: "Archive", Style: discordgo.SecondaryButton, CustomID: customID("archive", itemID, version)},
				discordgo.Button{Label: "Delay", Style: discordgo.PrimaryButton, CustomID: customID("delay", itemID, version)},
				discordgo.Button{Label: "Keep", Style: discordgo.SuccessButton, CustomID: customID("keep", itemID, version)},
				discordgo.Button{Label: "Delete", Style: discordgo.DangerButton, CustomID: customID("delete", itemID, version)},
			}},
		},
	}
	if imageURL != "" {
		s.persistEmbed(ctx, imageURL)
		send.Embeds = []*discordgo.MessageEmbed{{Image: &discordgo.MessageEmbedImage{URL: imageURL}}}
	}

	msg, err := s.session.ChannelMessageSendComplex(channelID, send)
	if err != nil {
		return "", fmt.Errorf("send hitl prompt: %w", err)
	}
	return msg.ID, nil
}

func (s *Service) SetSendPromptHookForTest(hook func(context.Context, string, string, int64, string, string) (string, error)) {
	s.sendHook = hook
}

func (s *Service) SendSystemMessage(channelID, content string) error {
	if strings.TrimSpace(channelID) == "" || strings.TrimSpace(content) == "" {
		return nil
	}
	if s.session == nil {
		return fmt.Errorf("discord bot token is not configured")
	}
	_, err := s.session.ChannelMessageSend(channelID, content)
	if err != nil {
		return fmt.Errorf("send system message: %w", err)
	}
	return nil
}

func (s *Service) SetEmbedPersistenceDir(dir string) {
	s.embedDir = strings.TrimSpace(dir)
}

func (s *Service) persistEmbed(ctx context.Context, imageURL string) {
	if s.embedDir == "" || imageURL == "" {
		return
	}
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, imageURL, nil)
	if err != nil {
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil || len(data) == 0 {
		return
	}
	_ = os.MkdirAll(s.embedDir, 0o755)
	sum := sha256.Sum256([]byte(imageURL))
	name := hex.EncodeToString(sum[:8]) + ".img"
	_ = os.WriteFile(filepath.Join(s.embedDir, name), data, 0o644)
}

func customID(action, itemID string, version int64) string {
	return "jr:v1:" + action + ":" + itemID + ":" + strconv.FormatInt(version, 10)
}
