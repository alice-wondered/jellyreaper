package discord

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	editHook  func(context.Context, string, string, string) error

	httpClient      *http.Client
	imagePersistDir string
	jellyfinBaseURL string
	jellyfinAPIKey  string
}

func NewService(botToken string, publicKey ed25519.PublicKey) (*Service, error) {
	service := &Service{publicKey: publicKey}
	service.httpClient = &http.Client{Timeout: 15 * time.Second}
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

func (s *Service) VerifyInteractionPayload(signatureHex string, timestamp string, body []byte) (bool, string) {
	if len(s.publicKey) != ed25519.PublicKeySize {
		return false, "public key missing or invalid length"
	}
	if strings.TrimSpace(signatureHex) == "" {
		return false, "missing signature header"
	}
	if strings.TrimSpace(timestamp) == "" {
		return false, "missing timestamp header"
	}

	sig, err := hex.DecodeString(strings.TrimSpace(signatureHex))
	if err != nil {
		return false, "signature header is not valid hex"
	}
	if len(sig) != ed25519.SignatureSize {
		return false, "signature has invalid length"
	}

	message := make([]byte, 0, len(timestamp)+len(body))
	message = append(message, []byte(strings.TrimSpace(timestamp))...)
	message = append(message, body...)
	if !ed25519.Verify(s.publicKey, message, sig) {
		return false, "signature mismatch"
	}

	return true, ""
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

	if file, err := s.prepareEmbedFile(ctx, itemID, imageURL); err == nil && file != nil {
		send.Files = []*discordgo.File{file}
		send.Embeds = []*discordgo.MessageEmbed{{Image: &discordgo.MessageEmbedImage{URL: "attachment://" + file.Name}}}
	} else if normalized := normalizeHTTPURL(imageURL); normalized != "" {
		send.Embeds = []*discordgo.MessageEmbed{{
			Image: &discordgo.MessageEmbedImage{URL: normalized},
		}}
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

func (s *Service) SetEditPromptHookForTest(hook func(context.Context, string, string, string) error) {
	s.editHook = hook
}

func (s *Service) FinalizeHITLPrompt(ctx context.Context, channelID, messageID, content string) error {
	if strings.TrimSpace(channelID) == "" || strings.TrimSpace(messageID) == "" {
		return nil
	}
	if s.editHook != nil {
		return s.editHook(ctx, channelID, messageID, content)
	}
	if s.session == nil {
		return fmt.Errorf("discord bot token is not configured")
	}

	trimmed := strings.TrimSpace(content)
	emptyComponents := []discordgo.MessageComponent{}
	edit := &discordgo.MessageEdit{ID: messageID, Channel: channelID, Content: &trimmed, Components: &emptyComponents}
	if _, err := s.session.ChannelMessageEditComplex(edit); err != nil {
		return fmt.Errorf("finalize hitl prompt: %w", err)
	}
	return nil
}

func (s *Service) SetEmbedPersistenceDir(dir string) {
	s.imagePersistDir = strings.TrimSpace(dir)
}

func (s *Service) SetJellyfinImageSource(baseURL string, apiKey string) {
	s.jellyfinBaseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	s.jellyfinAPIKey = strings.TrimSpace(apiKey)
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

func customID(action, itemID string, version int64) string {
	return "jr:v1:" + action + ":" + itemID + ":" + strconv.FormatInt(version, 10)
}

func (s *Service) prepareEmbedFile(ctx context.Context, itemID string, imageURL string) (*discordgo.File, error) {
	resolved := s.resolveImageURL(itemID, imageURL)
	if resolved == "" {
		return nil, fmt.Errorf("no image URL available")
	}

	data, ext, err := s.fetchImage(ctx, resolved)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("empty image payload")
	}

	name := "cover" + ext
	if err := s.persistImage(resolved, data, ext); err != nil {
		// best effort only
	}

	return &discordgo.File{
		Name:        name,
		ContentType: http.DetectContentType(data),
		Reader:      bytes.NewReader(data),
	}, nil
}

func (s *Service) resolveImageURL(itemID string, imageURL string) string {
	if normalized := normalizeHTTPURL(imageURL); normalized != "" {
		return normalized
	}
	if s.jellyfinBaseURL == "" || itemID == "" || strings.Contains(itemID, ":") {
		return ""
	}
	return s.jellyfinBaseURL + "/Items/" + url.PathEscape(itemID) + "/Images/Primary"
}

func normalizeHTTPURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return ""
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return ""
	}
	if parsed.Host == "" {
		return ""
	}
	return parsed.String()
}

func (s *Service) fetchImage(ctx context.Context, imageURL string) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, imageURL, nil)
	if err != nil {
		return nil, "", err
	}
	if s.jellyfinAPIKey != "" {
		req.Header.Set("X-Emby-Token", s.jellyfinAPIKey)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("image fetch status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, "", err
	}
	ctype := strings.ToLower(resp.Header.Get("Content-Type"))
	ext := ".img"
	switch {
	case strings.Contains(ctype, "jpeg") || strings.Contains(ctype, "jpg"):
		ext = ".jpg"
	case strings.Contains(ctype, "png"):
		ext = ".png"
	case strings.Contains(ctype, "webp"):
		ext = ".webp"
	}
	return data, ext, nil
}

func (s *Service) persistImage(imageURL string, data []byte, ext string) error {
	if s.imagePersistDir == "" {
		return nil
	}
	if err := os.MkdirAll(s.imagePersistDir, 0o755); err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(imageURL))
	name := hex.EncodeToString(sum[:8]) + ext
	return os.WriteFile(filepath.Join(s.imagePersistDir, name), data, 0o644)
}
