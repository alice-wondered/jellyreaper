package jellyfin

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"jellyreaper/internal/domain"
)

type WebhookPayload struct {
	EventID            string    `json:"EventId,omitempty"`
	NotificationID     string    `json:"NotificationId,omitempty"`
	NotificationType   string    `json:"NotificationType,omitempty"`
	Event              string    `json:"Event,omitempty"`
	ItemID             string    `json:"ItemId,omitempty"`
	ItemType           string    `json:"ItemType,omitempty"`
	SeasonID           string    `json:"SeasonId,omitempty"`
	SeasonName         string    `json:"SeasonName,omitempty"`
	SeriesID           string    `json:"SeriesId,omitempty"`
	SeriesName         string    `json:"SeriesName,omitempty"`
	ServerURL          string    `json:"ServerUrl,omitempty"`
	PrimaryImageTag    string    `json:"PrimaryImageTag,omitempty"`
	PrimaryImageURL    string    `json:"PrimaryImageUrl,omitempty"`
	UserID             string    `json:"UserId,omitempty"`
	ServerID           string    `json:"ServerId,omitempty"`
	Name               string    `json:"Name,omitempty"`
	DateCreated        time.Time `json:"DateCreated,omitempty"`
	DateLastMediaAdded time.Time `json:"DateLastMediaAdded,omitempty"`
	LastPlayedAt       time.Time `json:"LastPlayedAt,omitempty"`
	PlayCountTotal     int64     `json:"PlayCountTotal,omitempty"`
}

type WebhookEvent struct {
	Payload    WebhookPayload
	Raw        map[string]any
	ItemID     string
	EventID    string
	EventType  string
	DedupeKey  string
	OccurredAt time.Time
}

func BuildWebhookEvent(payload WebhookPayload, raw map[string]any) WebhookEvent {
	payload.ItemID = domain.NormalizeID(payload.ItemID)
	payload.SeasonID = domain.NormalizeID(payload.SeasonID)
	payload.SeriesID = domain.NormalizeID(payload.SeriesID)
	payload.UserID = domain.NormalizeID(payload.UserID)
	payload.PrimaryImageURL = derivePrimaryImageURL(payload)

	eventID := payload.EventID
	if eventID == "" {
		eventID = payload.NotificationID
	}

	eventType := strings.TrimSpace(payload.NotificationType)
	if eventType == "" {
		eventType = strings.TrimSpace(payload.Event)
	}

	dedupeKey := "jellyfin:payload:" + shortHashMap(raw)
	if eventID != "" {
		dedupeKey = "jellyfin:" + eventID
	}

	return WebhookEvent{
		Payload:   payload,
		Raw:       raw,
		ItemID:    domain.NormalizeID(payload.ItemID),
		EventID:   strings.TrimSpace(eventID),
		EventType: eventType,
		DedupeKey: dedupeKey,
	}
}

func derivePrimaryImageURL(payload WebhookPayload) string {
	if strings.TrimSpace(payload.PrimaryImageURL) != "" {
		return strings.TrimSpace(payload.PrimaryImageURL)
	}
	base := strings.TrimRight(strings.TrimSpace(payload.ServerURL), "/")
	itemID := strings.TrimSpace(payload.ItemID)
	if base == "" || itemID == "" {
		return ""
	}
	path := "/Items/" + url.PathEscape(itemID) + "/Images/Primary"
	if tag := strings.TrimSpace(payload.PrimaryImageTag); tag != "" {
		return base + path + "?tag=" + url.QueryEscape(tag)
	}
	return base + path
}

func shortHashMap(m map[string]any) string {
	payload, _ := json.Marshal(m)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:8])
}
