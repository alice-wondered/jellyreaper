package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"

	"jellyreaper/internal/discord"
	"jellyreaper/internal/domain"
	"jellyreaper/internal/jellyfin"
	"jellyreaper/internal/jobs"
	"jellyreaper/internal/repo"
)

const (
	defaultDelayDuration = 24 * time.Hour
	defaultExpireDays    = 90
	defaultHITLTimeoutH  = 48
)

type targetRef struct {
	Type      string
	ID        string
	Name      string
	ImageURL  string
	Canonical string
}

type Service struct {
	repository repo.Repository
	logger     *slog.Logger
	wake       func(time.Time)
	now        func() time.Time
}

func NewService(repository repo.Repository, logger *slog.Logger, wake func(time.Time)) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	if wake == nil {
		wake = func(time.Time) {}
	}
	return &Service{repository: repository, logger: logger, wake: wake, now: time.Now}
}

func (s *Service) HandleJellyfinWebhook(ctx context.Context, event jellyfin.WebhookEvent) error {
	now := s.now().UTC()
	dedupeKey := event.DedupeKey
	itemID := event.ItemID
	targets := deriveTargets(event)

	err := s.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		processed, err := tx.IsProcessed(ctx, dedupeKey)
		if err != nil {
			return err
		}
		if processed {
			return nil
		}

		recordedEvent := domain.Event{
			EventID:        "evt:jellyfin:" + shortHash(dedupeKey),
			FlowID:         flowIDFromItem(itemID),
			ItemID:         itemID,
			Type:           jellyfinEventType(event.EventType),
			Source:         "jellyfin",
			OccurredAt:     now,
			IdempotencyKey: dedupeKey,
			Payload:        event.Raw,
		}
		if err := tx.AppendEvent(ctx, recordedEvent); err != nil {
			return err
		}

		if itemID != "" {
			media := domain.MediaItem{
				ItemID:     itemID,
				Name:       event.Payload.Name,
				Title:      event.Payload.Name,
				ItemType:   event.Payload.ItemType,
				SeasonID:   event.Payload.SeasonID,
				SeasonName: event.Payload.SeasonName,
				SeriesID:   event.Payload.SeriesID,
				SeriesName: event.Payload.SeriesName,
				ImageURL:   event.Payload.PrimaryImageURL,
				UpdatedAt:  now,
			}
			if err := tx.UpsertMedia(ctx, media); err != nil {
				return err
			}
		}

		for _, target := range targets {
			flow, found, err := tx.GetFlow(ctx, target.Canonical)
			if err != nil {
				return err
			}
			if !found {
				flow = domain.Flow{
					FlowID:      flowIDFromItem(target.Canonical),
					ItemID:      target.Canonical,
					SubjectType: target.Type,
					DisplayName: target.Name,
					ImageURL:    target.ImageURL,
					State:       domain.FlowStateActive,
					Version:     0,
					PolicySnapshot: domain.PolicySnapshot{
						ExpireAfterDays: defaultExpireDays,
						HITLTimeoutHrs:  defaultHITLTimeoutH,
						TimeoutAction:   "delete",
					},
					CreatedAt: now,
				}
				if err := tx.UpsertFlowCAS(ctx, flow, 0); err != nil {
					return err
				}
			} else {
				flow.DisplayName = target.Name
				flow.ImageURL = target.ImageURL
				flow.SubjectType = target.Type
				expected := flow.Version
				flow.Version = expected + 1
				flow.UpdatedAt = now
				if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
					return err
				}
			}

			if flow.State == domain.FlowStatePendingReview || flow.State == domain.FlowStateArchived || flow.State == domain.FlowStateDeleted {
				continue
			}

			payloadJSON, err := json.Marshal(jobs.EvaluatePolicyPayload{Reason: "jellyfin_webhook"})
			if err != nil {
				return err
			}
			if err := tx.EnqueueJob(ctx, domain.JobRecord{
				JobID:          "job:eval:" + target.Canonical + ":" + strconv.FormatInt(now.UnixNano(), 10),
				FlowID:         flowIDFromItem(target.Canonical),
				ItemID:         target.Canonical,
				Kind:           domain.JobKindEvaluatePolicy,
				Status:         domain.JobStatusPending,
				RunAt:          now,
				MaxAttempts:    5,
				IdempotencyKey: dedupeKey + ":eval:" + target.Canonical,
				PayloadJSON:    payloadJSON,
				CreatedAt:      now,
				UpdatedAt:      now,
			}); err != nil {
				return err
			}
		}

		return tx.MarkProcessed(ctx, dedupeKey, now)
	})
	if err != nil {
		return err
	}

	s.logger.InfoContext(ctx, "processed jellyfin webhook", "dedupe_key", dedupeKey, "item_id", itemID, "target_count", len(targets))
	s.wake(now)
	return nil
}

func (s *Service) HandleDiscordComponentInteraction(ctx context.Context, interaction discord.IncomingInteraction) (*discordgo.InteractionResponse, error) {
	if interaction.Raw == nil {
		return interactionResponse("Missing interaction data."), nil
	}

	if interaction.CustomID == "" {
		return interactionResponse("Missing interaction data."), nil
	}

	parsed, err := parseCustomID(interaction.CustomID)
	if err != nil {
		return interactionResponse("Invalid action payload."), nil
	}

	now := s.now().UTC()
	dedupeKey := discordDedupeKey(interaction)
	alreadyProcessed := false
	staleVersion := false

	err = s.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		processed, err := tx.IsProcessed(ctx, dedupeKey)
		if err != nil {
			return err
		}
		if processed {
			alreadyProcessed = true
			return nil
		}

		flow, found, err := tx.GetFlow(ctx, parsed.ItemID)
		if err != nil {
			return err
		}
		if !found {
			flow = domain.Flow{
				FlowID:      flowIDFromItem(parsed.ItemID),
				ItemID:      parsed.ItemID,
				SubjectType: inferSubjectType(parsed.ItemID),
				DisplayName: parsed.ItemID,
				State:       domain.FlowStateActive,
				Version:     0,
				PolicySnapshot: domain.PolicySnapshot{
					ExpireAfterDays: defaultExpireDays,
					HITLTimeoutHrs:  defaultHITLTimeoutH,
					TimeoutAction:   "delete",
				},
				CreatedAt: now,
			}
		}

		expectedVersion := flow.Version
		if parsed.Version != expectedVersion {
			staleVersion = true
			return nil
		}

		flow.UpdatedAt = now
		flow.HITLOutcome = parsed.Action

		switch parsed.Action {
		case "archive":
			flow.State = domain.FlowStateArchived
		case "keep":
			flow.State = domain.FlowStateActive
			if err := enqueueEvaluatePolicy(ctx, tx, flow, now, "discord_keep", now); err != nil {
				return err
			}
		case "delay":
			flow.State = domain.FlowStatePendingReview
			flow.NextActionAt = now.Add(defaultDelayDuration)
			if err := enqueueEvaluatePolicy(ctx, tx, flow, now, "discord_delay", flow.NextActionAt); err != nil {
				return err
			}
		case "delete":
			flow.State = domain.FlowStateDeleteQueued
			flow.NextActionAt = now
			if err := enqueueExecuteDelete(ctx, tx, flow, now); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported action %q", parsed.Action)
		}

		flow.Version = expectedVersion + 1
		if flow.CreatedAt.IsZero() {
			flow.CreatedAt = now
		}
		if err := tx.UpsertFlowCAS(ctx, flow, expectedVersion); err != nil {
			return err
		}

		event := domain.Event{
			EventID:        "evt:discord:" + shortHash(dedupeKey),
			FlowID:         flow.FlowID,
			ItemID:         flow.ItemID,
			Type:           "discord.interaction." + parsed.Action,
			Source:         "discord",
			OccurredAt:     now,
			IdempotencyKey: dedupeKey,
			Payload: map[string]any{
				"interaction_id": interaction.InteractionID,
				"custom_id":      interaction.CustomID,
				"action":         parsed.Action,
				"version":        parsed.Version,
			},
		}
		if err := tx.AppendEvent(ctx, event); err != nil {
			return err
		}

		return tx.MarkProcessed(ctx, dedupeKey, now)
	})
	if err != nil {
		return nil, err
	}

	if alreadyProcessed {
		return interactionResponse("This interaction was already processed."), nil
	}
	if staleVersion {
		return interactionResponse("This decision is stale. Please refresh and try again."), nil
	}

	s.wake(now)
	return interactionResponse(fmt.Sprintf("Applied action %q to %s.", parsed.Action, parsed.ItemID)), nil
}

func (s *Service) IngestBackfillPlayback(ctx context.Context, events []jellyfin.PlaybackEvent) error {
	for i, e := range events {
		key := "backfill:play:" + e.ItemID + ":" + e.Type + ":" + strconv.FormatInt(e.Date.Unix(), 10)
		if e.Date.IsZero() {
			key = "backfill:play:" + e.ItemID + ":" + e.Type + ":" + strconv.Itoa(i)
		}
		if err := s.HandleJellyfinWebhook(ctx, jellyfin.WebhookEvent{
			Payload: jellyfin.WebhookPayload{
				ItemID:           e.ItemID,
				Name:             e.Name,
				NotificationType: e.Type,
			},
			Raw:       map[string]any{"source": "backfill", "type": e.Type, "item_id": e.ItemID, "name": e.Name},
			ItemID:    e.ItemID,
			EventType: e.Type,
			DedupeKey: key,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) IngestBackfillItems(ctx context.Context, items []jellyfin.ItemSnapshot) error {
	for i, it := range items {
		key := "backfill:item:" + it.ItemID + ":" + strconv.Itoa(i)
		if err := s.HandleJellyfinWebhook(ctx, jellyfin.WebhookEvent{
			Payload: jellyfin.WebhookPayload{
				ItemID:           it.ItemID,
				ItemType:         it.ItemType,
				Name:             it.Name,
				SeasonID:         it.SeasonID,
				SeasonName:       it.SeasonName,
				SeriesID:         it.SeriesID,
				SeriesName:       it.SeriesName,
				PrimaryImageURL:  it.ImageURL,
				NotificationType: "ItemChanged",
			},
			Raw:       map[string]any{"source": "backfill", "item_id": it.ItemID, "item_type": it.ItemType, "name": it.Name},
			ItemID:    it.ItemID,
			EventType: "ItemChanged",
			DedupeKey: key,
		}); err != nil {
			return err
		}
	}
	return nil
}

type customID struct {
	Action  string
	ItemID  string
	Version int64
}

func parseCustomID(value string) (customID, error) {
	if !strings.HasPrefix(value, "jr:v1:") {
		return customID{}, fmt.Errorf("invalid custom id")
	}
	remainder := strings.TrimPrefix(value, "jr:v1:")
	firstSep := strings.Index(remainder, ":")
	lastSep := strings.LastIndex(remainder, ":")
	if firstSep <= 0 || lastSep <= firstSep {
		return customID{}, fmt.Errorf("invalid custom id")
	}
	action := remainder[:firstSep]
	itemID := remainder[firstSep+1 : lastSep]
	versionPart := remainder[lastSep+1:]

	switch action {
	case "archive", "keep", "delay", "delete":
	default:
		return customID{}, fmt.Errorf("unsupported action %q", action)
	}

	if itemID == "" {
		return customID{}, fmt.Errorf("item id is required")
	}

	version, err := strconv.ParseInt(versionPart, 10, 64)
	if err != nil {
		return customID{}, fmt.Errorf("invalid version")
	}

	return customID{Action: action, ItemID: itemID, Version: version}, nil
}

func enqueueEvaluatePolicy(ctx context.Context, tx repo.TxRepository, flow domain.Flow, now time.Time, reason string, runAt time.Time) error {
	payload, err := json.Marshal(jobs.EvaluatePolicyPayload{Reason: reason})
	if err != nil {
		return err
	}

	return tx.EnqueueJob(ctx, domain.JobRecord{
		JobID:          "job:eval:" + flow.ItemID + ":" + strconv.FormatInt(now.UnixNano(), 10),
		FlowID:         flow.FlowID,
		ItemID:         flow.ItemID,
		Kind:           domain.JobKindEvaluatePolicy,
		Status:         domain.JobStatusPending,
		RunAt:          runAt,
		MaxAttempts:    5,
		IdempotencyKey: "discord:eval:" + flow.ItemID + ":" + strconv.FormatInt(flow.Version+1, 10),
		PayloadJSON:    payload,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
}

func enqueueExecuteDelete(ctx context.Context, tx repo.TxRepository, flow domain.Flow, now time.Time) error {
	payload, err := json.Marshal(jobs.ExecuteDeletePayload{RequestedBy: "discord"})
	if err != nil {
		return err
	}

	return tx.EnqueueJob(ctx, domain.JobRecord{
		JobID:          "job:delete:" + flow.ItemID + ":" + strconv.FormatInt(now.UnixNano(), 10),
		FlowID:         flow.FlowID,
		ItemID:         flow.ItemID,
		Kind:           domain.JobKindExecuteDelete,
		Status:         domain.JobStatusPending,
		RunAt:          now,
		MaxAttempts:    5,
		IdempotencyKey: "discord:delete:" + flow.ItemID + ":" + strconv.FormatInt(flow.Version+1, 10),
		PayloadJSON:    payload,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
}

func interactionResponse(content string) *discordgo.InteractionResponse {
	return &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{Content: content},
	}
}

func jellyfinEventType(t string) string {
	t = strings.TrimSpace(t)
	if t == "" {
		return "jellyfin.webhook.received"
	}
	return "jellyfin." + t
}

func discordDedupeKey(interaction discord.IncomingInteraction) string {
	if interaction.Raw == nil {
		return "discord:missing"
	}
	if interaction.InteractionID != "" {
		return "discord:" + interaction.InteractionID
	}
	return "discord:fallback:" + shortHash(interaction.Token)
}

func flowIDFromItem(itemID string) string {
	if itemID == "" {
		return ""
	}
	return "flow:" + itemID
}

func deriveTargets(event jellyfin.WebhookEvent) []targetRef {
	p := event.Payload
	push := func(out []targetRef, t targetRef) []targetRef {
		if t.ID == "" {
			return out
		}
		t.Canonical = targetKey(t.Type, t.ID)
		for _, existing := range out {
			if existing.Canonical == t.Canonical {
				return out
			}
		}
		return append(out, t)
	}

	var out []targetRef
	typeLower := strings.ToLower(strings.TrimSpace(p.ItemType))
	switch typeLower {
	case "episode":
		out = push(out, targetRef{Type: "season", ID: p.SeasonID, Name: chooseName(p.SeasonName, p.Name), ImageURL: p.PrimaryImageURL})
		out = push(out, targetRef{Type: "series", ID: p.SeriesID, Name: chooseName(p.SeriesName, p.SeasonName), ImageURL: p.PrimaryImageURL})
	case "season":
		out = push(out, targetRef{Type: "season", ID: p.ItemID, Name: chooseName(p.Name, p.SeasonName), ImageURL: p.PrimaryImageURL})
		out = push(out, targetRef{Type: "series", ID: p.SeriesID, Name: chooseName(p.SeriesName, p.Name), ImageURL: p.PrimaryImageURL})
	case "series":
		out = push(out, targetRef{Type: "series", ID: p.ItemID, Name: chooseName(p.Name, p.SeriesName), ImageURL: p.PrimaryImageURL})
	case "movie":
		out = push(out, targetRef{Type: "movie", ID: p.ItemID, Name: p.Name, ImageURL: p.PrimaryImageURL})
	default:
		out = push(out, targetRef{Type: "item", ID: event.ItemID, Name: chooseName(p.Name, event.ItemID), ImageURL: p.PrimaryImageURL})
	}
	if len(out) == 0 && event.ItemID != "" {
		out = push(out, targetRef{Type: "item", ID: event.ItemID, Name: chooseName(p.Name, event.ItemID), ImageURL: p.PrimaryImageURL})
	}
	return out
}

func targetKey(targetType, id string) string {
	return "target:" + targetType + ":" + strings.TrimSpace(id)
}

func inferSubjectType(canonical string) string {
	parts := strings.SplitN(canonical, ":", 3)
	if len(parts) == 3 && parts[0] == "target" {
		return parts[1]
	}
	return "item"
}

func chooseName(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return "Unknown"
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}
