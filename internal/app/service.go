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
	defaultExpireDays    = 30
	defaultHITLTimeoutH  = 48
	backfillReviewDelay  = 24 * time.Hour
	ingestProgressEvery  = 200
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
	eventAt := now
	if !event.OccurredAt.IsZero() {
		eventAt = event.OccurredAt.UTC()
	}
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
			OccurredAt:     eventAt,
			IdempotencyKey: dedupeKey,
			Payload:        event.Raw,
		}
		if err := tx.AppendEvent(ctx, recordedEvent); err != nil {
			return err
		}

		if itemID != "" {
			existing, found, err := tx.GetMedia(ctx, itemID)
			if err != nil {
				return err
			}
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
			if found {
				if media.Name == "" {
					media.Name = existing.Name
				}
				if media.Title == "" {
					media.Title = existing.Title
				}
				if media.ItemType == "" {
					media.ItemType = existing.ItemType
				}
				if media.SeasonID == "" {
					media.SeasonID = existing.SeasonID
				}
				if media.SeasonName == "" {
					media.SeasonName = existing.SeasonName
				}
				if media.SeriesID == "" {
					media.SeriesID = existing.SeriesID
				}
				if media.SeriesName == "" {
					media.SeriesName = existing.SeriesName
				}
				if media.ImageURL == "" {
					media.ImageURL = existing.ImageURL
				}
				media.LastPlayedAt = existing.LastPlayedAt
				media.PlayCountTotal = existing.PlayCountTotal
			}
			if isPlaybackEvent(event.EventType) {
				if eventAt.After(media.LastPlayedAt) {
					media.LastPlayedAt = eventAt
				}
				media.PlayCountTotal++
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

			runAt := now
			if playAt, known, err := mostRecentPlayForTarget(ctx, tx, flow); err != nil {
				return err
			} else if known {
				expireDays := flow.PolicySnapshot.ExpireAfterDays
				if expireDays <= 0 {
					expireDays = defaultExpireDays
				}
				dueAt := playAt.Add(time.Duration(expireDays) * 24 * time.Hour)
				if dueAt.After(runAt) {
					runAt = dueAt
				}
			}

			if flow.NextActionAt.Before(runAt) {
				expected := flow.Version
				flow.NextActionAt = runAt
				flow.UpdatedAt = now
				flow.Version = expected + 1
				if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
					return err
				}
			}

			if err := upsertEvaluatePolicyJob(ctx, tx, flow, now, runAt, "jellyfin_webhook", dedupeKey+":eval"); err != nil {
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
	decisionDisplayName := parsed.ItemID

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
		if strings.TrimSpace(flow.DisplayName) != "" {
			decisionDisplayName = strings.TrimSpace(flow.DisplayName)
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
		return interactionMessageUpdateResponse("Decision already recorded."), nil
	}
	if staleVersion {
		return interactionMessageUpdateResponse("This decision is stale. Please refresh and try again."), nil
	}

	s.wake(now)
	return interactionDecisionUpdateResponse(parsed.Action, decisionDisplayName), nil
}

func (s *Service) IngestBackfillPlayback(ctx context.Context, events []jellyfin.PlaybackEvent) error {
	total := len(events)
	for i, e := range events {
		if total > 0 && ((i+1)%ingestProgressEvery == 0 || i+1 == total) {
			s.logger.InfoContext(ctx, "backfill playback ingest progress", "processed", i+1, "total", total)
		}

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
			Raw:        map[string]any{"source": "backfill", "type": e.Type, "item_id": e.ItemID, "name": e.Name},
			ItemID:     e.ItemID,
			EventType:  e.Type,
			DedupeKey:  key,
			OccurredAt: e.Date,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) IngestBackfillItems(ctx context.Context, items []jellyfin.ItemSnapshot) error {
	now := s.now().UTC()
	total := len(items)
	for i, it := range items {
		if total > 0 && ((i+1)%ingestProgressEvery == 0 || i+1 == total) {
			s.logger.InfoContext(ctx, "backfill item ingest progress", "processed", i+1, "total", total)
		}

		key := "backfill:item:" + it.ItemID + ":" + strconv.Itoa(i)
		targets := deriveTargets(jellyfin.WebhookEvent{Payload: jellyfin.WebhookPayload{
			ItemID:          it.ItemID,
			ItemType:        it.ItemType,
			Name:            it.Name,
			SeasonID:        it.SeasonID,
			SeasonName:      it.SeasonName,
			SeriesID:        it.SeriesID,
			SeriesName:      it.SeriesName,
			PrimaryImageURL: it.ImageURL,
		}})

		err := s.repository.WithTx(ctx, func(tx repo.TxRepository) error {
			processed, err := tx.IsProcessed(ctx, key)
			if err != nil {
				return err
			}
			if processed {
				return nil
			}

			existingMedia, mediaFound, err := tx.GetMedia(ctx, it.ItemID)
			if err != nil {
				return err
			}

			media := domain.MediaItem{
				ItemID:         it.ItemID,
				Name:           it.Name,
				Title:          it.Name,
				ItemType:       it.ItemType,
				SeasonID:       it.SeasonID,
				SeasonName:     it.SeasonName,
				SeriesID:       it.SeriesID,
				SeriesName:     it.SeriesName,
				ImageURL:       it.ImageURL,
				LastPlayedAt:   it.LastPlayedAt,
				PlayCountTotal: int64(it.PlayCount),
				UpdatedAt:      now,
			}
			if mediaFound {
				if media.Name == "" {
					media.Name = existingMedia.Name
				}
				if media.Title == "" {
					media.Title = existingMedia.Title
				}
				if media.ItemType == "" {
					media.ItemType = existingMedia.ItemType
				}
				if media.SeasonID == "" {
					media.SeasonID = existingMedia.SeasonID
				}
				if media.SeasonName == "" {
					media.SeasonName = existingMedia.SeasonName
				}
				if media.SeriesID == "" {
					media.SeriesID = existingMedia.SeriesID
				}
				if media.SeriesName == "" {
					media.SeriesName = existingMedia.SeriesName
				}
				if media.ImageURL == "" {
					media.ImageURL = existingMedia.ImageURL
				}
				if existingMedia.LastPlayedAt.After(media.LastPlayedAt) {
					media.LastPlayedAt = existingMedia.LastPlayedAt
				}
				if existingMedia.PlayCountTotal > media.PlayCountTotal {
					media.PlayCountTotal = existingMedia.PlayCountTotal
				}
			}
			if err := tx.UpsertMedia(ctx, media); err != nil {
				return err
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
					expected := flow.Version
					flow.DisplayName = target.Name
					flow.ImageURL = target.ImageURL
					flow.SubjectType = target.Type
					flow.Version = expected + 1
					flow.UpdatedAt = now
					if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
						return err
					}
				}

				if flow.State != domain.FlowStateActive {
					continue
				}

				runAt := now.Add(backfillReviewDelay)
				if !it.LastPlayedAt.IsZero() {
					dueAt := it.LastPlayedAt.Add(time.Duration(defaultExpireDays) * 24 * time.Hour)
					if dueAt.After(runAt) {
						runAt = dueAt
					}
				}
				if flow.NextActionAt.After(runAt) {
					runAt = flow.NextActionAt
				}

				if !flow.NextActionAt.Equal(runAt) {
					expected := flow.Version
					flow.NextActionAt = runAt
					flow.UpdatedAt = now
					flow.Version = expected + 1
					if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
						return err
					}
				}

				if err := upsertEvaluatePolicyJob(ctx, tx, flow, now, runAt, "backfill_index", "backfill:item"); err != nil {
					return err
				}
			}

			return tx.MarkProcessed(ctx, key, now)
		})
		if err != nil {
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

func interactionMessageUpdateResponse(content string) *discordgo.InteractionResponse {
	return &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseUpdateMessage,
		Data: &discordgo.InteractionResponseData{
			Content:    content,
			Components: []discordgo.MessageComponent{},
		},
	}
}

func interactionDecisionUpdateResponse(action string, itemDisplay string) *discordgo.InteractionResponse {
	decision := strings.TrimSpace(action)
	if decision == "" {
		decision = "unknown"
	}
	item := strings.TrimSpace(itemDisplay)
	if item == "" {
		item = "item"
	}
	return interactionMessageUpdateResponse(fmt.Sprintf("Decision: %s for %s", strings.ToUpper(decision), item))
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
	allowFallback := true
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
		seasonLabel := formatSeasonLabel(p.SeasonName, p.SeriesName, p.Name)
		out = push(out, targetRef{Type: "season", ID: p.SeasonID, Name: seasonLabel, ImageURL: p.PrimaryImageURL})
		if p.SeasonID == "" {
			out = push(out, targetRef{Type: "item", ID: p.ItemID, Name: chooseName(p.Name, p.SeriesName), ImageURL: p.PrimaryImageURL})
		}
	case "season":
		seasonLabel := formatSeasonLabel(chooseName(p.Name, p.SeasonName), p.SeriesName, p.Name)
		out = push(out, targetRef{Type: "season", ID: p.ItemID, Name: seasonLabel, ImageURL: p.PrimaryImageURL})
	case "series":
		allowFallback = false
	case "movie":
		out = push(out, targetRef{Type: "movie", ID: p.ItemID, Name: p.Name, ImageURL: p.PrimaryImageURL})
	default:
		out = push(out, targetRef{Type: "item", ID: event.ItemID, Name: chooseName(p.Name, event.ItemID), ImageURL: p.PrimaryImageURL})
	}
	if allowFallback && len(out) == 0 && event.ItemID != "" {
		out = push(out, targetRef{Type: "item", ID: event.ItemID, Name: chooseName(p.Name, event.ItemID), ImageURL: p.PrimaryImageURL})
	}
	return out
}

func mostRecentPlayForTarget(ctx context.Context, tx repo.TxRepository, flow domain.Flow) (time.Time, bool, error) {
	parts := strings.SplitN(flow.ItemID, ":", 3)
	if len(parts) == 3 && parts[0] == "target" {
		items, err := tx.ListMediaBySubject(ctx, parts[1], parts[2])
		if err != nil {
			return time.Time{}, false, err
		}
		latest := time.Time{}
		for _, item := range items {
			if item.LastPlayedAt.After(latest) {
				latest = item.LastPlayedAt
			}
		}
		if latest.IsZero() {
			return time.Time{}, false, nil
		}
		return latest, true, nil
	}

	item, found, err := tx.GetMedia(ctx, flow.ItemID)
	if err != nil {
		return time.Time{}, false, err
	}
	if !found || item.LastPlayedAt.IsZero() {
		return time.Time{}, false, nil
	}
	return item.LastPlayedAt, true, nil
}

func upsertEvaluatePolicyJob(ctx context.Context, tx repo.TxRepository, flow domain.Flow, now time.Time, runAt time.Time, reason string, idempotencyPrefix string) error {
	payload, err := json.Marshal(jobs.EvaluatePolicyPayload{Reason: reason})
	if err != nil {
		return err
	}

	jobID := "job:eval:scheduled:" + flow.ItemID
	job, found, err := tx.GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	if found {
		job.FlowID = flow.FlowID
		job.ItemID = flow.ItemID
		job.Kind = domain.JobKindEvaluatePolicy
		job.Status = domain.JobStatusPending
		job.RunAt = runAt
		job.LeaseOwner = ""
		job.LeaseUntil = time.Time{}
		job.MaxAttempts = 5
		job.IdempotencyKey = idempotencyPrefix + ":" + flow.ItemID
		job.PayloadJSON = payload
		job.LastError = ""
		job.UpdatedAt = now
		if job.CreatedAt.IsZero() {
			job.CreatedAt = now
		}
		return tx.UpdateJob(ctx, job)
	}

	return tx.EnqueueJob(ctx, domain.JobRecord{
		JobID:          jobID,
		FlowID:         flow.FlowID,
		ItemID:         flow.ItemID,
		Kind:           domain.JobKindEvaluatePolicy,
		Status:         domain.JobStatusPending,
		RunAt:          runAt,
		MaxAttempts:    5,
		IdempotencyKey: idempotencyPrefix + ":" + flow.ItemID,
		PayloadJSON:    payload,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
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

func isPlaybackEvent(eventType string) bool {
	t := strings.ToLower(strings.TrimSpace(eventType))
	return strings.Contains(t, "playback") || strings.Contains(t, "played")
}

func formatSeasonLabel(seasonName string, seriesName string, fallback string) string {
	season := strings.TrimSpace(seasonName)
	series := strings.TrimSpace(seriesName)
	if season != "" && series != "" {
		return season + " of " + series
	}
	return chooseName(seasonName, seriesName, fallback)
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}
