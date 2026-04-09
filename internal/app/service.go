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
	defaultDelayDuration         = 24 * time.Hour
	defaultExpireDays            = 30
	defaultHITLTimeoutH          = 48
	metaReviewDays               = "settings.review_days"
	metaDeferDays                = "settings.defer_days"
	metaHITLTimeoutHours         = "settings.hitl_timeout_hours"
	backfillReviewDelay          = 24 * time.Hour
	ingestProgressEvery          = 200
	defaultBackfillBatchSize     = 100
	defaultBackfillBatchTimeout  = 500 * time.Millisecond
	defaultBackfillQueueCapacity = 2000
)

type targetRef struct {
	Type      string
	ID        string
	Name      string
	ImageURL  string
	Canonical string
}

type hitlFinalizeRequest struct {
	channelID string
	messageID string
	content   string
}

type arrRemovalRequest struct {
	service     string
	providerIDs map[string]string
	itemID      string
}

type radarrRemover interface {
	RemoveByProviderIDs(context.Context, map[string]string) error
}

type sonarrRemover interface {
	RemoveByProviderIDs(context.Context, map[string]string) error
}

type Service struct {
	repository            repo.Repository
	logger                *slog.Logger
	discord               *discord.Service
	wake                  func(time.Time)
	now                   func() time.Time
	defaultDelayWindow    time.Duration
	defaultExpireDays     int
	defaultHITLTimeoutHrs int
	jellyfinClient        *jellyfin.Client
	radarr                radarrRemover
	sonarr                sonarrRemover
	backfillBatchSize     int
	backfillBatchTimeout  time.Duration
	backfillQueueCapacity int
}

func NewService(repository repo.Repository, logger *slog.Logger, wake func(time.Time)) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	if wake == nil {
		wake = func(time.Time) {}
	}
	return &Service{
		repository:            repository,
		logger:                logger,
		wake:                  wake,
		now:                   time.Now,
		defaultDelayWindow:    defaultDelayDuration,
		defaultExpireDays:     defaultExpireDays,
		defaultHITLTimeoutHrs: defaultHITLTimeoutH,
		backfillBatchSize:     defaultBackfillBatchSize,
		backfillBatchTimeout:  defaultBackfillBatchTimeout,
		backfillQueueCapacity: defaultBackfillQueueCapacity,
	}
}

func (s *Service) SetPolicyDefaults(defaultExpireDays int, defaultDelayWindow time.Duration) {
	if defaultExpireDays > 0 {
		s.defaultExpireDays = defaultExpireDays
	}
	if defaultDelayWindow > 0 {
		s.defaultDelayWindow = defaultDelayWindow
	}
}

func (s *Service) SetDefaultHITLTimeoutHours(hours int) {
	if hours > 0 {
		s.defaultHITLTimeoutHrs = hours
	}
}

func (s *Service) SetGlobalReviewDays(ctx context.Context, days int) error {
	if days <= 0 || days > 3650 {
		return fmt.Errorf("review days must be between 1 and 3650")
	}
	return s.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		return tx.SetMeta(ctx, metaReviewDays, strconv.Itoa(days))
	})
}

func (s *Service) SetGlobalDeferDays(ctx context.Context, days int) error {
	if days <= 0 || days > 3650 {
		return fmt.Errorf("defer days must be between 1 and 3650")
	}
	return s.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		return tx.SetMeta(ctx, metaDeferDays, strconv.Itoa(days))
	})
}

func (s *Service) SetGlobalHITLTimeoutHours(ctx context.Context, hours int) error {
	if hours <= 0 || hours > 8760 {
		return fmt.Errorf("hitl timeout hours must be between 1 and 8760")
	}
	return s.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		return tx.SetMeta(ctx, metaHITLTimeoutHours, strconv.Itoa(hours))
	})
}

func (s *Service) currentDefaultsFromMeta(ctx context.Context, tx repo.TxRepository) (int, time.Duration, int, error) {
	reviewDays := s.defaultExpireDays
	deferWindow := s.defaultDelayWindow
	hitlTimeoutHours := s.defaultHITLTimeoutHrs

	if raw, ok, err := tx.GetMeta(ctx, metaReviewDays); err != nil {
		return 0, 0, 0, err
	} else if ok {
		if parsed, convErr := strconv.Atoi(strings.TrimSpace(raw)); convErr == nil && parsed > 0 {
			reviewDays = parsed
		}
	}

	if raw, ok, err := tx.GetMeta(ctx, metaDeferDays); err != nil {
		return 0, 0, 0, err
	} else if ok {
		if parsed, convErr := strconv.Atoi(strings.TrimSpace(raw)); convErr == nil && parsed > 0 {
			deferWindow = time.Duration(parsed) * 24 * time.Hour
		}
	}

	if raw, ok, err := tx.GetMeta(ctx, metaHITLTimeoutHours); err != nil {
		return 0, 0, 0, err
	} else if ok {
		if parsed, convErr := strconv.Atoi(strings.TrimSpace(raw)); convErr == nil && parsed > 0 {
			hitlTimeoutHours = parsed
		}
	}

	return reviewDays, deferWindow, hitlTimeoutHours, nil
}

func (s *Service) SetBackfillWriteBatching(size int, timeout time.Duration, queueCapacity int) {
	if size > 0 {
		s.backfillBatchSize = size
	}
	if timeout > 0 {
		s.backfillBatchTimeout = timeout
	}
	if queueCapacity > 0 {
		s.backfillQueueCapacity = queueCapacity
	}
}

func (s *Service) SetDiscordService(discordSvc *discord.Service) {
	s.discord = discordSvc
}

func (s *Service) SetJellyfinClient(client *jellyfin.Client) {
	s.jellyfinClient = client
}

func (s *Service) SetRadarrService(remover radarrRemover) {
	s.radarr = remover
}

func (s *Service) SetSonarrService(remover sonarrRemover) {
	s.sonarr = remover
}

func (s *Service) HandleJellyfinWebhook(ctx context.Context, event jellyfin.WebhookEvent) error {
	if s.jellyfinClient != nil && isCatalogIndexEvent(event.EventType) && !isRemovalEvent(event.EventType) && event.ItemID != "" && len(event.Payload.ProviderIDs) == 0 {
		providerIDs, err := s.jellyfinClient.FetchProviderIDs(ctx, event.ItemID)
		if err != nil {
			s.logger.Warn("failed to lazy-fetch jellyfin provider ids", "item_id", event.ItemID, "error", err)
		} else if len(providerIDs) > 0 {
			event.Payload.ProviderIDs = providerIDs
		}
	}

	now := s.now().UTC()
	if sourceNow, ok := sourceTimestampForJellyfinEvent(event); ok {
		now = sourceNow
	}
	itemID, targets, finalizations, arrRemovals, err := s.applyJellyfinWebhookTx(ctx, event, now)
	if err != nil {
		return err
	}

	if s.discord != nil {
		for _, f := range finalizations {
			if err := s.discord.FinalizeHITLPrompt(ctx, f.channelID, f.messageID, f.content); err != nil {
				s.logger.Warn("failed to finalize playback-recovered HITL message", "item_id", itemID, "error", err)
			}
		}
	}

	for _, req := range arrRemovals {
		switch req.service {
		case "radarr":
			if s.radarr == nil {
				s.logger.Warn("radarr removal requested but radarr service is not configured", "item_id", req.itemID)
				continue
			}
			if err := s.radarr.RemoveByProviderIDs(ctx, req.providerIDs); err != nil {
				s.logger.Warn("radarr removal failed", "item_id", req.itemID, "error", err)
			}
		case "sonarr":
			if s.sonarr == nil {
				s.logger.Warn("sonarr removal requested but sonarr service is not configured", "item_id", req.itemID)
				continue
			}
			if err := s.sonarr.RemoveByProviderIDs(ctx, req.providerIDs); err != nil {
				s.logger.Warn("sonarr removal failed", "item_id", req.itemID, "error", err)
			}
		}
	}

	primaryTarget := ""
	if len(targets) > 0 {
		primaryTarget = targets[0].Canonical
	}

	s.logger.InfoContext(ctx,
		"processed jellyfin webhook",
		"lex", jellyfinLogLexicon(event.EventType),
		"event_type", event.EventType,
		"item_type", normalizeMediaType(event.Payload.ItemType),
		"title", chooseName(event.Payload.Name, event.Payload.SeasonName),
		"series", event.Payload.SeriesName,
		"season", event.Payload.SeasonName,
		"target_primary", primaryTarget,
		"target_count", len(targets),
		"item_id", itemID,
		"dedupe_key", event.DedupeKey,
	)
	s.wake(now)
	return nil
}

func (s *Service) applyJellyfinWebhookTx(ctx context.Context, event jellyfin.WebhookEvent, now time.Time) (string, []targetRef, []hitlFinalizeRequest, []arrRemovalRequest, error) {
	eventAt, _ := sourceTimestampForJellyfinEvent(event)
	playbackEvent := isPlaybackEvent(event.EventType)
	catalogIndexEvent := isCatalogIndexEvent(event.EventType)
	removalEvent := isRemovalEvent(event.EventType)
	dedupeKey := event.DedupeKey
	itemID := event.ItemID
	targets := deriveTargets(event)
	finalizations := make([]hitlFinalizeRequest, 0)
	arrRemovals := make([]arrRemovalRequest, 0)

	err := s.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		return s.applyJellyfinWebhookInTx(ctx, tx, event, now, eventAt, playbackEvent, catalogIndexEvent, removalEvent, dedupeKey, itemID, targets, &finalizations, &arrRemovals)
	})
	if err != nil {
		return "", nil, nil, nil, err
	}
	return itemID, targets, finalizations, arrRemovals, nil
}

func (s *Service) applyJellyfinWebhookInTx(ctx context.Context, tx repo.TxRepository, event jellyfin.WebhookEvent, now, eventAt time.Time, playbackEvent, catalogIndexEvent, removalEvent bool, dedupeKey, itemID string, targets []targetRef, finalizations *[]hitlFinalizeRequest, arrRemovals *[]arrRemovalRequest) error {
	defaultReviewDays, _, defaultHITLTimeoutHours, err := s.currentDefaultsFromMeta(ctx, tx)
	if err != nil {
		return err
	}
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

	seasonChildIDs := make([]string, 0)
	seasonChildren := make([]domain.MediaItem, 0)
	removedMedia := make([]domain.MediaItem, 0)
	seasonEpisodeDelta := map[string]int{}
	if removalEvent && strings.EqualFold(strings.TrimSpace(event.Payload.ItemType), "season") {
		seasonID := strings.TrimSpace(event.Payload.SeasonID)
		if seasonID == "" {
			seasonID = strings.TrimSpace(itemID)
		}
		if seasonID != "" {
			children, err := tx.ListMediaBySubject(ctx, "season", seasonID)
			if err != nil {
				return err
			}
			seasonChildren = append(seasonChildren, children...)
			for _, child := range children {
				if child.ItemID != "" {
					seasonChildIDs = append(seasonChildIDs, child.ItemID)
				}
			}
		}
	}

	if itemID != "" && (supportsMediaIndexType(event.Payload.ItemType) || playbackEvent) {
		existing, found, err := tx.GetMedia(ctx, itemID)
		if err != nil {
			return err
		}
		media := domain.MediaItem{ItemID: itemID, UpdatedAt: now}
		catalogUpdateAllowed := true
		playbackUpdateAllowed := true
		if found {
			media.CreatedAt = existing.CreatedAt
			media.Name = existing.Name
			media.Title = existing.Title
			media.ItemType = existing.ItemType
			media.SeasonID = existing.SeasonID
			media.SeasonName = existing.SeasonName
			media.SeriesID = existing.SeriesID
			media.SeriesName = existing.SeriesName
			media.ImageURL = existing.ImageURL
			media.LastPlayedAt = existing.LastPlayedAt
			media.PlayCountTotal = existing.PlayCountTotal
			media.LastCatalogEventAt = existing.LastCatalogEventAt
			media.LastPlaybackEventAt = existing.LastPlaybackEventAt
			media.ProviderIDs = existing.ProviderIDs
			if catalogIndexEvent && !eventAt.IsZero() && eventAt.Before(existing.LastCatalogEventAt) {
				catalogUpdateAllowed = false
			}
			if playbackEvent && !eventAt.IsZero() && eventAt.Before(existing.LastPlaybackEventAt) {
				playbackUpdateAllowed = false
			}
		}

		itemTypeLower := strings.ToLower(strings.TrimSpace(event.Payload.ItemType))
		if catalogIndexEvent && catalogUpdateAllowed {
			created := event.Payload.DateCreated.UTC()
			if created.IsZero() {
				created = event.Payload.DateLastMediaAdded.UTC()
			}
			if !created.IsZero() {
				if media.CreatedAt.IsZero() || created.Before(media.CreatedAt) {
					media.CreatedAt = created
				}
			}
			if event.Payload.Name != "" {
				media.Name = event.Payload.Name
				media.Title = event.Payload.Name
			}
			if event.Payload.ItemType != "" {
				media.ItemType = event.Payload.ItemType
			}
			if event.Payload.SeasonID != "" {
				media.SeasonID = event.Payload.SeasonID
			}
			if event.Payload.SeasonName != "" {
				media.SeasonName = event.Payload.SeasonName
			}
			if event.Payload.SeriesID != "" {
				media.SeriesID = event.Payload.SeriesID
			}
			if event.Payload.SeriesName != "" {
				media.SeriesName = event.Payload.SeriesName
			}
			if event.Payload.PrimaryImageURL != "" {
				media.ImageURL = event.Payload.PrimaryImageURL
			}
			if event.Payload.LastPlayedAt.After(media.LastPlayedAt) {
				media.LastPlayedAt = event.Payload.LastPlayedAt
			}
			if event.Payload.PlayCountTotal > media.PlayCountTotal {
				media.PlayCountTotal = event.Payload.PlayCountTotal
			}
			media.ProviderIDs = domain.MergeProviderIDs(media.ProviderIDs, event.Payload.ProviderIDs)
			if eventAt.After(media.LastCatalogEventAt) {
				media.LastCatalogEventAt = eventAt
			}
			if itemTypeLower == "episode" {
				oldSeasonID := strings.TrimSpace(existing.SeasonID)
				newSeasonID := strings.TrimSpace(media.SeasonID)
				switch {
				case !found && newSeasonID != "":
					seasonEpisodeDelta[newSeasonID]++
				case found && oldSeasonID != newSeasonID:
					if oldSeasonID != "" {
						seasonEpisodeDelta[oldSeasonID]--
					}
					if newSeasonID != "" {
						seasonEpisodeDelta[newSeasonID]++
					}
				}
			}
		}

		if playbackEvent && playbackUpdateAllowed {
			if eventAt.After(media.LastPlayedAt) {
				media.LastPlayedAt = eventAt
			}
			media.PlayCountTotal++
			if eventAt.After(media.LastPlaybackEventAt) {
				media.LastPlaybackEventAt = eventAt
			}
		}
		if !removalEvent {
			if err := tx.UpsertMedia(ctx, media); err != nil {
				return err
			}
		} else if found {
			removedMedia = append(removedMedia, existing)
			if itemTypeLower == "episode" {
				seasonID := strings.TrimSpace(existing.SeasonID)
				if seasonID == "" {
					seasonID = strings.TrimSpace(event.Payload.SeasonID)
				}
				if seasonID != "" {
					seasonEpisodeDelta[seasonID]--
				}
			}
		}
	}

	if removalEvent {
		if len(seasonChildren) > 0 {
			removedMedia = append(removedMedia, seasonChildren...)
		}
		if req, ok := buildARRRemovalRequest(event, removedMedia); ok && arrRemovals != nil {
			*arrRemovals = append(*arrRemovals, req)
		}

		if itemID != "" {
			if err := tx.DeleteMedia(ctx, itemID); err != nil {
				return err
			}
			if err := tx.DeleteFlow(ctx, targetKey("item", itemID)); err != nil {
				return err
			}
			if err := tx.DeleteFlow(ctx, targetKey("movie", itemID)); err != nil {
				return err
			}
		}

		for _, childID := range seasonChildIDs {
			if err := tx.DeleteMedia(ctx, childID); err != nil {
				return err
			}
			if err := tx.DeleteFlow(ctx, targetKey("item", childID)); err != nil {
				return err
			}
		}
	}

	for _, target := range targets {
		flow, found, err := tx.GetFlow(ctx, target.Canonical)
		if err != nil {
			return err
		}
		catalogFlowUpdateAllowed := true
		flowNeedsWrite := false
		targetID := strings.TrimSpace(target.ID)
		if !found {
			if !catalogIndexEvent {
				continue
			}
			flow = domain.Flow{
				FlowID:      flowIDFromItem(target.Canonical),
				ItemID:      target.Canonical,
				SubjectType: target.Type,
				DisplayName: target.Name,
				ImageURL:    target.ImageURL,
				State:       domain.FlowStateActive,
				Version:     0,
				PolicySnapshot: domain.PolicySnapshot{
					ExpireAfterDays: defaultReviewDays,
					HITLTimeoutHrs:  defaultHITLTimeoutHours,
					TimeoutAction:   "delete",
				},
				LastCatalogEventAt: eventAt,
				CreatedAt:          now,
			}
			if target.Type == "season" {
				flow.EpisodeCount = 0
			}
			if err := tx.UpsertFlowCAS(ctx, flow, 0); err != nil {
				return err
			}
		} else {
			if catalogIndexEvent && !eventAt.IsZero() && eventAt.Before(flow.LastCatalogEventAt) {
				catalogFlowUpdateAllowed = false
			}
			if catalogIndexEvent {
				if catalogFlowUpdateAllowed && target.Name != "" && flow.DisplayName != target.Name {
					flow.DisplayName = target.Name
					flowNeedsWrite = true
				}
				if catalogFlowUpdateAllowed && target.ImageURL != "" && flow.ImageURL != target.ImageURL {
					flow.ImageURL = target.ImageURL
					flowNeedsWrite = true
				}
				if catalogFlowUpdateAllowed && target.Type != "" && flow.SubjectType != target.Type {
					flow.SubjectType = target.Type
					flowNeedsWrite = true
				}
				if catalogFlowUpdateAllowed && eventAt.After(flow.LastCatalogEventAt) {
					flow.LastCatalogEventAt = eventAt
					flowNeedsWrite = true
				}
			}
			if flowNeedsWrite {
				expected := flow.Version
				flow.Version = expected + 1
				flow.UpdatedAt = now
				if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
					return err
				}
			}
		}

		if target.Type == "season" {
			if delta, ok := seasonEpisodeDelta[targetID]; ok && delta != 0 {
				next := flow.EpisodeCount + delta
				if next < 0 {
					next = 0
				}
				if next != flow.EpisodeCount {
					expected := flow.Version
					flow.EpisodeCount = next
					flow.UpdatedAt = now
					flow.Version = expected + 1
					if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
						return err
					}
				}
				delete(seasonEpisodeDelta, targetID)
			}
		}

		if removalEvent {
			if !shouldRemoveTargetFlowForRemovalEvent(event, target, &flow, tx, ctx, now) {
				continue
			}
			if err := tx.DeleteFlow(ctx, target.Canonical); err != nil {
				return err
			}
			continue
		}

		if !catalogIndexEvent && !playbackEvent {
			continue
		}

		if playbackEvent && flow.State == domain.FlowStatePendingReview {
			runAt := now
			resolvedPlayedAt := time.Time{}
			if playAt, known, err := mostRecentPlayForTarget(ctx, tx, flow); err != nil {
				return err
			} else if known {
				resolvedPlayedAt = playAt
				expireDays := flow.PolicySnapshot.ExpireAfterDays
				if expireDays <= 0 {
					expireDays = defaultExpireDays
				}
				dueAt := playAt.Add(time.Duration(expireDays) * 24 * time.Hour)
				if dueAt.After(runAt) {
					runAt = dueAt
				}
			}

			expected := flow.Version
			flow.State = domain.FlowStateActive
			flow.HITLOutcome = "played"
			flow.DecisionDeadlineAt = time.Time{}
			flow.NextActionAt = runAt
			flow.UpdatedAt = now
			flow.Version = expected + 1
			if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
				return err
			}
			if err := upsertEvaluatePolicyJob(ctx, tx, flow, now, runAt, "jellyfin_playback_recovered", dedupeKey+":eval"); err != nil {
				return err
			}
			if finalizations != nil && strings.TrimSpace(flow.Discord.ChannelID) != "" && strings.TrimSpace(flow.Discord.MessageID) != "" {
				display := strings.TrimSpace(flow.DisplayName)
				if display == "" {
					display = target.Canonical
				}
				*finalizations = append(*finalizations, hitlFinalizeRequest{
					channelID: flow.Discord.ChannelID,
					messageID: flow.Discord.MessageID,
					content:   fmt.Sprintf("Resolved: PLAYED at %s for %s", humanTimeLabel(resolvedPlayedAt), display),
				})
			}
			continue
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

	now, err := discordInteractionTimestamp(interaction)
	if err != nil {
		return interactionResponse("Invalid interaction timestamp."), nil
	}
	dedupeKey := discordDedupeKey(interaction)
	alreadyProcessed := false
	processedMessage := ""
	staleVersion := false
	staleMessage := ""
	decisionDisplayName := parsed.ItemID
	resolvedAction := parsed.Action

	err = s.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		_, defaultDeferWindow, _, err := s.currentDefaultsFromMeta(ctx, tx)
		if err != nil {
			return err
		}

		processed, err := tx.IsProcessed(ctx, dedupeKey)
		if err != nil {
			return err
		}
		if processed {
			alreadyProcessed = true
			if flow, found, err := tx.GetFlow(ctx, parsed.ItemID); err == nil {
				if found {
					display := strings.TrimSpace(flow.DisplayName)
					if display == "" {
						display = decisionDisplayName
					}
					processedMessage = staleDecisionMessage(flow, display, interaction)
				} else {
					processedMessage = fmt.Sprintf("Resolved: %s for %s (target no longer exists)", interactionDecisionLabel(parsed.Action), decisionDisplayName)
				}
			}
			return nil
		}

		flow, found, err := tx.GetFlow(ctx, parsed.ItemID)
		if err != nil {
			return err
		}
		if !found {
			staleVersion = true
			staleMessage = fmt.Sprintf("Resolved: %s for %s (target no longer exists)", interactionDecisionLabel(parsed.Action), decisionDisplayName)
			return nil
		}
		if strings.TrimSpace(flow.DisplayName) != "" {
			decisionDisplayName = strings.TrimSpace(flow.DisplayName)
		}

		expectedVersion := flow.Version
		if parsed.Version != expectedVersion {
			staleVersion = true
			staleMessage = staleDecisionMessage(flow, decisionDisplayName, interaction)
			return nil
		}

		flow.UpdatedAt = now
		effectiveAction := parsed.Action
		resolvedAction = effectiveAction
		flow.HITLOutcome = effectiveAction

		switch effectiveAction {
		case "archive":
			flow.State = domain.FlowStateArchived
		case "delay":
			flow.State = domain.FlowStateActive
			flow.NextActionAt = now.Add(defaultDeferWindow)
			flow.DecisionDeadlineAt = time.Time{}
			clearDiscordPromptLink(&flow.Discord)
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
			Type:           "discord.interaction." + effectiveAction,
			Source:         "discord",
			OccurredAt:     now,
			IdempotencyKey: dedupeKey,
			Payload: map[string]any{
				"interaction_id": interaction.InteractionID,
				"custom_id":      interaction.CustomID,
				"action":         effectiveAction,
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
		if strings.TrimSpace(processedMessage) == "" {
			processedMessage = fmt.Sprintf("Resolved: %s for %s", interactionDecisionLabel(parsed.Action), decisionDisplayName)
		}
		return interactionMessageUpdateResponse(processedMessage), nil
	}
	if staleVersion {
		if strings.TrimSpace(staleMessage) == "" {
			staleMessage = "Resolved: this prompt has already been handled."
		}
		return interactionMessageUpdateResponse(staleMessage), nil
	}

	s.wake(now)
	actor := discordInteractionActor(interaction)
	s.logger.InfoContext(ctx,
		"processed discord interaction decision",
		"lex", discordActionLexicon(resolvedAction),
		"action", resolvedAction,
		"subject_type", inferSubjectType(parsed.ItemID),
		"title", decisionDisplayName,
		"actor", actor,
		"item_id", parsed.ItemID,
	)
	return interactionDecisionUpdateResponse(resolvedAction, decisionDisplayName), nil
}

func (s *Service) ApplyAIDecision(ctx context.Context, itemID string, action string) error {
	itemID = strings.TrimSpace(itemID)
	action = strings.TrimSpace(strings.ToLower(action))
	if itemID == "" {
		return fmt.Errorf("item id is required")
	}
	if action == "" {
		return fmt.Errorf("action is required")
	}

	now := s.now().UTC()
	finalizeChannelID := ""
	finalizeMessageID := ""
	finalizeDisplayName := ""
	err := s.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, itemID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("flow not found for item %s", itemID)
		}

		expectedVersion := flow.Version
		flow.UpdatedAt = now
		flow.HITLOutcome = action
		finalizeChannelID = flow.Discord.ChannelID
		finalizeMessageID = flow.Discord.MessageID
		finalizeDisplayName = strings.TrimSpace(flow.DisplayName)

		switch action {
		case "archive":
			flow.State = domain.FlowStateArchived
			flow.NextActionAt = time.Time{}
			flow.DecisionDeadlineAt = time.Time{}
		case "unarchive":
			flow.State = domain.FlowStateActive
			flow.DecisionDeadlineAt = time.Time{}
			clearDiscordPromptLink(&flow.Discord)
			if err := enqueueEvaluatePolicy(ctx, tx, flow, now, "ai_unarchive", now); err != nil {
				return err
			}
		case "delete":
			flow.State = domain.FlowStateDeleteQueued
			flow.NextActionAt = now
			flow.DecisionDeadlineAt = time.Time{}
			if err := enqueueExecuteDelete(ctx, tx, flow, now); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported ai action %q", action)
		}

		flow.Version = expectedVersion + 1
		if err := tx.UpsertFlowCAS(ctx, flow, expectedVersion); err != nil {
			return err
		}

		event := domain.Event{
			EventID:        "evt:ai:" + shortHash(itemID+":"+action+":"+strconv.FormatInt(now.UnixNano(), 10)),
			FlowID:         flow.FlowID,
			ItemID:         flow.ItemID,
			Type:           "ai.decision." + action,
			Source:         "ai",
			OccurredAt:     now,
			IdempotencyKey: "ai:" + action + ":" + itemID + ":" + strconv.FormatInt(expectedVersion+1, 10),
			Payload: map[string]any{
				"action": action,
			},
		}
		return tx.AppendEvent(ctx, event)
	})
	if err != nil {
		return err
	}
	if s.discord != nil && finalizeChannelID != "" && finalizeMessageID != "" {
		display := finalizeDisplayName
		if display == "" {
			display = "item"
		}
		content := fmt.Sprintf("Resolved: %s for %s (AI).", interactionDecisionLabel(action), display)
		if err := s.discord.FinalizeHITLPrompt(ctx, finalizeChannelID, finalizeMessageID, content); err != nil {
			s.logger.Warn("failed to finalize ai decision HITL message", "item_id", itemID, "error", err)
		}
	}
	s.wake(now)
	return nil
}

func (s *Service) ApplyAIDecisionBatch(ctx context.Context, itemIDs []string, action string) error {
	if len(itemIDs) == 0 {
		return fmt.Errorf("item ids are required")
	}
	seen := map[string]struct{}{}
	for _, id := range itemIDs {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		if err := s.ApplyAIDecision(ctx, trimmed, action); err != nil {
			return err
		}
	}
	if len(seen) == 0 {
		return fmt.Errorf("item ids are required")
	}
	return nil
}

func (s *Service) ApplyAIDelayDays(ctx context.Context, itemID string, days int) error {
	itemID = strings.TrimSpace(itemID)
	if itemID == "" {
		return fmt.Errorf("item id is required")
	}
	if days <= 0 {
		return fmt.Errorf("days must be > 0")
	}
	now := s.now().UTC()
	delayUntil := now.Add(time.Duration(days) * 24 * time.Hour)
	finalizeChannelID := ""
	finalizeMessageID := ""
	finalizeDisplayName := ""
	err := s.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, itemID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("flow not found for item %s", itemID)
		}
		expectedVersion := flow.Version
		finalizeChannelID = flow.Discord.ChannelID
		finalizeMessageID = flow.Discord.MessageID
		finalizeDisplayName = strings.TrimSpace(flow.DisplayName)
		flow.State = domain.FlowStateActive
		flow.HITLOutcome = "delay"
		flow.NextActionAt = delayUntil
		flow.DecisionDeadlineAt = time.Time{}
		if flow.PolicySnapshot.ExpireAfterDays <= 0 {
			flow.PolicySnapshot.ExpireAfterDays = s.defaultExpireDays
		}
		flow.PolicySnapshot.ExpireAfterDays = days
		clearDiscordPromptLink(&flow.Discord)
		flow.UpdatedAt = now
		flow.Version = expectedVersion + 1
		if err := tx.UpsertFlowCAS(ctx, flow, expectedVersion); err != nil {
			return err
		}
		if err := enqueueEvaluatePolicy(ctx, tx, flow, now, "ai_delay", delayUntil); err != nil {
			return err
		}
		return tx.AppendEvent(ctx, domain.Event{
			EventID:        "evt:ai:delay:" + shortHash(itemID+":"+strconv.FormatInt(now.UnixNano(), 10)),
			FlowID:         flow.FlowID,
			ItemID:         flow.ItemID,
			Type:           "ai.decision.delay",
			Source:         "ai",
			OccurredAt:     now,
			IdempotencyKey: "ai:delay:" + itemID + ":" + strconv.Itoa(days) + ":" + strconv.FormatInt(flow.Version, 10),
			Payload: map[string]any{
				"days":        days,
				"delay_until": delayUntil.Format(time.RFC3339),
			},
		})
	})
	if err != nil {
		return err
	}
	if s.discord != nil && finalizeChannelID != "" && finalizeMessageID != "" {
		display := finalizeDisplayName
		if display == "" {
			display = "item"
		}
		content := fmt.Sprintf("Resolved: DELAYED %d days for %s (AI).", days, display)
		if err := s.discord.FinalizeHITLPrompt(ctx, finalizeChannelID, finalizeMessageID, content); err != nil {
			s.logger.Warn("failed to finalize ai delay HITL message", "item_id", itemID, "error", err)
		}
	}
	s.wake(now)
	return nil
}

func (s *Service) IngestBackfillPlayback(ctx context.Context, events []jellyfin.PlaybackEvent) error {
	return s.ingestBackfillPlaybackWithCursor(ctx, events, "", "")
}

func (s *Service) IngestBackfillPlaybackWithCursor(ctx context.Context, events []jellyfin.PlaybackEvent, cursorKey string, cursorValue string) error {
	return s.ingestBackfillPlaybackWithCursor(ctx, events, cursorKey, cursorValue)
}

func (s *Service) ingestBackfillPlaybackWithCursor(ctx context.Context, events []jellyfin.PlaybackEvent, cursorKey string, cursorValue string) error {
	total := len(events)
	opCh := make(chan backfillWriteOp, s.backfillQueueCapacity)
	for i, e := range events {
		if total > 0 && ((i+1)%ingestProgressEvery == 0 || i+1 == total) {
			s.logger.InfoContext(ctx, "backfill playback ingest progress", "lex", "BACKFILL-INGEST", "stream", "playback", "processed", i+1, "total", total)
		}

		key := "backfill:play:" + e.ItemID + ":" + e.Type + ":" + strconv.FormatInt(e.Date.UnixNano(), 10)
		if e.Date.IsZero() {
			key = "backfill:play:" + e.ItemID + ":" + e.Type + ":" + strconv.Itoa(i)
		}
		evt := jellyfin.WebhookEvent{
			Payload: jellyfin.WebhookPayload{
				ItemID:           e.ItemID,
				Name:             "",
				NotificationType: e.Type,
			},
			Raw:        map[string]any{"source": "backfill", "type": e.Type, "item_id": e.ItemID, "name": e.Name},
			ItemID:     e.ItemID,
			EventType:  e.Type,
			DedupeKey:  key,
			OccurredAt: e.Date,
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case opCh <- backfillWriteOp{event: &evt}:
		}
	}
	if cursorKey != "" {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case opCh <- backfillWriteOp{cursorKey: cursorKey, cursorValue: cursorValue}:
		}
	}
	close(opCh)
	if err := s.processWebhookBatches(ctx, opCh, "backfill_playback"); err != nil {
		return err
	}
	s.wake(s.now().UTC())
	return nil
}

func (s *Service) IngestBackfillItems(ctx context.Context, items []jellyfin.ItemSnapshot) error {
	return s.ingestBackfillItemsWithCursor(ctx, items, "", "")
}

func (s *Service) IngestBackfillItemsWithCursor(ctx context.Context, items []jellyfin.ItemSnapshot, cursorKey string, cursorValue string) error {
	return s.ingestBackfillItemsWithCursor(ctx, items, cursorKey, cursorValue)
}

func (s *Service) ingestBackfillItemsWithCursor(ctx context.Context, items []jellyfin.ItemSnapshot, cursorKey string, cursorValue string) error {
	total := len(items)
	opCh := make(chan backfillWriteOp, s.backfillQueueCapacity)
	for i, it := range items {
		if total > 0 && ((i+1)%ingestProgressEvery == 0 || i+1 == total) {
			s.logger.InfoContext(ctx, "backfill item ingest progress", "lex", "BACKFILL-INGEST", "stream", "items", "processed", i+1, "total", total)
		}

		key := backfillItemDedupeKey(it, i)
		evt := jellyfin.WebhookEvent{
			Payload: jellyfin.WebhookPayload{
				ItemID:             it.ItemID,
				ProviderIDs:        it.ProviderIDs,
				ItemType:           it.ItemType,
				Name:               it.Name,
				SeasonID:           it.SeasonID,
				SeasonName:         it.SeasonName,
				SeriesID:           it.SeriesID,
				SeriesName:         it.SeriesName,
				PrimaryImageURL:    it.ImageURL,
				DateCreated:        it.DateCreated,
				DateLastMediaAdded: it.DateLastMediaAdded,
				LastPlayedAt:       it.LastPlayedAt,
				PlayCountTotal:     int64(it.PlayCount),
				NotificationType:   "ItemUpdated",
			},
			Raw:        map[string]any{"source": "backfill", "item_id": it.ItemID},
			ItemID:     it.ItemID,
			EventType:  "ItemUpdated",
			DedupeKey:  key,
			OccurredAt: it.LastPlayedAt,
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case opCh <- backfillWriteOp{event: &evt}:
		}
	}
	if cursorKey != "" {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case opCh <- backfillWriteOp{cursorKey: cursorKey, cursorValue: cursorValue}:
		}
	}
	close(opCh)
	if err := s.processWebhookBatches(ctx, opCh, "backfill_items"); err != nil {
		return err
	}
	s.wake(s.now().UTC())
	return nil
}

type backfillWriteOp struct {
	event       *jellyfin.WebhookEvent
	cursorKey   string
	cursorValue string
}

func (s *Service) processWebhookBatches(ctx context.Context, ops <-chan backfillWriteOp, source string) error {
	batchSize := s.backfillBatchSize
	if batchSize <= 0 {
		batchSize = defaultBackfillBatchSize
	}
	flushTimeout := s.backfillBatchTimeout
	if flushTimeout <= 0 {
		flushTimeout = defaultBackfillBatchTimeout
	}

	batch := make([]backfillWriteOp, 0, batchSize)
	timer := time.NewTimer(flushTimeout)
	defer timer.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := s.repository.WithTx(ctx, func(tx repo.TxRepository) error {
			for _, op := range batch {
				if op.event == nil {
					if op.cursorKey != "" {
						if err := tx.SetMeta(ctx, op.cursorKey, op.cursorValue); err != nil {
							return err
						}
					}
					continue
				}
				event := *op.event
				now := s.now().UTC()
				if sourceNow, ok := sourceTimestampForJellyfinEvent(event); ok {
					now = sourceNow
				}
				eventAt := now
				if err := s.applyJellyfinWebhookInTx(
					ctx,
					tx,
					event,
					now,
					eventAt,
					isPlaybackEvent(event.EventType),
					isCatalogIndexEvent(event.EventType),
					isRemovalEvent(event.EventType),
					event.DedupeKey,
					event.ItemID,
					deriveTargets(event),
					nil,
					nil,
				); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		s.logger.InfoContext(ctx, "flushed backfill write batch", "lex", "BACKFILL-WRITE", "source", source, "batch_size", len(batch))
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case op, ok := <-ops:
			if !ok {
				return flush()
			}
			batch = append(batch, op)
			if len(batch) >= batchSize {
				if err := flush(); err != nil {
					return err
				}
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(flushTimeout)
			}
		case <-timer.C:
			if err := flush(); err != nil {
				return err
			}
			timer.Reset(flushTimeout)
		}
	}
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
	case "archive", "delay", "delete":
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
	decision := interactionDecisionLabel(action)
	item := strings.TrimSpace(itemDisplay)
	if item == "" {
		item = "item"
	}
	return interactionMessageUpdateResponse(fmt.Sprintf("Resolved: %s for %s", decision, item))
}

func staleDecisionMessage(flow domain.Flow, itemDisplay string, interaction discord.IncomingInteraction) string {
	item := strings.TrimSpace(itemDisplay)
	if item == "" {
		item = "item"
	}
	decision := strings.TrimSpace(strings.ToLower(flow.HITLOutcome))
	hasDecision := decision != ""
	if !hasDecision {
		switch flow.State {
		case domain.FlowStateArchived:
			decision = "archive"
			hasDecision = true
		case domain.FlowStateDeleteInProgress:
			decision = "delete_requested"
			hasDecision = true
		case domain.FlowStateDeleteQueued:
			decision = "delete_requested"
			hasDecision = true
		case domain.FlowStateDeleted:
			decision = "delete"
			hasDecision = true
		}
	}
	message := ""
	if hasDecision {
		message = fmt.Sprintf("Resolved: %s for %s", interactionDecisionLabel(decision), item)
	} else {
		message = fmt.Sprintf("This prompt is stale for %s. No decision has been recorded yet.", item)
	}
	if reference := latestPromptReference(flow, interaction); reference != "" {
		return message + "\n\n" + reference
	}
	return message
}

func latestPromptReference(flow domain.Flow, interaction discord.IncomingInteraction) string {
	clickedMessageID := ""
	if interaction.Raw != nil && interaction.Raw.Message != nil {
		clickedMessageID = strings.TrimSpace(interaction.Raw.Message.ID)
	}
	currentChannelID := strings.TrimSpace(flow.Discord.ChannelID)
	currentMessageID := strings.TrimSpace(flow.Discord.MessageID)
	previousChannelID := strings.TrimSpace(flow.Discord.PreviousChannelID)
	previousMessageID := strings.TrimSpace(flow.Discord.PreviousMessageID)

	if clickedMessageID != "" {
		if currentMessageID != "" && clickedMessageID == currentMessageID {
			return ""
		}
		if currentMessageID == "" && previousMessageID != "" && clickedMessageID == previousMessageID {
			return "No newer prompt exists yet; this message is now closed."
		}
	}

	targetChannelID := currentChannelID
	targetMessageID := currentMessageID
	if targetChannelID == "" || targetMessageID == "" {
		targetChannelID = previousChannelID
		targetMessageID = previousMessageID
	}
	if targetChannelID == "" || targetMessageID == "" {
		return ""
	}

	guildID := strings.TrimSpace(interaction.GuildID)
	if guildID != "" {
		return fmt.Sprintf("This prompt is stale. Use the latest one: https://discord.com/channels/%s/%s/%s", guildID, targetChannelID, targetMessageID)
	}
	return fmt.Sprintf("This prompt is stale. Use the latest one in channel %s (message %s).", targetChannelID, targetMessageID)
}

func clearDiscordPromptLink(ctx *domain.DiscordContext) {
	if ctx == nil {
		return
	}
	msg := strings.TrimSpace(ctx.MessageID)
	if msg != "" {
		ctx.PreviousChannelID = strings.TrimSpace(ctx.ChannelID)
		ctx.PreviousMessageID = msg
	}
	ctx.MessageID = ""
}

func interactionDecisionLabel(action string) string {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "archive":
		return "ARCHIVED"
	case "delay":
		return "DELAYED"
	case "delete", "delete_requested":
		return "DELETE REQUESTED"
	default:
		v := strings.TrimSpace(action)
		if v == "" {
			return "RESOLVED"
		}
		return strings.ToUpper(v)
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
		seasonLabel := formatSeasonLabel(p.SeasonName, p.SeriesName, p.Name)
		out = push(out, targetRef{Type: "season", ID: p.SeasonID, Name: seasonLabel, ImageURL: p.PrimaryImageURL})
	case "season":
		seasonLabel := formatSeasonLabel(chooseName(p.Name, p.SeasonName), p.SeriesName, p.Name)
		out = push(out, targetRef{Type: "season", ID: p.ItemID, Name: seasonLabel, ImageURL: p.PrimaryImageURL})
	case "series":
	case "movie":
		out = push(out, targetRef{Type: "movie", ID: p.ItemID, Name: p.Name, ImageURL: p.PrimaryImageURL})
	default:
	}
	return out
}

func shouldRemoveTargetFlowForRemovalEvent(event jellyfin.WebhookEvent, target targetRef, flow *domain.Flow, tx repo.TxRepository, ctx context.Context, now time.Time) bool {
	itemType := strings.ToLower(strings.TrimSpace(event.Payload.ItemType))
	if itemType == "episode" && target.Type == "season" {
		if flow != nil && flow.EpisodeCount > 0 {
			return false
		}
		remaining, err := tx.ListMediaBySubject(ctx, "season", strings.TrimSpace(target.ID))
		if err != nil {
			return true
		}
		if flow != nil && len(remaining) > 0 && flow.EpisodeCount != len(remaining) {
			expected := flow.Version
			flow.EpisodeCount = len(remaining)
			flow.UpdatedAt = now
			flow.Version = expected + 1
			if upsertErr := tx.UpsertFlowCAS(ctx, *flow, expected); upsertErr != nil {
				return true
			}
		}
		return len(remaining) == 0
	}
	return true
}

func buildARRRemovalRequest(event jellyfin.WebhookEvent, removed []domain.MediaItem) (arrRemovalRequest, bool) {
	itemType := strings.ToLower(strings.TrimSpace(event.Payload.ItemType))
	if itemType == "movie" {
		if ids := domain.NormalizeProviderIDs(event.Payload.ProviderIDs); len(ids) > 0 {
			return arrRemovalRequest{service: "radarr", providerIDs: ids, itemID: event.ItemID}, true
		}
		for _, media := range removed {
			if strings.EqualFold(strings.TrimSpace(media.ItemType), "movie") {
				if ids := domain.NormalizeProviderIDs(media.ProviderIDs); len(ids) > 0 {
					return arrRemovalRequest{service: "radarr", providerIDs: ids, itemID: event.ItemID}, true
				}
			}
		}
		return arrRemovalRequest{}, false
	}

	if itemType == "episode" || itemType == "season" || itemType == "series" {
		if ids := domain.NormalizeProviderIDs(event.Payload.ProviderIDs); len(ids) > 0 {
			return arrRemovalRequest{service: "sonarr", providerIDs: ids, itemID: event.ItemID}, true
		}
		for _, media := range removed {
			if ids := domain.NormalizeProviderIDs(media.ProviderIDs); len(ids) > 0 {
				return arrRemovalRequest{service: "sonarr", providerIDs: ids, itemID: event.ItemID}, true
			}
		}
	}

	return arrRemovalRequest{}, false
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
	return "target:" + targetType + ":" + domain.NormalizeID(id)
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

func isCatalogIndexEvent(eventType string) bool {
	t := strings.ToLower(strings.TrimSpace(eventType))
	if t == "" {
		return false
	}
	if strings.Contains(t, "itemadded") || strings.Contains(t, "itemupdated") || strings.Contains(t, "newitem") {
		return true
	}
	if strings.Contains(t, "library") && strings.Contains(t, "item") {
		return true
	}
	return false
}

func isRemovalEvent(eventType string) bool {
	t := strings.ToLower(strings.TrimSpace(eventType))
	if t == "" {
		return false
	}
	return strings.Contains(t, "removed") || strings.Contains(t, "deleted")
}

func catalogEventTimestamp(payload jellyfin.WebhookPayload) time.Time {
	if payload.DateLastMediaAdded.After(payload.DateCreated) {
		return payload.DateLastMediaAdded.UTC()
	}
	if !payload.DateCreated.IsZero() {
		return payload.DateCreated.UTC()
	}
	return time.Time{}
}

func sourceTimestampForJellyfinEvent(event jellyfin.WebhookEvent) (time.Time, bool) {
	if !event.OccurredAt.IsZero() {
		return event.OccurredAt.UTC(), true
	}
	if event.Payload.LastPlayedAt.After(time.Time{}) {
		return event.Payload.LastPlayedAt.UTC(), true
	}
	if ts := catalogEventTimestamp(event.Payload); !ts.IsZero() {
		return ts.UTC(), true
	}
	if ts := rawEventTimestamp(event.Raw); !ts.IsZero() {
		return ts.UTC(), true
	}
	return time.Time{}, false
}

func rawEventTimestamp(raw map[string]any) time.Time {
	if raw == nil {
		return time.Time{}
	}
	keys := []string{"Date", "DateCreated", "DateLastMediaAdded", "Timestamp", "OccurredAt"}
	for _, key := range keys {
		v, ok := raw[key]
		if !ok {
			continue
		}
		switch val := v.(type) {
		case string:
			if ts, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(val)); err == nil {
				return ts.UTC()
			}
		case time.Time:
			return val.UTC()
		case float64:
			if val > 0 {
				return time.Unix(0, int64(val)*int64(time.Millisecond)).UTC()
			}
		}
	}
	return time.Time{}
}

func discordInteractionTimestamp(interaction discord.IncomingInteraction) (time.Time, error) {
	ts, err := discordgo.SnowflakeTimestamp(interaction.InteractionID)
	if err != nil {
		return time.Time{}, err
	}
	return ts.UTC(), nil
}

func discordInteractionActor(interaction discord.IncomingInteraction) string {
	if interaction.Raw == nil {
		return "unknown"
	}
	if interaction.Raw.Member != nil && interaction.Raw.Member.User != nil {
		user := interaction.Raw.Member.User
		if user.Username != "" {
			return user.Username
		}
		if user.ID != "" {
			return user.ID
		}
	}
	if interaction.Raw.User != nil {
		if interaction.Raw.User.Username != "" {
			return interaction.Raw.User.Username
		}
		if interaction.Raw.User.ID != "" {
			return interaction.Raw.User.ID
		}
	}
	return "unknown"
}

func jellyfinLogLexicon(eventType string) string {
	if isRemovalEvent(eventType) {
		return "REMOVAL"
	}
	if isPlaybackEvent(eventType) {
		return "PLAYBACK-METRIC"
	}
	if isCatalogIndexEvent(eventType) {
		return "CATALOG-INDEX"
	}
	return "JELLYFIN-EVENT"
}

func discordActionLexicon(action string) string {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "archive":
		return "ARCHIVAL"
	case "delay":
		return "DEFER"
	case "delete":
		return "DELETION-REQUEST"
	default:
		return "DISCORD-DECISION"
	}
}

func normalizeMediaType(itemType string) string {
	t := strings.ToUpper(strings.TrimSpace(itemType))
	if t == "" {
		return "UNKNOWN"
	}
	return t
}

func supportsMediaIndexType(itemType string) bool {
	t := strings.ToLower(strings.TrimSpace(itemType))
	return t == "movie" || t == "episode"
}

func backfillItemDedupeKey(item jellyfin.ItemSnapshot, ordinal int) string {
	revision := item.DateLastMediaAdded.UnixNano()
	if revision == 0 {
		revision = item.DateCreated.UnixNano()
	}
	if revision == 0 {
		revision = item.LastPlayedAt.UnixNano()
	}
	if revision == 0 {
		revision = int64(item.PlayCount)
	}
	if revision == 0 {
		revision = int64(ordinal)
	}
	return "backfill:item:" + item.ItemID + ":" + strconv.FormatInt(revision, 10)
}

func formatSeasonLabel(seasonName string, seriesName string, fallback string) string {
	season := strings.TrimSpace(seasonName)
	series := strings.TrimSpace(seriesName)
	if season != "" && series != "" {
		return season + " of " + series
	}
	return chooseName(seasonName, seriesName, fallback)
}

func humanTimeLabel(t time.Time) string {
	if t.IsZero() {
		return "unknown"
	}
	unix := t.UTC().Unix()
	return fmt.Sprintf("<t:%d:R> (<t:%d:f>)", unix, unix)
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}
