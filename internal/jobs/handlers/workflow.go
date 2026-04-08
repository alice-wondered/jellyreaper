package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"jellyreaper/internal/discord"
	"jellyreaper/internal/domain"
	"jellyreaper/internal/jellyfin"
	"jellyreaper/internal/jobs"
	"jellyreaper/internal/repo"
)

const minHITLResponseWindow = 24 * time.Hour
const metaReviewDays = "settings.review_days"

type EvaluatePolicyHandler struct {
	repository        repo.Repository
	logger            *slog.Logger
	defaultExpireDays int
}

func NewEvaluatePolicyHandler(repository repo.Repository, logger *slog.Logger) *EvaluatePolicyHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &EvaluatePolicyHandler{repository: repository, logger: logger, defaultExpireDays: 30}
}

func (h *EvaluatePolicyHandler) SetDefaultExpireDays(days int) {
	if days > 0 {
		h.defaultExpireDays = days
	}
}

func (h *EvaluatePolicyHandler) Kind() domain.JobKind { return domain.JobKindEvaluatePolicy }

func (h *EvaluatePolicyHandler) Handle(ctx context.Context, job domain.JobRecord) error {
	now := time.Now().UTC()

	return h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, job.ItemID)
		if err != nil {
			return err
		}
		if !found {
			flow = domain.Flow{
				FlowID:    "flow:" + job.ItemID,
				ItemID:    job.ItemID,
				State:     domain.FlowStateActive,
				Version:   0,
				CreatedAt: now,
			}
		}

		if flow.State == domain.FlowStateArchived || flow.State == domain.FlowStateDeleted || flow.State == domain.FlowStateDeleteQueued || flow.State == domain.FlowStateDeleteInProgress {
			h.logger.Info("policy evaluation skipped", "lex", "POLICY-EVAL", "item_id", job.ItemID, "reason", "terminal_or_archived_state", "flow_state", flow.State)
			return nil
		}
		if flow.State == domain.FlowStatePendingReview {
			h.logger.Info("policy evaluation skipped", "lex", "POLICY-EVAL", "item_id", job.ItemID, "reason", "already_pending_review", "flow_state", flow.State)
			return nil
		}

		expireDays := flow.PolicySnapshot.ExpireAfterDays
		if raw, ok, err := tx.GetMeta(ctx, metaReviewDays); err != nil {
			return err
		} else if ok {
			if parsed, convErr := strconv.Atoi(strings.TrimSpace(raw)); convErr == nil && parsed > 0 {
				expireDays = parsed
			}
		}
		if expireDays <= 0 {
			expireDays = h.defaultExpireDays
		}

		lastPlayed, known, err := mostRecentPlayForFlow(ctx, tx, flow)
		if err != nil {
			return err
		}
		if !known {
			createdAt, createdKnown, err := mostRecentCreatedForFlow(ctx, tx, flow)
			if err != nil {
				return err
			}
			if createdKnown {
				lastPlayed = createdAt
				known = true
				h.logger.Info("policy evaluation fallback timestamp", "lex", "POLICY-EVAL", "item_id", job.ItemID, "reason", "use_created_at", "fallback_at", lastPlayed)
			} else {
				lastPlayed = time.Unix(0, 0).UTC()
				known = true
				h.logger.Info("policy evaluation fallback timestamp", "lex", "POLICY-EVAL", "item_id", job.ItemID, "reason", "use_epoch", "fallback_at", lastPlayed)
			}
		}
		if known {
			dueAt := lastPlayed.Add(time.Duration(expireDays) * 24 * time.Hour)
			if dueAt.After(now) {
				expected := flow.Version
				flow.NextActionAt = dueAt
				flow.UpdatedAt = now
				flow.Version = expected + 1
				if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
					return err
				}

				payload, err := json.Marshal(jobs.EvaluatePolicyPayload{Reason: "not_due_yet"})
				if err != nil {
					return err
				}
				h.logger.Info("policy evaluation deferred", "lex", "POLICY-EVAL", "item_id", job.ItemID, "reason", "not_due_yet", "last_played_at", lastPlayed, "due_at", dueAt)
				return upsertScheduledEvaluateJob(ctx, tx, flow, now, dueAt, payload)
			}
		}

		expected := flow.Version
		flow.State = domain.FlowStatePendingReview
		flow.HITLOutcome = ""
		flow.UpdatedAt = now
		flow.Version = expected + 1
		if flow.CreatedAt.IsZero() {
			flow.CreatedAt = now
		}
		if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
			return err
		}

		payload, err := json.Marshal(jobs.SendHITLPromptPayload{ChannelID: flow.Discord.ChannelID})
		if err != nil {
			return err
		}

		promptJob := domain.JobRecord{
			JobID:          "job:prompt:" + job.ItemID + ":" + strconv.FormatInt(now.UnixNano(), 10),
			FlowID:         flow.FlowID,
			ItemID:         flow.ItemID,
			Kind:           domain.JobKindSendHITLPrompt,
			Status:         domain.JobStatusPending,
			RunAt:          now,
			MaxAttempts:    5,
			IdempotencyKey: "job:prompt:" + flow.ItemID + ":" + strconv.FormatInt(flow.Version, 10),
			PayloadJSON:    payload,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		h.logger.Info("policy evaluation queued hitl", "lex", "POLICY-EVAL", "item_id", job.ItemID, "reason", "stale_due", "flow_state", flow.State)
		return tx.EnqueueJob(ctx, promptJob)
	})
}

func mostRecentPlayForFlow(ctx context.Context, tx repo.TxRepository, flow domain.Flow) (time.Time, bool, error) {
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

func mostRecentCreatedForFlow(ctx context.Context, tx repo.TxRepository, flow domain.Flow) (time.Time, bool, error) {
	parts := strings.SplitN(flow.ItemID, ":", 3)
	if len(parts) == 3 && parts[0] == "target" {
		items, err := tx.ListMediaBySubject(ctx, parts[1], parts[2])
		if err != nil {
			return time.Time{}, false, err
		}
		latest := time.Time{}
		for _, item := range items {
			if item.CreatedAt.After(latest) {
				latest = item.CreatedAt
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
	if !found || item.CreatedAt.IsZero() {
		return time.Time{}, false, nil
	}
	return item.CreatedAt, true, nil
}

type SendHITLPromptHandler struct {
	repository       repo.Repository
	logger           *slog.Logger
	discord          *discord.Service
	defaultChannelID string
	hitlTimeout      time.Duration
}

func NewSendHITLPromptHandler(repository repo.Repository, logger *slog.Logger, discord *discord.Service, defaultChannelID string, hitlTimeout time.Duration) *SendHITLPromptHandler {
	if logger == nil {
		logger = slog.Default()
	}
	if hitlTimeout <= 0 {
		hitlTimeout = 48 * time.Hour
	}
	return &SendHITLPromptHandler{
		repository:       repository,
		logger:           logger,
		discord:          discord,
		defaultChannelID: defaultChannelID,
		hitlTimeout:      hitlTimeout,
	}
}

func (h *SendHITLPromptHandler) Kind() domain.JobKind { return domain.JobKindSendHITLPrompt }

func (h *SendHITLPromptHandler) Handle(ctx context.Context, job domain.JobRecord) error {
	if h.discord == nil {
		return fmt.Errorf("discord session not configured")
	}

	var payload jobs.SendHITLPromptPayload
	if err := json.Unmarshal(job.PayloadJSON, &payload); err != nil && len(job.PayloadJSON) > 0 {
		return fmt.Errorf("decode prompt payload: %w", err)
	}

	now := time.Now().UTC()
	var flow domain.Flow
	var found bool
	shouldSend := true
	statusLine := ""
	staleMessageID := ""
	staleChannelID := ""
	err := h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		var err error
		flow, found, err = tx.GetFlow(ctx, job.ItemID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("flow not found for item %s", job.ItemID)
		}
		if flow.State != domain.FlowStatePendingReview {
			shouldSend = false
			return nil
		}
		if lastPlayedAt, ok, err := mostRecentPlayForFlow(ctx, tx, flow); err != nil {
			return err
		} else if ok {
			statusLine = "Last played at: " + humanTimeLabel(lastPlayedAt)
		}
		if flow.Discord.MessageID != "" {
			shouldSend = false
			staleMessageID = strings.TrimSpace(flow.Discord.MessageID)
			staleChannelID = strings.TrimSpace(flow.Discord.ChannelID)
			return nil
		}
		return nil
	})
	if err != nil {
		return err
	}
	if staleMessageID != "" {
		exists, _ := h.discord.HITLPromptExists(ctx, staleChannelID, staleMessageID)
		if exists {
			return nil
		}
		clearNow := time.Now().UTC()
		cleared := false
		err = h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
			current, found, err := tx.GetFlow(ctx, job.ItemID)
			if err != nil {
				return err
			}
			if !found || current.State != domain.FlowStatePendingReview {
				shouldSend = false
				return nil
			}
			if strings.TrimSpace(current.Discord.MessageID) == "" {
				return nil
			}
			expected := current.Version
			current.Discord.MessageID = ""
			current.UpdatedAt = clearNow
			current.Version = expected + 1
			if err := tx.UpsertFlowCAS(ctx, current, expected); err != nil {
				return err
			}
			cleared = true
			flow = current
			return nil
		})
		if err != nil {
			return err
		}
		if cleared {
			shouldSend = true
		}
	}
	if !shouldSend {
		return nil
	}

	channelID := payload.ChannelID
	if channelID == "" {
		channelID = flow.Discord.ChannelID
	}
	if channelID == "" {
		channelID = h.defaultChannelID
	}
	if channelID == "" {
		return fmt.Errorf("no discord channel configured for item %s", job.ItemID)
	}

	version := flow.Version
	messageID, err := h.discord.SendHITLPrompt(ctx, channelID, job.ItemID, version, flow.DisplayName, flow.ImageURL, statusLine)
	if err != nil {
		return err
	}

	return h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		current, found, err := tx.GetFlow(ctx, job.ItemID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("flow not found for update %s", job.ItemID)
		}

		expected := current.Version
		current.State = domain.FlowStatePendingReview
		deadline := now.Add(h.hitlTimeout)
		minDeadline := now.Add(minHITLResponseWindow)
		if deadline.Before(minDeadline) {
			deadline = minDeadline
		}

		current.DecisionDeadlineAt = deadline
		current.NextActionAt = current.DecisionDeadlineAt
		current.Discord.ChannelID = channelID
		current.Discord.MessageID = messageID
		current.UpdatedAt = now
		current.Version = expected + 1

		if err := tx.UpsertFlowCAS(ctx, current, expected); err != nil {
			return err
		}

		timeoutPayload, err := json.Marshal(jobs.HITLTimeoutPayload{DefaultAction: "delete"})
		if err != nil {
			return err
		}

		return tx.EnqueueJob(ctx, domain.JobRecord{
			JobID:          "job:timeout:" + job.ItemID + ":" + strconv.FormatInt(now.UnixNano(), 10),
			FlowID:         current.FlowID,
			ItemID:         current.ItemID,
			Kind:           domain.JobKindHITLTimeout,
			Status:         domain.JobStatusPending,
			RunAt:          current.DecisionDeadlineAt,
			MaxAttempts:    5,
			IdempotencyKey: "job:timeout:" + current.ItemID + ":" + strconv.FormatInt(current.Version, 10),
			PayloadJSON:    timeoutPayload,
			CreatedAt:      now,
			UpdatedAt:      now,
		})
	})
}

type HITLTimeoutHandler struct {
	repository repo.Repository
	discord    *discord.Service
	logger     *slog.Logger
}

func NewHITLTimeoutHandler(repository repo.Repository, discordSvc *discord.Service, logger *slog.Logger) *HITLTimeoutHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &HITLTimeoutHandler{repository: repository, discord: discordSvc, logger: logger}
}

func (h *HITLTimeoutHandler) Kind() domain.JobKind { return domain.JobKindHITLTimeout }

func (h *HITLTimeoutHandler) Handle(ctx context.Context, job domain.JobRecord) error {
	now := time.Now().UTC()
	finalizeChannelID := ""
	finalizeMessageID := ""
	finalizeDisplayName := ""
	err := h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, job.ItemID)
		if err != nil {
			return err
		}
		if !found || flow.State != domain.FlowStatePendingReview {
			return nil
		}
		if flow.HITLOutcome != "" && flow.HITLOutcome != "delete" {
			return nil
		}

		deadline := flow.DecisionDeadlineAt
		if deadline.IsZero() {
			deadline = now.Add(minHITLResponseWindow)
		}
		if deadline.After(now) {
			expected := flow.Version
			flow.NextActionAt = deadline
			flow.UpdatedAt = now
			flow.Version = expected + 1
			if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
				return err
			}

			timeoutPayload, err := json.Marshal(jobs.HITLTimeoutPayload{DefaultAction: "delete"})
			if err != nil {
				return err
			}
			return tx.EnqueueJob(ctx, domain.JobRecord{
				JobID:          "job:timeout:" + job.ItemID + ":" + strconv.FormatInt(now.UnixNano(), 10),
				FlowID:         flow.FlowID,
				ItemID:         flow.ItemID,
				Kind:           domain.JobKindHITLTimeout,
				Status:         domain.JobStatusPending,
				RunAt:          deadline,
				MaxAttempts:    5,
				IdempotencyKey: "job:timeout:deferred:" + flow.ItemID + ":" + strconv.FormatInt(flow.Version, 10),
				PayloadJSON:    timeoutPayload,
				CreatedAt:      now,
				UpdatedAt:      now,
			})
		}

		expected := flow.Version
		flow.State = domain.FlowStateDeleteQueued
		flow.NextActionAt = now
		flow.HITLOutcome = "delete"
		flow.UpdatedAt = now
		flow.Version = expected + 1
		if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
			return err
		}

		finalizeChannelID = flow.Discord.ChannelID
		finalizeMessageID = flow.Discord.MessageID
		finalizeDisplayName = flow.DisplayName

		payload, err := json.Marshal(jobs.ExecuteDeletePayload{RequestedBy: "timeout"})
		if err != nil {
			return err
		}
		return tx.EnqueueJob(ctx, domain.JobRecord{
			JobID:          "job:delete:" + job.ItemID + ":" + strconv.FormatInt(now.UnixNano(), 10),
			FlowID:         flow.FlowID,
			ItemID:         flow.ItemID,
			Kind:           domain.JobKindExecuteDelete,
			Status:         domain.JobStatusPending,
			RunAt:          now,
			MaxAttempts:    5,
			IdempotencyKey: "job:delete:" + flow.ItemID + ":" + strconv.FormatInt(flow.Version, 10),
			PayloadJSON:    payload,
			CreatedAt:      now,
			UpdatedAt:      now,
		})
	})
	if err != nil {
		return err
	}

	if h.discord != nil && finalizeChannelID != "" && finalizeMessageID != "" {
		name := strings.TrimSpace(finalizeDisplayName)
		if name == "" {
			name = strings.TrimSpace(job.ItemID)
		}
		content := fmt.Sprintf("Resolved: DELETE REQUESTED for %s (timeout).", name)
		if err := h.discord.FinalizeHITLPrompt(ctx, finalizeChannelID, finalizeMessageID, content); err != nil {
			h.logger.Warn("failed to finalize timeout HITL message", "item_id", job.ItemID, "error", err)
		}
	}

	return nil
}

func upsertScheduledEvaluateJob(ctx context.Context, tx repo.TxRepository, flow domain.Flow, now time.Time, runAt time.Time, payload []byte) error {
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
		job.IdempotencyKey = "job:eval:scheduled:" + flow.ItemID
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
		IdempotencyKey: "job:eval:scheduled:" + flow.ItemID,
		PayloadJSON:    payload,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
}

type ExecuteDeleteHandler struct {
	repository repo.Repository
	client     *jellyfin.Client
	discord    *discord.Service
}

func NewExecuteDeleteHandler(repository repo.Repository, client *jellyfin.Client) *ExecuteDeleteHandler {
	return &ExecuteDeleteHandler{repository: repository, client: client}
}

func (h *ExecuteDeleteHandler) SetDiscordService(discordSvc *discord.Service) {
	h.discord = discordSvc
}

func (h *ExecuteDeleteHandler) Kind() domain.JobKind { return domain.JobKindExecuteDelete }

func (h *ExecuteDeleteHandler) Handle(ctx context.Context, job domain.JobRecord) error {
	if h.client == nil {
		return fmt.Errorf("jellyfin client is not configured")
	}

	flow, expected, ok, err := h.loadFlowForDelete(ctx, job.ItemID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	deletedChildren := []domain.MediaItem{}
	if flow.SubjectType == "season" || flow.SubjectType == "series" {
		children, err := h.deleteAggregateChildren(ctx, flow)
		if err != nil {
			return err
		}
		deletedChildren = children
	} else {
		deleteID := flow.ItemID
		if strings.HasPrefix(deleteID, "target:item:") || strings.HasPrefix(deleteID, "target:movie:") {
			deleteID = deleteID[strings.LastIndex(deleteID, ":")+1:]
		}
		if err := h.client.DeleteItem(ctx, deleteID); err != nil {
			return err
		}
		deletedChildren = append(deletedChildren, domain.MediaItem{ItemID: deleteID})
	}

	now := time.Now().UTC()
	committed := false
	err = h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		current, found, err := tx.GetFlow(ctx, flow.ItemID)
		if err != nil {
			return err
		}
		if !found || current.Version != expected {
			return nil
		}

		if err := tx.DeleteFlow(ctx, current.ItemID); err != nil {
			return err
		}

		seasonIDs := map[string]struct{}{}
		for _, child := range deletedChildren {
			childID := strings.TrimSpace(child.ItemID)
			if childID == "" {
				continue
			}
			if flow.SubjectType == "series" && strings.TrimSpace(child.SeasonID) != "" {
				seasonIDs[strings.TrimSpace(child.SeasonID)] = struct{}{}
			}
			if err := tx.DeleteMedia(ctx, childID); err != nil {
				return err
			}

			childFlowID := "target:item:" + childID
			if err := tx.DeleteFlow(ctx, childFlowID); err != nil {
				return err
			}
			if err := tx.DeleteFlow(ctx, "target:movie:"+childID); err != nil {
				return err
			}
		}
		if flow.SubjectType == "series" {
			for seasonID := range seasonIDs {
				if err := tx.DeleteFlow(ctx, "target:season:"+seasonID); err != nil {
					return err
				}
			}
		}

		if err := tx.AppendEvent(ctx, domain.Event{
			EventID:        "evt:delete:" + current.ItemID + ":" + strconv.FormatInt(now.UnixNano(), 10),
			FlowID:         current.FlowID,
			ItemID:         current.ItemID,
			Type:           "jellyfin.item.deleted",
			Source:         "scheduler",
			OccurredAt:     now,
			IdempotencyKey: job.IdempotencyKey + ":deleted",
			Payload: map[string]any{
				"job_id": job.JobID,
			},
		}); err != nil {
			return err
		}
		committed = true
		return nil
	})
	if err != nil {
		return err
	}
	if committed && h.discord != nil && strings.TrimSpace(flow.Discord.ChannelID) != "" && strings.TrimSpace(flow.Discord.MessageID) != "" {
		name := strings.TrimSpace(flow.DisplayName)
		if name == "" {
			name = flow.ItemID
		}
		if err := h.discord.FinalizeHITLPrompt(ctx, flow.Discord.ChannelID, flow.Discord.MessageID, fmt.Sprintf("Resolved: DELETED for %s", name)); err != nil {
			// best effort
		}
	}
	return nil
}

func (h *ExecuteDeleteHandler) deleteAggregateChildren(ctx context.Context, flow domain.Flow) ([]domain.MediaItem, error) {
	parts := strings.SplitN(flow.ItemID, ":", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid target id %s", flow.ItemID)
	}
	children, err := h.listChildren(ctx, parts[1], parts[2])
	if err != nil {
		return nil, err
	}
	deleted := make([]domain.MediaItem, 0, len(children))
	for _, child := range children {
		if child.ItemID == "" {
			continue
		}
		if err := h.client.DeleteItem(ctx, child.ItemID); err != nil {
			return nil, err
		}
		deleted = append(deleted, child)
	}
	return deleted, nil
}

func (h *ExecuteDeleteHandler) listChildren(ctx context.Context, subjectType, subjectID string) ([]domain.MediaItem, error) {
	var out []domain.MediaItem
	err := h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		var err error
		out, err = tx.ListMediaBySubject(ctx, subjectType, subjectID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (h *ExecuteDeleteHandler) loadFlowForDelete(ctx context.Context, itemID string) (domain.Flow, int64, bool, error) {
	var out domain.Flow
	var expected int64
	var ok bool
	err := h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, itemID)
		if err != nil {
			return err
		}
		if !found || flow.State != domain.FlowStateDeleteQueued {
			ok = false
			return nil
		}

		expected = flow.Version
		flow.State = domain.FlowStateDeleteInProgress
		flow.UpdatedAt = time.Now().UTC()
		flow.Version = expected + 1
		if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
			return err
		}

		out = flow
		expected = flow.Version
		ok = true
		return nil
	})
	if err != nil {
		return domain.Flow{}, 0, false, err
	}
	return out, expected, ok, nil
}

func humanTimeLabel(t time.Time) string {
	if t.IsZero() {
		return "unknown"
	}
	return t.UTC().Format("2006-01-02 15:04 UTC")
}
