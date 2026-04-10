package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"jellyreaper/internal/discord"
	"jellyreaper/internal/domain"
	"jellyreaper/internal/jellyfin"
	"jellyreaper/internal/jobs"
	"jellyreaper/internal/radarr"
	"jellyreaper/internal/repo"
	"jellyreaper/internal/scheduler"
	"jellyreaper/internal/sonarr"
)

const minHITLResponseWindow = 24 * time.Hour
const metaReviewDays = "settings.review_days"
const metaHITLTimeoutHours = "settings.hitl_timeout_hours"

type EvaluatePolicyHandler struct {
	repository        repo.Repository
	logger            *slog.Logger
	evalScheduler     scheduler.EvalRequester
	defaultExpireDays int
	defaultHITLHours  int
}

func NewEvaluatePolicyHandler(repository repo.Repository, logger *slog.Logger) *EvaluatePolicyHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &EvaluatePolicyHandler{
		repository:        repository,
		logger:            logger,
		evalScheduler:     scheduler.NewScheduler(nil, nil),
		defaultExpireDays: 30,
		defaultHITLHours:  48,
	}
}

func (h *EvaluatePolicyHandler) SetEvalScheduler(s scheduler.EvalRequester) {
	h.evalScheduler = s
}

func (h *EvaluatePolicyHandler) SetDefaultExpireDays(days int) {
	if days > 0 {
		h.defaultExpireDays = days
	}
}

func (h *EvaluatePolicyHandler) SetDefaultHITLTimeoutHours(hours int) {
	if hours > 0 {
		h.defaultHITLHours = hours
	}
}

func (h *EvaluatePolicyHandler) Kind() domain.JobKind { return domain.JobKindEvaluatePolicy }

// OnTerminalFailure re-schedules the singleton eval job with a cooldown
// so the flow doesn't get stuck if policy evaluation hits a persistent
// error (e.g. corrupt media index, transient DB issue that outlasted
// all retries).
func (h *EvaluatePolicyHandler) OnTerminalFailure(ctx context.Context, job domain.JobRecord) error {
	now := time.Now().UTC()
	retryAfter := now.Add(10 * time.Minute)
	return h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, job.ItemID)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		h.logger.Info("eval policy terminal failure: re-scheduling eval", "lex", "POLICY-EVAL", "item_id", job.ItemID, "retry_at", retryAfter)
		return h.evalScheduler.RequestEval(ctx, tx, flow, now, retryAfter, "eval_recovery", "eval:recovery:"+flow.ItemID, flow.Version)
	})
}

func (h *EvaluatePolicyHandler) Handle(ctx context.Context, job domain.JobRecord) error {
	now := time.Now().UTC()

	payload, err := jobs.DecodePayload[jobs.EvaluatePolicyPayload](job)
	if err != nil {
		return err
	}

	return h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, job.ItemID)
		if err != nil {
			return err
		}
		if !found {
			// Flow is gone (e.g. delete handler purged it). Stale eval — bail.
			h.logger.Info("policy evaluation skipped", "lex", "POLICY-EVAL", "item_id", job.ItemID, "reason", "flow_missing")
			return nil
		}
		if payload.FlowVersion != 0 && flow.Version != payload.FlowVersion {
			// Something newer mutated the flow after this eval was scheduled.
			// The latest writer is responsible for scheduling the next action.
			h.logger.Info("policy evaluation skipped", "lex", "POLICY-EVAL", "item_id", job.ItemID, "reason", "stale_version", "payload_version", payload.FlowVersion, "flow_version", flow.Version)
			return nil
		}

		if flow.State == domain.FlowStateArchived ||
			flow.State == domain.FlowStateDeleted ||
			flow.State == domain.FlowStateDeleteQueued ||
			flow.State == domain.FlowStateDeleteFailed {
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

		hitlTimeoutHours := flow.PolicySnapshot.HITLTimeoutHrs
		if raw, ok, err := tx.GetMeta(ctx, metaHITLTimeoutHours); err != nil {
			return err
		} else if ok {
			if parsed, convErr := strconv.Atoi(strings.TrimSpace(raw)); convErr == nil && parsed > 0 {
				hitlTimeoutHours = parsed
			}
		}
		if hitlTimeoutHours <= 0 {
			hitlTimeoutHours = h.defaultHITLHours
		}
		flow.PolicySnapshot.HITLTimeoutHrs = hitlTimeoutHours
		if strings.TrimSpace(flow.PolicySnapshot.TimeoutAction) == "" {
			flow.PolicySnapshot.TimeoutAction = "delete"
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

				h.logger.Info("policy evaluation deferred", "lex", "POLICY-EVAL", "item_id", job.ItemID, "reason", "not_due_yet", "last_played_at", lastPlayed, "due_at", dueAt)
				return h.evalScheduler.RequestEval(ctx, tx, flow, now, dueAt, "not_due_yet", "eval:"+flow.ItemID, flow.Version)
			}
		}

		expected := flow.Version
		flow.State = domain.FlowStatePendingReview
		// Clear any prior cycle's outcome so the fresh PendingReview phase
		// starts unpoisoned. The previous outcome (e.g. "played", "delay")
		// belonged to a cycle that has now closed.
		flow.HITLOutcome = ""
		flow.DecisionDeadlineAt = time.Time{}
		flow.UpdatedAt = now
		flow.Version = expected + 1
		if flow.CreatedAt.IsZero() {
			flow.CreatedAt = now
		}
		if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
			return err
		}

		promptPayload, err := json.Marshal(jobs.SendHITLPromptPayload{
			ChannelID:   flow.Discord.ChannelID,
			FlowVersion: flow.Version,
		})
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
			PayloadJSON:    promptPayload,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		h.logger.Info("policy evaluation queued hitl", "lex", "POLICY-EVAL", "item_id", job.ItemID, "reason", "stale_due", "flow_state", flow.State, "flow_version", flow.Version)
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
		if latest.IsZero() && (parts[1] == "movie" || parts[1] == "item") {
			for _, candidate := range domain.AlternateIDForms(parts[2]) {
				media, found, err := tx.GetMedia(ctx, candidate)
				if err != nil {
					return time.Time{}, false, err
				}
				if found && media.LastPlayedAt.After(latest) {
					latest = media.LastPlayedAt
				}
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
	evalScheduler    scheduler.EvalRequester
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
		evalScheduler:    scheduler.NewScheduler(nil, nil),
	}
}

func (h *SendHITLPromptHandler) SetEvalScheduler(s scheduler.EvalRequester) {
	h.evalScheduler = s
}

func (h *SendHITLPromptHandler) Kind() domain.JobKind { return domain.JobKindSendHITLPrompt }

func (h *SendHITLPromptHandler) Handle(ctx context.Context, job domain.JobRecord) error {
	if h.discord == nil {
		return fmt.Errorf("discord session not configured")
	}

	payload, err := jobs.DecodePayload[jobs.SendHITLPromptPayload](job)
	if err != nil {
		return fmt.Errorf("decode prompt payload: %w", err)
	}

	now := time.Now().UTC()
	var flow domain.Flow
	shouldSend := true
	statusLine := ""
	staleMessageID := ""
	staleChannelID := ""
	err = h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		current, found, err := tx.GetFlow(ctx, job.ItemID)
		if err != nil {
			return err
		}
		if !found {
			h.logger.Info("hitl prompt skipped", "lex", "HITL-PROMPT", "item_id", job.ItemID, "reason", "flow_missing")
			shouldSend = false
			return nil
		}
		if payload.FlowVersion != 0 && current.Version != payload.FlowVersion {
			h.logger.Info("hitl prompt skipped", "lex", "HITL-PROMPT", "item_id", job.ItemID, "reason", "stale_version", "payload_version", payload.FlowVersion, "flow_version", current.Version)
			shouldSend = false
			return nil
		}
		if current.State != domain.FlowStatePendingReview {
			h.logger.Info("hitl prompt skipped", "lex", "HITL-PROMPT", "item_id", job.ItemID, "reason", "not_pending_review", "flow_state", current.State)
			shouldSend = false
			return nil
		}
		flow = current
		if lastPlayedAt, ok, err := mostRecentPlayForFlow(ctx, tx, flow); err != nil {
			return err
		} else if ok {
			statusLine = "Last played at: " + humanTimeLabel(lastPlayedAt)
		} else if createdAt, createdKnown, err := mostRecentCreatedForFlow(ctx, tx, flow); err != nil {
			return err
		} else if createdKnown {
			statusLine = "Last played at: never (added " + humanTimeLabel(createdAt) + ")"
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
			if !found {
				h.logger.Info("hitl prompt skipped", "lex", "HITL-PROMPT", "item_id", job.ItemID, "reason", "flow_missing_during_stale_clear")
				shouldSend = false
				return nil
			}
			if current.State != domain.FlowStatePendingReview {
				h.logger.Info("hitl prompt skipped", "lex", "HITL-PROMPT", "item_id", job.ItemID, "reason", "not_pending_review_during_stale_clear", "flow_state", current.State)
				shouldSend = false
				return nil
			}
			if strings.TrimSpace(current.Discord.MessageID) == "" {
				return nil
			}
			expected := current.Version
			current.Discord.PreviousChannelID = strings.TrimSpace(current.Discord.ChannelID)
			current.Discord.PreviousMessageID = strings.TrimSpace(current.Discord.MessageID)
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

	// The version we embed in the Discord button is the version we're about
	// to write in tx3. If anything mutates the flow between this point and
	// tx3 the CAS will fail (or the state guard below will bail), we'll
	// finalize the message as obsolete, and the next eval cycle will create
	// a fresh prompt.
	buttonVersion := flow.Version + 1
	messageID, err := h.discord.SendHITLPrompt(ctx, channelID, job.ItemID, buttonVersion, flow.DisplayName, flow.ImageURL, statusLine)
	if err != nil {
		return err
	}

	bailReason := ""
	bailDetails := []any{}
	err = h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		current, found, err := tx.GetFlow(ctx, job.ItemID)
		if err != nil {
			return err
		}
		if !found {
			bailReason = "flow_missing_after_send"
			return nil
		}
		// Drift check: between tx1/tx2 and now, did anyone touch the flow?
		// If so, the message we just sent references a stale version and
		// will be no-op'd by the interaction handler. Bail and finalize the
		// dangling message below.
		if current.Version != flow.Version {
			bailReason = "version_drift_after_send"
			bailDetails = []any{"expected_version", flow.Version, "actual_version", current.Version}
			return nil
		}
		if current.State != domain.FlowStatePendingReview {
			bailReason = "state_changed_after_send"
			bailDetails = []any{"flow_state", current.State}
			return nil
		}

		expected := current.Version
		timeoutWindow := h.hitlTimeout
		if hours := current.PolicySnapshot.HITLTimeoutHrs; hours > 0 {
			timeoutWindow = time.Duration(hours) * time.Hour
		}
		deadline := now.Add(timeoutWindow)
		minDeadline := now.Add(minHITLResponseWindow)
		if deadline.Before(minDeadline) {
			deadline = minDeadline
		}

		current.DecisionDeadlineAt = deadline
		current.NextActionAt = current.DecisionDeadlineAt
		current.Discord.ChannelID = channelID
		if existing := strings.TrimSpace(current.Discord.MessageID); existing != "" && existing != strings.TrimSpace(messageID) {
			current.Discord.PreviousChannelID = strings.TrimSpace(channelID)
			current.Discord.PreviousMessageID = existing
		}
		current.Discord.MessageID = messageID
		current.UpdatedAt = now
		current.Version = expected + 1

		if err := tx.UpsertFlowCAS(ctx, current, expected); err != nil {
			return err
		}

		timeoutPayload, err := json.Marshal(jobs.HITLTimeoutPayload{
			DefaultAction: "delete",
			FlowVersion:   current.Version,
		})
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
	if err != nil {
		return err
	}
	if bailReason != "" {
		args := append([]any{"lex", "HITL-PROMPT", "item_id", job.ItemID, "reason", bailReason}, bailDetails...)
		h.logger.Info("hitl prompt skipped after send; finalizing dangling message", args...)
		// Best-effort finalize so the user isn't left looking at a dead button.
		if finalizeErr := h.discord.FinalizeHITLPrompt(ctx, channelID, messageID, "Resolved: this prompt is obsolete (state moved on)."); finalizeErr != nil {
			h.logger.Warn("failed to finalize obsolete prompt", "lex", "HITL-PROMPT", "item_id", job.ItemID, "error", finalizeErr)
		}
	}
	return nil
}

// OnTerminalFailure rolls the flow back from pending_review to active
// and re-schedules the singleton evaluate_policy job so the state machine
// will re-attempt the HITL cycle after a cooldown.
func (h *SendHITLPromptHandler) OnTerminalFailure(ctx context.Context, job domain.JobRecord) error {
	now := time.Now().UTC()
	retryAfter := now.Add(10 * time.Minute)
	return h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, job.ItemID)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if flow.State != domain.FlowStatePendingReview {
			return nil
		}
		expected := flow.Version
		flow.State = domain.FlowStateActive
		flow.HITLOutcome = ""
		flow.DecisionDeadlineAt = time.Time{}
		flow.NextActionAt = retryAfter
		flow.UpdatedAt = now
		flow.Version = expected + 1
		if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
			return err
		}
		h.logger.Info("hitl prompt terminal failure: rolled flow back to active", "lex", "HITL-PROMPT", "item_id", job.ItemID, "retry_at", retryAfter)
		return h.evalScheduler.RequestEval(ctx, tx, flow, now, retryAfter, "hitl_prompt_recovery", "eval:recovery:"+flow.ItemID, flow.Version)
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

// OnTerminalFailure re-enqueues a fresh timeout job so the default
// action (delete) is re-attempted. The flow stays in pending_review —
// the human already missed their window, so re-entering the
// eval→prompt cycle would be wrong. We just need another shot at
// applying the timeout action.
func (h *HITLTimeoutHandler) OnTerminalFailure(ctx context.Context, job domain.JobRecord) error {
	now := time.Now().UTC()
	retryAfter := now.Add(10 * time.Minute)
	return h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, job.ItemID)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if flow.State != domain.FlowStatePendingReview {
			return nil
		}

		timeoutPayload, err := json.Marshal(jobs.HITLTimeoutPayload{
			DefaultAction: "delete",
			FlowVersion:   flow.Version,
		})
		if err != nil {
			return err
		}
		h.logger.Info("hitl timeout terminal failure: re-scheduling timeout", "lex", "HITL-TIMEOUT", "item_id", job.ItemID, "retry_at", retryAfter)
		return tx.EnqueueJob(ctx, domain.JobRecord{
			JobID:          "job:timeout:recovery:" + job.ItemID + ":" + strconv.FormatInt(now.UnixNano(), 10),
			FlowID:         flow.FlowID,
			ItemID:         flow.ItemID,
			Kind:           domain.JobKindHITLTimeout,
			Status:         domain.JobStatusPending,
			RunAt:          retryAfter,
			MaxAttempts:    5,
			IdempotencyKey: "timeout:recovery:" + flow.ItemID + ":" + strconv.FormatInt(now.Unix()/(10*60), 10),
			PayloadJSON:    timeoutPayload,
			CreatedAt:      now,
			UpdatedAt:      now,
		})
	})
}

func (h *HITLTimeoutHandler) Handle(ctx context.Context, job domain.JobRecord) error {
	now := time.Now().UTC()

	payload, err := jobs.DecodePayload[jobs.HITLTimeoutPayload](job)
	if err != nil {
		return fmt.Errorf("decode timeout payload: %w", err)
	}

	finalizeChannelID := ""
	finalizeMessageID := ""
	finalizeDisplayName := ""
	err = h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, job.ItemID)
		if err != nil {
			return err
		}
		if !found {
			h.logger.Info("hitl timeout skipped", "lex", "HITL-TIMEOUT", "item_id", job.ItemID, "reason", "flow_missing")
			return nil
		}
		if payload.FlowVersion != 0 && flow.Version != payload.FlowVersion {
			h.logger.Info("hitl timeout skipped", "lex", "HITL-TIMEOUT", "item_id", job.ItemID, "reason", "stale_version", "payload_version", payload.FlowVersion, "flow_version", flow.Version)
			return nil
		}
		if flow.State != domain.FlowStatePendingReview {
			h.logger.Info("hitl timeout skipped", "lex", "HITL-TIMEOUT", "item_id", job.ItemID, "reason", "not_pending_review", "flow_state", flow.State)
			return nil
		}
		if flow.HITLOutcome != "" && flow.HITLOutcome != "delete" {
			h.logger.Info("hitl timeout skipped", "lex", "HITL-TIMEOUT", "item_id", job.ItemID, "reason", "outcome_resolved", "outcome", flow.HITLOutcome)
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

			timeoutPayload, err := json.Marshal(jobs.HITLTimeoutPayload{
				DefaultAction: "delete",
				FlowVersion:   flow.Version,
			})
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

		// Delete jobs intentionally carry no FlowVersion — destruction is
		// authoritative.
		deletePayload, err := json.Marshal(jobs.ExecuteDeletePayload{RequestedBy: "timeout"})
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
			PayloadJSON:    deletePayload,
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

type ExecuteDeleteHandler struct {
	repository repo.Repository
	client     *jellyfin.Client
	discord    *discord.Service
	logger     *slog.Logger
	radarr     radarrRemover
	sonarr     sonarrRemover
}

type radarrRemover interface {
	RemoveByProviderIDs(context.Context, map[string]string) error
}

type sonarrRemover interface {
	RemoveSeasonByProviderIDs(context.Context, map[string]string, int) error
}

func NewExecuteDeleteHandler(repository repo.Repository, client *jellyfin.Client) *ExecuteDeleteHandler {
	return &ExecuteDeleteHandler{repository: repository, client: client, logger: slog.Default()}
}

func (h *ExecuteDeleteHandler) SetDiscordService(discordSvc *discord.Service) {
	h.discord = discordSvc
}

func (h *ExecuteDeleteHandler) SetLogger(logger *slog.Logger) {
	if logger != nil {
		h.logger = logger
	}
}

func (h *ExecuteDeleteHandler) SetRadarrService(remover radarrRemover) {
	h.radarr = remover
}

func (h *ExecuteDeleteHandler) SetSonarrService(remover sonarrRemover) {
	h.sonarr = remover
}

func (h *ExecuteDeleteHandler) Kind() domain.JobKind { return domain.JobKindExecuteDelete }

func (h *ExecuteDeleteHandler) Handle(ctx context.Context, job domain.JobRecord) error {
	if h.client == nil {
		return fmt.Errorf("jellyfin client is not configured")
	}

	// Single read-only snapshot at the top. We do NOT mutate the flow into a
	// transitional state — DeleteQueued is the only "delete in flight" signal,
	// and other handlers bail on it via their state guards. The flow either
	// gets force-deleted in the final tx or, on terminal failure, gets force-
	// transitioned to DeleteFailed by the dispatcher hook.
	flow, found, err := h.getFlow(ctx, job.ItemID)
	if err != nil {
		return err
	}
	if !found {
		// Already gone — nothing to do. Delete is idempotent.
		h.logger.Info("execute delete skipped", "lex", "DELETION", "item_id", job.ItemID, "reason", "flow_missing")
		return nil
	}
	if flow.SubjectType != "season" && flow.SubjectType != "movie" && flow.SubjectType != "item" {
		return fmt.Errorf("unsupported delete subject type: %s", flow.SubjectType)
	}

	deletedChildren := []domain.MediaItem{}
	radarrPrimaryDelete := false
	sonarrPrimarySeasonDelete := false
	if flow.SubjectType == "season" {
		children, err := h.listChildren(ctx, "season", strings.TrimPrefix(flow.ItemID, "target:season:"))
		if err != nil {
			return err
		}
		deletedChildren = children
		if h.sonarr != nil {
			providerIDs := domain.NormalizeProviderIDs(flow.ProviderIDs)
			seasonNumber := seasonNumberFromDeletedMedia(deletedChildren)
			switch {
			case len(providerIDs) == 0:
				h.logger.Info("sonarr season delete falling back to jellyfin due to missing projection provider ids", "lex", "DELETE-SONARR", "item_id", flow.ItemID)
			case seasonNumber <= 0:
				h.logger.Info("sonarr season delete falling back to jellyfin due to missing season number", "lex", "DELETE-SONARR", "item_id", flow.ItemID)
			default:
				h.logger.Info("execute delete sonarr primary season action", "lex", "DELETE-SONARR", "item_id", flow.ItemID, "season_number", seasonNumber, "child_count", len(deletedChildren))
				if err := h.sonarr.RemoveSeasonByProviderIDs(ctx, providerIDs, seasonNumber); err != nil {
					if errors.Is(err, sonarr.ErrNotManaged) {
						h.logger.Info("sonarr season delete falling back to jellyfin because media is unmanaged", "lex", "DELETE-SONARR", "item_id", flow.ItemID, "error", err)
					} else {
						return fmt.Errorf("sonarr primary season delete for %s: %w", flow.ItemID, err)
					}
				} else {
					sonarrPrimarySeasonDelete = true
				}
			}
		}
		if !sonarrPrimarySeasonDelete {
			children, err := h.deleteAggregateChildren(ctx, flow)
			if err != nil {
				return err
			}
			deletedChildren = children
		}
	} else {
		deleteID := flow.ItemID
		if strings.HasPrefix(deleteID, "target:item:") || strings.HasPrefix(deleteID, "target:movie:") {
			deleteID = deleteID[strings.LastIndex(deleteID, ":")+1:]
		}
		media, found, err := h.getMedia(ctx, deleteID)
		if err != nil {
			return err
		}
		if found {
			deletedChildren = append(deletedChildren, media)
		}

		if h.radarr != nil && (flow.SubjectType == "movie" || flow.SubjectType == "item") {
			providerIDs := domain.NormalizeProviderIDs(flow.ProviderIDs)
			if len(providerIDs) == 0 {
				h.logger.Info("radarr movie delete falling back to jellyfin due to missing projection provider ids", "lex", "DELETE-RADARR", "item_id", flow.ItemID)
			} else if err := h.radarr.RemoveByProviderIDs(ctx, providerIDs); err != nil {
				if errors.Is(err, radarr.ErrNotManaged) {
					h.logger.Info("radarr movie delete falling back to jellyfin because media is unmanaged", "lex", "DELETE-RADARR", "item_id", flow.ItemID, "error", err)
				} else {
					return fmt.Errorf("radarr primary delete for %s: %w", flow.ItemID, err)
				}
			} else {
				radarrPrimaryDelete = true
			}
		}

		if !radarrPrimaryDelete {
			if err := h.client.DeleteItem(ctx, deleteID); err != nil {
				return err
			}
		}
		if len(deletedChildren) == 0 {
			deletedChildren = append(deletedChildren, domain.MediaItem{ItemID: deleteID})
		}
	}

	now := time.Now().UTC()
	purged := 0
	err = h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		// Force-delete: no version check, no state check. The I/O has
		// already happened externally; the local state must catch up
		// regardless of what concurrent writers may have done.
		current, found, err := tx.GetFlow(ctx, flow.ItemID)
		if err != nil {
			return err
		}
		if found {
			if err := tx.DeleteFlow(ctx, current.ItemID); err != nil {
				return err
			}
		}

		for _, child := range deletedChildren {
			childID := strings.TrimSpace(child.ItemID)
			if childID == "" {
				continue
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
		if err := tx.AppendEvent(ctx, domain.Event{
			EventID:        "evt:delete:" + flow.ItemID + ":" + strconv.FormatInt(now.UnixNano(), 10),
			FlowID:         flow.FlowID,
			ItemID:         flow.ItemID,
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

		// Cleanup-last (within this tx): purge any other jobs referencing
		// this item — stale eval/prompt/timeout records, and any sibling
		// delete jobs that piled up. This blasts our own job record too;
		// CompleteJob is nil-on-not-found so the dispatcher will treat the
		// vanished record as a successful completion.
		n, err := tx.DeleteJobsForItem(ctx, flow.ItemID)
		if err != nil {
			return err
		}
		purged = n
		return nil
	})
	if err != nil {
		return err
	}
	h.logger.Info("execute delete committed",
		"lex", "DELETION",
		"item_id", flow.ItemID,
		"subject_type", flow.SubjectType,
		"child_count", len(deletedChildren),
		"jobs_purged", purged,
	)
	if h.discord != nil && strings.TrimSpace(flow.Discord.ChannelID) != "" && strings.TrimSpace(flow.Discord.MessageID) != "" {
		name := strings.TrimSpace(flow.DisplayName)
		if name == "" {
			name = flow.ItemID
		}
		if err := h.discord.FinalizeHITLPrompt(ctx, flow.Discord.ChannelID, flow.Discord.MessageID, fmt.Sprintf("Resolved: DELETED for %s", name)); err != nil {
			h.logger.Warn("failed to finalize delete prompt", "lex", "DELETION", "item_id", flow.ItemID, "error", err)
		}
	}
	return nil
}

func (h *ExecuteDeleteHandler) getFlow(ctx context.Context, itemID string) (domain.Flow, bool, error) {
	var out domain.Flow
	var found bool
	err := h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		var err error
		out, found, err = tx.GetFlow(ctx, itemID)
		return err
	})
	if err != nil {
		return domain.Flow{}, false, err
	}
	return out, found, nil
}

func (h *ExecuteDeleteHandler) getMedia(ctx context.Context, itemID string) (domain.MediaItem, bool, error) {
	var media domain.MediaItem
	var found bool
	err := h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		var err error
		media, found, err = tx.GetMedia(ctx, itemID)
		return err
	})
	if err != nil {
		return domain.MediaItem{}, false, err
	}
	return media, found, nil
}

func seasonNumberFromDeletedMedia(deleted []domain.MediaItem) int {
	for _, media := range deleted {
		name := strings.TrimSpace(media.SeasonName)
		if name == "" {
			continue
		}
		tokens := strings.FieldsFunc(name, func(r rune) bool {
			return r < '0' || r > '9'
		})
		for _, tok := range tokens {
			if tok == "" {
				continue
			}
			if n, err := strconv.Atoi(tok); err == nil && n > 0 {
				return n
			}
		}
	}
	return 0
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

func humanTimeLabel(t time.Time) string {
	if t.IsZero() {
		return "unknown"
	}
	unix := t.UTC().Unix()
	return fmt.Sprintf("<t:%d:R> (<t:%d:f>)", unix, unix)
}
