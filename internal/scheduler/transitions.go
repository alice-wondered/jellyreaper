package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/jobs"
	"jellyreaper/internal/repo"
)

// FlowManager is the single authority on flow state transitions.
// External systems declare intent ("this was played", "delete this",
// "delay 30 days") and the FlowManager handles loading the flow,
// validating state, writing the CAS update, scheduling/purging jobs,
// recording the event, and returning any post-commit side-effects.
//
// Callers never touch flow state or job scheduling directly.
type FlowManager struct {
	repo   repo.Repository
	eval   EvalRequester
	logger *slog.Logger
}

func NewFlowManager(repository repo.Repository, eval EvalRequester, logger *slog.Logger) *FlowManager {
	if logger == nil {
		logger = slog.Default()
	}
	if eval == nil {
		eval = NewScheduler(nil, nil)
	}
	return &FlowManager{repo: repository, eval: eval, logger: logger}
}

// TransitionResult captures side-effects that must happen after the tx
// commits. Callers inspect this and perform Discord I/O, wake signals, etc.
type TransitionResult struct {
	Flow           domain.Flow
	FinalizePrompt *PromptFinalization
	WakeAt         time.Time
}

// PromptFinalization describes a Discord HITL prompt message that should
// be edited/finalized after the transaction commits.
type PromptFinalization struct {
	ChannelID string
	MessageID string
	Content   string
}

// TransitionSource identifies who/what triggered the transition, used
// for event recording.
type TransitionSource struct {
	Source  string         // "ai", "discord", "jellyfin", "scheduler", "system"
	Actor  string         // username, interaction ID, etc. (optional)
	Reason string         // human-readable reason (optional)
	Extra  map[string]any // additional event payload fields (optional)
}

// Archive transitions a flow to archived state.
func (m *FlowManager) Archive(ctx context.Context, itemID string, src TransitionSource) (*TransitionResult, error) {
	now := time.Now().UTC()
	var result TransitionResult
	err := m.repo.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, itemID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("flow not found: %s", itemID)
		}
		if flow.State == domain.FlowStateArchived {
			result.Flow = flow
			return nil
		}
		if flow.State == domain.FlowStateDeleteQueued || flow.State == domain.FlowStateDeleted || flow.State == domain.FlowStateDeleteFailed {
			return fmt.Errorf("cannot archive flow in state %s", flow.State)
		}
		if flow.State == domain.FlowStatePendingReview {
			result.FinalizePrompt = captureFinalization(&flow, "archive", src)
		}
		expected := flow.Version
		flow.State = domain.FlowStateArchived
		flow.HITLOutcome = "archive"
		flow.NextActionAt = time.Time{}
		flow.DecisionDeadlineAt = time.Time{}
		flow.UpdatedAt = now
		flow.Version = expected + 1
		if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
			return err
		}
		result.Flow = flow
		return m.appendTransitionEvent(ctx, tx, flow, now, "archive", src)
	})
	return &result, err
}

// Unarchive transitions a flow from archived back to active and
// schedules an immediate eval.
func (m *FlowManager) Unarchive(ctx context.Context, itemID string, src TransitionSource) (*TransitionResult, error) {
	now := time.Now().UTC()
	var result TransitionResult
	err := m.repo.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, itemID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("flow not found: %s", itemID)
		}
		if flow.State != domain.FlowStateArchived {
			return fmt.Errorf("cannot unarchive flow in state %s", flow.State)
		}
		expected := flow.Version
		flow.State = domain.FlowStateActive
		flow.HITLOutcome = ""
		flow.DecisionDeadlineAt = time.Time{}
		flow.NextActionAt = now
		flow.Discord.ClearPromptLink()
		flow.UpdatedAt = now
		flow.Version = expected + 1
		if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
			return err
		}
		if err := m.eval.RequestEval(ctx, tx, flow, now, now, src.evalReason("unarchive"), "eval:unarchive:"+flow.ItemID, flow.Version); err != nil {
			return err
		}
		result.Flow = flow
		result.WakeAt = now
		return m.appendTransitionEvent(ctx, tx, flow, now, "unarchive", src)
	})
	return &result, err
}

// DelayRequest contains parameters for a Delay transition.
type DelayRequest struct {
	Days             int // delay period in days
	ExpireAfterDays  int // if > 0, updates PolicySnapshot.ExpireAfterDays
	TransitionSource     // embedded source info
}

// Delay transitions a flow to active with a future eval.
func (m *FlowManager) Delay(ctx context.Context, itemID string, req DelayRequest) (*TransitionResult, error) {
	if req.Days <= 0 {
		return nil, fmt.Errorf("days must be > 0")
	}
	now := time.Now().UTC()
	delayUntil := now.Add(time.Duration(req.Days) * 24 * time.Hour)
	var result TransitionResult
	err := m.repo.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, itemID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("flow not found: %s", itemID)
		}
		if flow.State == domain.FlowStateDeleteQueued || flow.State == domain.FlowStateDeleted || flow.State == domain.FlowStateDeleteFailed {
			return fmt.Errorf("cannot delay flow in state %s", flow.State)
		}
		if flow.State == domain.FlowStatePendingReview {
			result.FinalizePrompt = captureFinalization(&flow, "delay", req.TransitionSource)
		}
		expected := flow.Version
		flow.State = domain.FlowStateActive
		flow.HITLOutcome = "delay"
		flow.NextActionAt = delayUntil
		flow.DecisionDeadlineAt = time.Time{}
		if req.ExpireAfterDays > 0 {
			flow.PolicySnapshot.ExpireAfterDays = req.ExpireAfterDays
		}
		flow.Discord.ClearPromptLink()
		flow.UpdatedAt = now
		flow.Version = expected + 1
		if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
			return err
		}
		reason := req.evalReason("delay")
		if err := m.eval.RequestEval(ctx, tx, flow, now, delayUntil, reason, "eval:"+reason+":"+flow.ItemID, flow.Version); err != nil {
			return err
		}
		result.Flow = flow
		result.WakeAt = delayUntil
		extra := map[string]any{"days": req.Days, "delay_until": delayUntil.Format(time.RFC3339)}
		for k, v := range req.Extra {
			extra[k] = v
		}
		src := req.TransitionSource
		src.Extra = extra
		return m.appendTransitionEvent(ctx, tx, flow, now, "delay", src)
	})
	return &result, err
}

// Delete transitions a flow to delete_queued and enqueues an
// execute_delete job.
func (m *FlowManager) Delete(ctx context.Context, itemID string, src TransitionSource) (*TransitionResult, error) {
	now := time.Now().UTC()
	var result TransitionResult
	err := m.repo.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, itemID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("flow not found: %s", itemID)
		}
		if flow.State == domain.FlowStateDeleteQueued || flow.State == domain.FlowStateDeleted {
			result.Flow = flow
			return nil
		}
		if flow.State == domain.FlowStateDeleteFailed {
			return fmt.Errorf("cannot delete flow in state %s (needs resurrection first)", flow.State)
		}
		if flow.State == domain.FlowStatePendingReview {
			result.FinalizePrompt = captureFinalization(&flow, "delete", src)
		}
		expected := flow.Version
		flow.State = domain.FlowStateDeleteQueued
		flow.HITLOutcome = "delete"
		flow.NextActionAt = now
		flow.UpdatedAt = now
		flow.Version = expected + 1
		if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
			return err
		}
		requestedBy := src.Source
		if requestedBy == "" {
			requestedBy = "unknown"
		}
		payload, err := json.Marshal(jobs.ExecuteDeletePayload{RequestedBy: requestedBy})
		if err != nil {
			return err
		}
		if err := tx.EnqueueJob(ctx, domain.JobRecord{
			JobID:          "job:delete:" + flow.ItemID + ":" + strconv.FormatInt(now.UnixNano(), 10),
			FlowID:         flow.FlowID,
			ItemID:         flow.ItemID,
			Kind:           domain.JobKindExecuteDelete,
			Status:         domain.JobStatusPending,
			RunAt:          now,
			MaxAttempts:    5,
			IdempotencyKey: requestedBy + ":delete:" + flow.ItemID + ":" + strconv.FormatInt(flow.Version, 10),
			PayloadJSON:    payload,
			CreatedAt:      now,
			UpdatedAt:      now,
		}); err != nil {
			return err
		}
		result.Flow = flow
		result.WakeAt = now
		return m.appendTransitionEvent(ctx, tx, flow, now, "delete", src)
	})
	return &result, err
}

// PlayedRequest contains parameters for a Played transition.
type PlayedRequest struct {
	NextEvalAt time.Time // when to re-evaluate (lastPlay + expireDays)
	PlayedAt   time.Time // when the play occurred (for finalization display)
	TransitionSource
}

// Played transitions a flow from pending_review to active because the
// user played the media, making the HITL prompt moot.
func (m *FlowManager) Played(ctx context.Context, itemID string, req PlayedRequest) (*TransitionResult, error) {
	now := time.Now().UTC()
	var result TransitionResult
	err := m.repo.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, itemID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("flow not found: %s", itemID)
		}
		if flow.State != domain.FlowStatePendingReview {
			return fmt.Errorf("cannot apply played recovery to flow in state %s", flow.State)
		}
		result.FinalizePrompt = captureFinalization(&flow, "played", req.TransitionSource)
		expected := flow.Version
		flow.State = domain.FlowStateActive
		flow.HITLOutcome = "played"
		flow.DecisionDeadlineAt = time.Time{}
		flow.NextActionAt = req.NextEvalAt
		flow.UpdatedAt = now
		flow.Version = expected + 1
		if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
			return err
		}
		if _, err := tx.DeleteJobsForItem(ctx, flow.ItemID); err != nil {
			return err
		}
		reason := req.evalReason("playback_recovered")
		if err := m.eval.RequestEval(ctx, tx, flow, now, req.NextEvalAt, reason, "eval:"+reason+":"+flow.ItemID, flow.Version); err != nil {
			return err
		}
		result.Flow = flow
		result.WakeAt = req.NextEvalAt
		return m.appendTransitionEvent(ctx, tx, flow, now, "played", req.TransitionSource)
	})
	return &result, err
}

// RequestReview transitions a flow to pending_review and enqueues a
// send_hitl_prompt job.
func (m *FlowManager) RequestReview(ctx context.Context, itemID string, src TransitionSource) (*TransitionResult, error) {
	now := time.Now().UTC()
	var result TransitionResult
	err := m.repo.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, itemID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("flow not found: %s", itemID)
		}
		if flow.State == domain.FlowStateDeleteQueued || flow.State == domain.FlowStateDeleted || flow.State == domain.FlowStateDeleteFailed {
			return fmt.Errorf("cannot request review for flow in state %s", flow.State)
		}
		if flow.State == domain.FlowStatePendingReview {
			result.Flow = flow
			return nil
		}
		expected := flow.Version
		flow.State = domain.FlowStatePendingReview
		flow.HITLOutcome = ""
		flow.DecisionDeadlineAt = time.Time{}
		flow.NextActionAt = now
		flow.UpdatedAt = now
		if flow.CreatedAt.IsZero() {
			flow.CreatedAt = now
		}
		flow.Version = expected + 1
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
		if err := tx.EnqueueJob(ctx, domain.JobRecord{
			JobID:          "job:prompt:" + flow.ItemID + ":" + strconv.FormatInt(now.UnixNano(), 10),
			FlowID:         flow.FlowID,
			ItemID:         flow.ItemID,
			Kind:           domain.JobKindSendHITLPrompt,
			Status:         domain.JobStatusPending,
			RunAt:          now,
			MaxAttempts:    5,
			IdempotencyKey: "prompt:" + flow.ItemID + ":" + strconv.FormatInt(flow.Version, 10),
			PayloadJSON:    promptPayload,
			CreatedAt:      now,
			UpdatedAt:      now,
		}); err != nil {
			return err
		}
		result.Flow = flow
		result.WakeAt = now
		return m.appendTransitionEvent(ctx, tx, flow, now, "request_review", src)
	})
	return &result, err
}

// RollbackToActive transitions a flow from pending_review back to
// active with a cooldown eval. Used by terminal failure recovery.
func (m *FlowManager) RollbackToActive(ctx context.Context, itemID string, retryAfter time.Duration, src TransitionSource) (*TransitionResult, error) {
	now := time.Now().UTC()
	retryAt := now.Add(retryAfter)
	var result TransitionResult
	err := m.repo.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, itemID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("flow not found: %s", itemID)
		}
		if flow.State != domain.FlowStatePendingReview {
			return fmt.Errorf("cannot rollback flow in state %s (expected pending_review)", flow.State)
		}
		expected := flow.Version
		flow.State = domain.FlowStateActive
		flow.HITLOutcome = ""
		flow.DecisionDeadlineAt = time.Time{}
		flow.NextActionAt = retryAt
		flow.UpdatedAt = now
		flow.Version = expected + 1
		if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
			return err
		}
		reason := src.evalReason("recovery")
		if err := m.eval.RequestEval(ctx, tx, flow, now, retryAt, reason, "eval:"+reason+":"+flow.ItemID, flow.Version); err != nil {
			return err
		}
		result.Flow = flow
		result.WakeAt = retryAt
		return m.appendTransitionEvent(ctx, tx, flow, now, "rollback_to_active", src)
	})
	return &result, err
}

// --- internal helpers ---

func (s TransitionSource) evalReason(fallback string) string {
	if s.Reason != "" {
		return s.Reason
	}
	if s.Source != "" {
		return s.Source + "_" + fallback
	}
	return fallback
}

func (m *FlowManager) appendTransitionEvent(ctx context.Context, tx repo.TxRepository, flow domain.Flow, now time.Time, action string, src TransitionSource) error {
	eventType := action
	if src.Source != "" {
		eventType = src.Source + "." + action
	}
	payload := map[string]any{"action": action}
	for k, v := range src.Extra {
		payload[k] = v
	}
	if src.Actor != "" {
		payload["actor"] = src.Actor
	}
	return tx.AppendEvent(ctx, domain.Event{
		EventID:        "evt:" + shortHash(flow.ItemID+":"+action+":"+strconv.FormatInt(now.UnixNano(), 10)),
		FlowID:         flow.FlowID,
		ItemID:         flow.ItemID,
		Type:           eventType,
		Source:         src.Source,
		OccurredAt:     now,
		IdempotencyKey: src.Source + ":" + action + ":" + flow.ItemID + ":" + strconv.FormatInt(flow.Version, 10),
		Payload:        payload,
	})
}

func captureFinalization(flow *domain.Flow, action string, src TransitionSource) *PromptFinalization {
	ch := flow.Discord.ChannelID
	msg := flow.Discord.MessageID
	if ch == "" || msg == "" {
		return nil
	}
	display := flow.DisplayName
	if display == "" {
		display = flow.ItemID
	}
	label := actionLabel(action)
	suffix := ""
	if src.Source != "" {
		suffix = " (" + src.Source + ")"
	}
	return &PromptFinalization{
		ChannelID: ch,
		MessageID: msg,
		Content:   fmt.Sprintf("Resolved: %s for %s%s.", label, display, suffix),
	}
}

func actionLabel(action string) string {
	switch action {
	case "archive":
		return "ARCHIVED"
	case "delete":
		return "DELETE REQUESTED"
	case "delay":
		return "DELAYED"
	case "played":
		return "PLAYED"
	case "unarchive":
		return "UNARCHIVED"
	default:
		return action
	}
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}
