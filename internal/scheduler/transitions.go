package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/jobs"
	"jellyreaper/internal/repo"
)

// FlowManager centralizes flow state transitions. External systems
// (webhooks, Discord interactions, AI decisions) declare intent through
// these methods instead of directly mutating flow state and scheduling
// jobs. This keeps the state machine rules in one place.
//
// All transition methods operate within a caller-provided transaction.
// Side-effects that require I/O (e.g. Discord prompt finalization) are
// returned in TransitionResult for the caller to execute after commit.
type FlowManager struct {
	eval   EvalRequester
	logger *slog.Logger
}

func NewFlowManager(eval EvalRequester, logger *slog.Logger) *FlowManager {
	if logger == nil {
		logger = slog.Default()
	}
	if eval == nil {
		eval = NewScheduler(nil, nil)
	}
	return &FlowManager{eval: eval, logger: logger}
}

// TransitionResult captures side-effects that must happen after the tx
// commits. Callers inspect this and perform Discord I/O, wake signals, etc.
type TransitionResult struct {
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

// Archive transitions a flow to archived state.
func (m *FlowManager) Archive(ctx context.Context, tx repo.TxRepository, flow *domain.Flow, now time.Time) (*TransitionResult, error) {
	if flow.State == domain.FlowStateArchived {
		return &TransitionResult{}, nil // idempotent
	}
	if flow.State == domain.FlowStateDeleteQueued || flow.State == domain.FlowStateDeleted || flow.State == domain.FlowStateDeleteFailed {
		return nil, fmt.Errorf("cannot archive flow in state %s", flow.State)
	}
	result := &TransitionResult{}
	if flow.State == domain.FlowStatePendingReview {
		result.FinalizePrompt = captureFinalization(flow, "archive")
	}
	expected := flow.Version
	flow.State = domain.FlowStateArchived
	flow.HITLOutcome = "archive"
	flow.NextActionAt = time.Time{}
	flow.DecisionDeadlineAt = time.Time{}
	flow.UpdatedAt = now
	flow.Version = expected + 1
	if err := tx.UpsertFlowCAS(ctx, *flow, expected); err != nil {
		return nil, err
	}
	return result, nil
}

// Unarchive transitions a flow from archived back to active and
// schedules an immediate eval.
func (m *FlowManager) Unarchive(ctx context.Context, tx repo.TxRepository, flow *domain.Flow, now time.Time, evalReason string) (*TransitionResult, error) {
	if flow.State != domain.FlowStateArchived {
		return nil, fmt.Errorf("cannot unarchive flow in state %s", flow.State)
	}
	expected := flow.Version
	flow.State = domain.FlowStateActive
	flow.HITLOutcome = ""
	flow.DecisionDeadlineAt = time.Time{}
	flow.NextActionAt = now
	flow.Discord.ClearPromptLink()
	flow.UpdatedAt = now
	flow.Version = expected + 1
	if err := tx.UpsertFlowCAS(ctx, *flow, expected); err != nil {
		return nil, err
	}
	if err := m.eval.RequestEval(ctx, tx, *flow, now, now, evalReason, "eval:"+evalReason+":"+flow.ItemID, flow.Version); err != nil {
		return nil, err
	}
	return &TransitionResult{WakeAt: now}, nil
}

// Delay transitions a flow from pending_review (or active) to active
// with a future eval. Callers should set any PolicySnapshot fields
// (e.g. ExpireAfterDays) on the flow before calling this method, as
// Delay performs the CAS write.
func (m *FlowManager) Delay(ctx context.Context, tx repo.TxRepository, flow *domain.Flow, now time.Time, delayUntil time.Time, evalReason string) (*TransitionResult, error) {
	if flow.State == domain.FlowStateDeleteQueued || flow.State == domain.FlowStateDeleted || flow.State == domain.FlowStateDeleteFailed {
		return nil, fmt.Errorf("cannot delay flow in state %s", flow.State)
	}
	result := &TransitionResult{WakeAt: delayUntil}
	if flow.State == domain.FlowStatePendingReview {
		result.FinalizePrompt = captureFinalization(flow, "delay")
	}
	expected := flow.Version
	flow.State = domain.FlowStateActive
	flow.HITLOutcome = "delay"
	flow.NextActionAt = delayUntil
	flow.DecisionDeadlineAt = time.Time{}
	flow.Discord.ClearPromptLink()
	flow.UpdatedAt = now
	flow.Version = expected + 1
	if err := tx.UpsertFlowCAS(ctx, *flow, expected); err != nil {
		return nil, err
	}
	if err := m.eval.RequestEval(ctx, tx, *flow, now, delayUntil, evalReason, "eval:"+evalReason+":"+flow.ItemID, flow.Version); err != nil {
		return nil, err
	}
	return result, nil
}

// Delete transitions a flow to delete_queued and enqueues an
// execute_delete job.
func (m *FlowManager) Delete(ctx context.Context, tx repo.TxRepository, flow *domain.Flow, now time.Time, requestedBy string) (*TransitionResult, error) {
	if flow.State == domain.FlowStateDeleteQueued || flow.State == domain.FlowStateDeleted {
		return &TransitionResult{}, nil // idempotent
	}
	if flow.State == domain.FlowStateDeleteFailed {
		return nil, fmt.Errorf("cannot delete flow in state %s (needs resurrection first)", flow.State)
	}
	result := &TransitionResult{WakeAt: now}
	if flow.State == domain.FlowStatePendingReview {
		result.FinalizePrompt = captureFinalization(flow, "delete")
	}
	expected := flow.Version
	flow.State = domain.FlowStateDeleteQueued
	flow.HITLOutcome = "delete"
	flow.NextActionAt = now
	flow.UpdatedAt = now
	flow.Version = expected + 1
	if err := tx.UpsertFlowCAS(ctx, *flow, expected); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(jobs.ExecuteDeletePayload{RequestedBy: requestedBy})
	if err != nil {
		return nil, err
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
		return nil, err
	}
	return result, nil
}

// Played transitions a flow from pending_review to active because the
// user played the media, making the HITL prompt moot. Purges all stale
// jobs and re-schedules the singleton eval.
func (m *FlowManager) Played(ctx context.Context, tx repo.TxRepository, flow *domain.Flow, now time.Time, nextEvalAt time.Time, evalIdempotencyKey string) (*TransitionResult, error) {
	if flow.State != domain.FlowStatePendingReview {
		return nil, fmt.Errorf("cannot apply played recovery to flow in state %s", flow.State)
	}
	result := &TransitionResult{WakeAt: nextEvalAt}
	result.FinalizePrompt = captureFinalization(flow, "played")
	expected := flow.Version
	flow.State = domain.FlowStateActive
	flow.HITLOutcome = "played"
	flow.DecisionDeadlineAt = time.Time{}
	flow.NextActionAt = nextEvalAt
	flow.UpdatedAt = now
	flow.Version = expected + 1
	if err := tx.UpsertFlowCAS(ctx, *flow, expected); err != nil {
		return nil, err
	}
	// Purge the entire stale HITL job chain (prompt, timeout, eval
	// singleton). RequestEval immediately recreates the singleton.
	if _, err := tx.DeleteJobsForItem(ctx, flow.ItemID); err != nil {
		return nil, err
	}
	if err := m.eval.RequestEval(ctx, tx, *flow, now, nextEvalAt, "playback_recovered", evalIdempotencyKey, flow.Version); err != nil {
		return nil, err
	}
	return result, nil
}

// RequestReview transitions a flow to pending_review and enqueues a
// send_hitl_prompt job so a human can act on it immediately.
func (m *FlowManager) RequestReview(ctx context.Context, tx repo.TxRepository, flow *domain.Flow, now time.Time) (*TransitionResult, error) {
	if flow.State == domain.FlowStateDeleteQueued || flow.State == domain.FlowStateDeleted || flow.State == domain.FlowStateDeleteFailed {
		return nil, fmt.Errorf("cannot request review for flow in state %s", flow.State)
	}
	if flow.State == domain.FlowStatePendingReview {
		return &TransitionResult{}, nil // already pending
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
	if err := tx.UpsertFlowCAS(ctx, *flow, expected); err != nil {
		return nil, err
	}
	promptPayload, err := json.Marshal(jobs.SendHITLPromptPayload{
		ChannelID:   flow.Discord.ChannelID,
		FlowVersion: flow.Version,
	})
	if err != nil {
		return nil, err
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
		return nil, err
	}
	return &TransitionResult{WakeAt: now}, nil
}

// RollbackToActive transitions a flow from pending_review back to
// active and re-schedules the singleton eval. Used by handler
// OnTerminalFailure recovery paths when a HITL prompt or similar job
// exhausts retries.
func (m *FlowManager) RollbackToActive(ctx context.Context, tx repo.TxRepository, flow *domain.Flow, now time.Time, retryAt time.Time, evalReason string) (*TransitionResult, error) {
	if flow.State != domain.FlowStatePendingReview {
		return nil, fmt.Errorf("cannot rollback flow in state %s (expected pending_review)", flow.State)
	}
	expected := flow.Version
	flow.State = domain.FlowStateActive
	flow.HITLOutcome = ""
	flow.DecisionDeadlineAt = time.Time{}
	flow.NextActionAt = retryAt
	flow.UpdatedAt = now
	flow.Version = expected + 1
	if err := tx.UpsertFlowCAS(ctx, *flow, expected); err != nil {
		return nil, err
	}
	if err := m.eval.RequestEval(ctx, tx, *flow, now, retryAt, evalReason, "eval:"+evalReason+":"+flow.ItemID, flow.Version); err != nil {
		return nil, err
	}
	return &TransitionResult{WakeAt: retryAt}, nil
}

func captureFinalization(flow *domain.Flow, action string) *PromptFinalization {
	ch := flow.Discord.ChannelID
	msg := flow.Discord.MessageID
	if ch == "" || msg == "" {
		return nil
	}
	display := flow.DisplayName
	if display == "" {
		display = flow.ItemID
	}
	var content string
	switch action {
	case "archive":
		content = fmt.Sprintf("Resolved: ARCHIVED for %s.", display)
	case "delete":
		content = fmt.Sprintf("Resolved: DELETE REQUESTED for %s.", display)
	case "delay":
		content = fmt.Sprintf("Resolved: DELAYED for %s.", display)
	case "played":
		content = fmt.Sprintf("Resolved: PLAYED for %s.", display)
	default:
		content = fmt.Sprintf("Resolved: %s for %s.", action, display)
	}
	return &PromptFinalization{ChannelID: ch, MessageID: msg, Content: content}
}
