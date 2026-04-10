package scheduler

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
	now    func() time.Time
}

func NewFlowManager(repository repo.Repository, eval EvalRequester, logger *slog.Logger) *FlowManager {
	if logger == nil {
		logger = slog.Default()
	}
	if eval == nil {
		eval = NewScheduler(nil, nil)
	}
	return &FlowManager{repo: repository, eval: eval, logger: logger, now: func() time.Time { return time.Now().UTC() }}
}

// SetNowFunc overrides the clock for testing.
func (m *FlowManager) SetNowFunc(fn func() time.Time) {
	m.now = fn
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
	now := m.now()
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
	now := m.now()
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
	now := m.now()
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
			if result.FinalizePrompt != nil {
				display := flow.DisplayName
				if display == "" {
					display = flow.ItemID
				}
				suffix := ""
				if req.Source != "" {
					suffix = " (" + strings.ToUpper(req.Source) + ")"
				}
				result.FinalizePrompt.Content = fmt.Sprintf("Resolved: DELAYED %d days for %s%s.", req.Days, display, suffix)
			}
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
	now := m.now()
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
	NextEvalAt time.Time // if set, overrides the auto-computed next eval time
	TransitionSource
}

// Played transitions a flow from pending_review to active because the
// user played the media, making the HITL prompt moot. Computes the next
// eval time from the flow's media play history and policy unless
// NextEvalAt is explicitly set in the request.
func (m *FlowManager) Played(ctx context.Context, itemID string, req PlayedRequest) (*TransitionResult, error) {
	now := m.now()
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

		nextEvalAt := req.NextEvalAt
		if nextEvalAt.IsZero() {
			nextEvalAt = m.computeNextEval(ctx, tx, flow, now)
		}

		result.FinalizePrompt = captureFinalization(&flow, "played", req.TransitionSource)
		// Enrich with the actual play time for a more informative Discord message.
		if result.FinalizePrompt != nil {
			playedAt := m.latestPlayTime(ctx, tx, flow)
			if !playedAt.IsZero() {
				display := flow.DisplayName
				if display == "" {
					display = flow.ItemID
				}
				suffix := ""
				if req.Source != "" {
					suffix = " (" + strings.ToUpper(req.Source) + ")"
				}
				result.FinalizePrompt.Content = fmt.Sprintf("Resolved: PLAYED at %s for %s%s.", playedAt.UTC().Format("2006-01-02 15:04 UTC"), display, suffix)
			}
		}
		expected := flow.Version
		flow.State = domain.FlowStateActive
		flow.HITLOutcome = "played"
		flow.DecisionDeadlineAt = time.Time{}
		flow.NextActionAt = nextEvalAt
		flow.UpdatedAt = now
		flow.Version = expected + 1
		if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
			return err
		}
		if _, err := tx.DeleteJobsForItem(ctx, flow.ItemID); err != nil {
			return err
		}
		reason := req.evalReason("playback_recovered")
		if err := m.eval.RequestEval(ctx, tx, flow, now, nextEvalAt, reason, "eval:"+reason+":"+flow.ItemID, flow.Version); err != nil {
			return err
		}
		result.Flow = flow
		result.WakeAt = nextEvalAt
		return m.appendTransitionEvent(ctx, tx, flow, now, "played", req.TransitionSource)
	})
	return &result, err
}

// computeNextEval determines when the next eval should run based on
// the flow's media play history and policy snapshot.
func (m *FlowManager) computeNextEval(ctx context.Context, tx repo.TxRepository, flow domain.Flow, now time.Time) time.Time {
	expireDays := flow.PolicySnapshot.ExpireAfterDays
	if expireDays <= 0 {
		expireDays = 30
	}
	parts := strings.SplitN(flow.ItemID, ":", 3)
	if len(parts) == 3 && parts[0] == "target" {
		media, err := tx.ListMediaBySubject(ctx, parts[1], parts[2])
		if err == nil {
			var latest time.Time
			for _, item := range media {
				if item.LastPlayedAt.After(latest) {
					latest = item.LastPlayedAt
				}
			}
			if !latest.IsZero() {
				dueAt := latest.Add(time.Duration(expireDays) * 24 * time.Hour)
				if dueAt.After(now) {
					return dueAt
				}
			}
		}
	}
	return now
}

// RequestReview transitions a flow to pending_review and enqueues a
// send_hitl_prompt job.
func (m *FlowManager) RequestReview(ctx context.Context, itemID string, src TransitionSource) (*TransitionResult, error) {
	now := m.now()
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
	now := m.now()
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
		suffix = " (" + strings.ToUpper(src.Source) + ")"
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
		return "DELAYED" // caller may override with more specific content
	case "played":
		return "PLAYED"
	case "unarchive":
		return "UNARCHIVED"
	default:
		return action
	}
}

func (m *FlowManager) latestPlayTime(ctx context.Context, tx repo.TxRepository, flow domain.Flow) time.Time {
	parts := strings.SplitN(flow.ItemID, ":", 3)
	if len(parts) != 3 || parts[0] != "target" {
		return time.Time{}
	}
	media, err := tx.ListMediaBySubject(ctx, parts[1], parts[2])
	if err != nil {
		return time.Time{}
	}
	var latest time.Time
	for _, item := range media {
		if item.LastPlayedAt.After(latest) {
			latest = item.LastPlayedAt
		}
	}
	return latest
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}
