package jobs

import (
	"context"
	"encoding/json"
	"fmt"

	"jellyreaper/internal/domain"
)

// FlowVersion fields stamp the flow.Version observed at enqueue time. The
// handler verifies the current flow.Version still matches before acting; on
// mismatch the handler logs INFO and returns nil (stale jobs are an expected
// state, not an error). FlowVersion == 0 means "no version check" — used by
// tests and by self-defer paths that explicitly want to skip the check.
type EvaluatePolicyPayload struct {
	Reason      string `json:"reason"`
	FlowVersion int64  `json:"flow_version,omitempty"`
}

type SendHITLPromptPayload struct {
	ChannelID   string `json:"channel_id"`
	FlowVersion int64  `json:"flow_version,omitempty"`
}

type HITLTimeoutPayload struct {
	DefaultAction string `json:"default_action"`
	FlowVersion   int64  `json:"flow_version,omitempty"`
}

// ExecuteDeletePayload intentionally has no FlowVersion. Delete is
// destructive and authoritative — it does not bail on stale state.
type ExecuteDeletePayload struct {
	RequestedBy string `json:"requested_by"`
}

type VerifyDeletePayload struct {
	Attempt int `json:"attempt"`
}

type ReconcileItemPayload struct {
	Source string `json:"source"`
}

func DecodePayload[T any](job domain.JobRecord) (T, error) {
	var out T
	if len(job.PayloadJSON) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(job.PayloadJSON, &out); err != nil {
		return out, fmt.Errorf("decode payload for %s: %w", job.Kind, err)
	}
	return out, nil
}

type JobHandler interface {
	Kind() domain.JobKind
	Handle(ctx context.Context, job domain.JobRecord) error
}
