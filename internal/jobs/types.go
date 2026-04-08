package jobs

import (
	"context"
	"encoding/json"
	"fmt"

	"jellyreaper/internal/domain"
)

type EvaluatePolicyPayload struct {
	Reason string `json:"reason"`
}

type SendHITLPromptPayload struct {
	ChannelID string `json:"channel_id"`
}

type HITLTimeoutPayload struct {
	DefaultAction string `json:"default_action"`
}

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
