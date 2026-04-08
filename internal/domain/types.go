package domain

import "time"

type FlowState string

const (
	FlowStateActive           FlowState = "active"
	FlowStatePendingReview    FlowState = "pending_review"
	FlowStateDeleteQueued     FlowState = "delete_queued"
	FlowStateDeleteInProgress FlowState = "delete_in_progress"
	FlowStateDeleted          FlowState = "deleted"
	FlowStateArchived         FlowState = "archived"
	FlowStateDeleteFailed     FlowState = "delete_failed"
)

type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusLeased    JobStatus = "leased"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
	JobStatusCanceled  JobStatus = "canceled"
)

type JobKind string

const (
	JobKindEvaluatePolicy JobKind = "evaluate_policy"
	JobKindSendHITLPrompt JobKind = "send_hitl_prompt"
	JobKindHITLTimeout    JobKind = "hitl_timeout"
	JobKindExecuteDelete  JobKind = "execute_delete"
	JobKindVerifyDelete   JobKind = "verify_delete"
	JobKindReconcileItem  JobKind = "reconcile_item"
)

type MediaItem struct {
	ItemID              string    `json:"item_id"`
	Name                string    `json:"name"`
	ItemType            string    `json:"item_type"`
	SeasonID            string    `json:"season_id"`
	SeasonName          string    `json:"season_name"`
	SeriesID            string    `json:"series_id"`
	SeriesName          string    `json:"series_name"`
	ImageURL            string    `json:"image_url"`
	LibraryID           string    `json:"library_id"`
	Title               string    `json:"title"`
	Path                string    `json:"path"`
	SizeBytes           int64     `json:"size_bytes"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
	LastCatalogEventAt  time.Time `json:"last_catalog_event_at"`
	LastPlaybackEventAt time.Time `json:"last_playback_event_at"`
	LastPlayedAt        time.Time `json:"last_played_at"`
	PlayCountTotal      int64     `json:"play_count_total"`
	LastUserID          string    `json:"last_user_id"`
	IsDeletedInJellyfin bool      `json:"is_deleted_in_jellyfin"`
	IsArchived          bool      `json:"is_archived"`
}

type PolicySnapshot struct {
	ExpireAfterDays int    `json:"expire_after_days"`
	HITLTimeoutHrs  int    `json:"hitl_timeout_hours"`
	TimeoutAction   string `json:"timeout_action"`
}

type DiscordContext struct {
	ChannelID         string `json:"channel_id"`
	MessageID         string `json:"message_id"`
	PreviousChannelID string `json:"previous_channel_id"`
	PreviousMessageID string `json:"previous_message_id"`
	ThreadID          string `json:"thread_id"`
}

type Flow struct {
	FlowID             string         `json:"flow_id"`
	ItemID             string         `json:"item_id"`
	SubjectType        string         `json:"subject_type"`
	DisplayName        string         `json:"display_name"`
	ImageURL           string         `json:"image_url"`
	State              FlowState      `json:"state"`
	Version            int64          `json:"version"`
	PolicySnapshot     PolicySnapshot `json:"policy_snapshot"`
	NextActionAt       time.Time      `json:"next_action_at"`
	DecisionDeadlineAt time.Time      `json:"decision_deadline_at"`
	Discord            DiscordContext `json:"discord"`
	LastError          string         `json:"last_error"`
	HITLOutcome        string         `json:"hitl_outcome"`
	LastCatalogEventAt time.Time      `json:"last_catalog_event_at"`
	CreatedAt          time.Time      `json:"created_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
}

type Event struct {
	EventID        string         `json:"event_id"`
	FlowID         string         `json:"flow_id"`
	ItemID         string         `json:"item_id"`
	Type           string         `json:"type"`
	Source         string         `json:"source"`
	OccurredAt     time.Time      `json:"occurred_at"`
	IdempotencyKey string         `json:"idempotency_key"`
	Payload        map[string]any `json:"payload"`
}

type JobRecord struct {
	JobID          string    `json:"job_id"`
	FlowID         string    `json:"flow_id"`
	ItemID         string    `json:"item_id"`
	Kind           JobKind   `json:"kind"`
	Status         JobStatus `json:"status"`
	RunAt          time.Time `json:"run_at"`
	LeaseOwner     string    `json:"lease_owner"`
	LeaseUntil     time.Time `json:"lease_until"`
	Attempts       int       `json:"attempts"`
	MaxAttempts    int       `json:"max_attempts"`
	IdempotencyKey string    `json:"idempotency_key"`
	PayloadJSON    []byte    `json:"payload_json"`
	LastError      string    `json:"last_error"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}
