package ai

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/openai/openai-go/responses"
	"github.com/openai/openai-go/shared"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/repo"
)

const (
	historyLineCapacity = 20
	maxThreadContexts   = 256

	storageSkill = "Domain skill: The core object is a projection flow (`target:<subject_type>:<subject_id>`). Think of flows as user-visible review entries. `subject_type=movie` is one movie; `subject_type=season` is one TV season projection (episodes grouped by season). States: `pending_review` means ready now, `active` means scheduled for later, `archived` means hidden from normal review, `delete_queued` means deletion is in progress/queued. User wording mapping: 'next up for review' means first ready flow ordered by next action time; 'how many movies' means count projection flows with `subject_type=movie`; 'how many TV shows' means distinct series names represented by season projection flows. Operational actions are on projection flows only (movie/season), never on raw media-item IDs."
)

type Harness struct {
	repository repo.Repository
	client     openai.Client
	model      string
	logger     *slog.Logger
	decision   DecisionService

	mu              sync.Mutex
	history         map[string]*ringBuffer
	state           map[string]threadState
	historyRestorer func(context.Context, string, int) ([]string, error)
	maxThreads      int
	threadLRU       *list.List
	threadIndex     map[string]*list.Element
}

type DecisionService interface {
	ApplyAIDecision(context.Context, string, string) error
	ApplyAIDecisionBatch(context.Context, []string, string) error
	ApplyAIDelayDays(context.Context, string, int) error
	SetGlobalReviewDays(context.Context, int) error
	SetGlobalDeferDays(context.Context, int) error
	SetGlobalHITLTimeoutHours(context.Context, int) error
}

type ringBuffer struct {
	data  []string
	start int
	size  int
}

func newRingBuffer(capacity int) *ringBuffer {
	if capacity <= 0 {
		capacity = 1
	}
	return &ringBuffer{data: make([]string, capacity)}
}

func (r *ringBuffer) append(value string) {
	if len(r.data) == 0 {
		return
	}
	if r.size < len(r.data) {
		idx := (r.start + r.size) % len(r.data)
		r.data[idx] = value
		r.size++
		return
	}
	r.data[r.start] = value
	r.start = (r.start + 1) % len(r.data)
}

func (r *ringBuffer) snapshot() []string {
	out := make([]string, 0, r.size)
	for i := 0; i < r.size; i++ {
		idx := (r.start + i) % len(r.data)
		out = append(out, r.data[idx])
	}
	return out
}

type threadState struct {
	SelectedTargetID string
	CandidateIDs     []string
	PendingAction    string
	AliasToItemID    map[string]string
}

type intent struct {
	Intent string `json:"intent"`
	Query  string `json:"query"`
	Days   int    `json:"days"`
}

func NewHarness(repository repo.Repository, apiKey string, model string) *Harness {
	if strings.TrimSpace(model) == "" {
		model = "gpt-5-mini"
	}
	return &Harness{
		repository:  repository,
		client:      openai.NewClient(option.WithAPIKey(strings.TrimSpace(apiKey))),
		model:       model,
		logger:      slog.Default(),
		history:     map[string]*ringBuffer{},
		state:       map[string]threadState{},
		maxThreads:  maxThreadContexts,
		threadLRU:   list.New(),
		threadIndex: map[string]*list.Element{},
	}
}

func (h *Harness) SetHistoryRestorer(restorer func(context.Context, string, int) ([]string, error)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.historyRestorer = restorer
}

func (h *Harness) SetDecisionService(decision DecisionService) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.decision = decision
}

func (h *Harness) SetMaxThreadContexts(max int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if max <= 0 {
		max = 1
	}
	h.maxThreads = max
	h.pruneThreadContextsLocked()
}

func (h *Harness) HandleMention(ctx context.Context, threadID string, userName string, input string) (string, error) {
	h.touchThread(threadID)
	out, err := h.respondBestEffort(ctx, threadID, userName, input)
	if err != nil {
		h.logger.Error("ai mention processing failed", "thread_id", threadID, "user", userName, "error", err)
		return fmt.Sprintf("I couldn't complete that because the AI backend returned an error: `%s`", summarizeAIError(err)), nil
	}

	h.appendHistory(threadID, userName+": "+input)
	h.appendHistory(threadID, "assistant: "+out)
	return out, nil
}

func summarizeAIError(err error) string {
	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return "unknown error"
	}
	msg = strings.ReplaceAll(msg, "\n", " ")
	if len(msg) > 220 {
		msg = msg[:220] + "..."
	}
	return msg
}

func (h *Harness) respondBestEffort(ctx context.Context, threadID string, userName string, input string) (string, error) {
	h.ensureHistoryLoaded(ctx, threadID)
	history := h.getHistory(threadID)
	state := h.getThreadState(threadID)
	threadContext := h.serializeThreadContext(ctx, state)

	system := strings.Join([]string{
		"You are JellyReaper, a best-effort assistant for one media server.",
		"Only handle requests related to this media server and its review/archive workflow.",
		storageSkill,
		"Use tools to gather data and take actions.",
		"Tool usage guide: list_ready for queue overviews and next-up questions; query_library for generalized counting/filtering across flows and indexed media; fuzzy_search_targets for discovery; query_target_state to inspect one target; remember_alias to bind user phrases to a target; archive_target for archive/unarchive intents; schedule_delete for deletion scheduling on one projection target; schedule_delete_many for show-level intents mapped to multiple season targets; delay_target_days to defer a target by an explicit number of days; choose_candidate when user provides a numbered option; confirm_pending for yes/no confirmations; set_review_days, set_defer_days, and set_hitl_timeout_hours for policy tuning.",
		"Flow semantics: A flow is the schedulable review record. A projection is the user-facing target represented by the flow. For TV, one season flow maps many episode media items. Media items are index records used for context and counting, not direct operation targets.",
		"Tool-calling playbook: (1) informational/count question -> query_library; (2) next-up/review queue question -> list_ready (limit=1 for singular next item); (3) find candidate titles -> fuzzy_search_targets then query_target_state for one target; (4) mutate flow state (archive/delete/delay/policy) only after target is resolved and confirmed when required.",
		"Alias behavior: after resolving an ambiguous title or when user uses a shorthand phrase, call remember_alias to store that phrase for future turns. Prefer aliases first on follow-up requests.",
		"Disambiguation behavior: when multiple targets match, use choose_candidate path and keep options short (title + season/show context).",
		"Use schedule_delete for deletion requests; rely on domain logic for item vs projection delete semantics.",
		"When asked to change how long HITL waits before timeout, use set_hitl_timeout_hours.",
		"When asked 'what is next up for review', call list_ready with limit=1.",
		"When asked for counts, do not guess; call query_library first.",
		"Natural-language target resolution guide: prefer known aliases from thread memory first, then exact title, then partial title, and ask clarifying follow-up when ambiguous.",
		"When multiple targets are possible, present concise choices and invite a short follow-up reply (for example: `title a`, `title b`, or a clearer title).",
		"When you reply, use Discord markdown and keep it conversational and concise.",
		"Ask follow-up questions when needed to resolve ambiguity or confirm intent.",
		"You may reply directly in assistant text, or call response; either is acceptable.",
		"Never leak internal identifiers (item IDs, flow IDs, job IDs, event IDs).",
		"Use human-readable wording: movie/show names, season context when available, and relative time.",
		"The response message must be Discord markdown and user-friendly.",
	}, " ")
	userPrompt := "Thread context:\n" + threadContext + "\n" +
		"Recent context:\n" + strings.Join(history, "\n") + "\n" +
		"User " + userName + ": " + input

	tools := []responses.ToolUnionParam{
		{OfFunction: &responses.FunctionToolParam{Name: "response", Description: openai.String("Send a Discord markdown response back to the user."), Parameters: map[string]any{"type": "object", "properties": map[string]any{"message": map[string]any{"type": "string"}}, "required": []string{"message"}}}},
		{OfFunction: &responses.FunctionToolParam{Name: "list_ready", Description: openai.String("List targets currently ready for review."), Parameters: map[string]any{"type": "object", "properties": map[string]any{"limit": map[string]any{"type": "integer", "minimum": 1, "maximum": 20}}}}},
		{OfFunction: &responses.FunctionToolParam{Name: "query_library", Description: openai.String("Generalized query over projection flows and indexed media. Use this for counts and lightweight discovery."), Parameters: map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}, "scope": map[string]any{"type": "string", "enum": []string{"", "flows", "media", "both"}}, "subject_type": map[string]any{"type": "string", "enum": []string{"", "movie", "season", "episode"}}, "limit": map[string]any{"type": "integer", "minimum": 1, "maximum": 50}}}}},
		{OfFunction: &responses.FunctionToolParam{Name: "fuzzy_search_targets", Description: openai.String("Find projection targets by fuzzy title/series/season text, optionally filtered by subject type."), Parameters: map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}, "subject_type": map[string]any{"type": "string", "enum": []string{"", "movie", "season"}}, "limit": map[string]any{"type": "integer", "minimum": 1, "maximum": 30}}, "required": []string{"query"}}}},
		{OfFunction: &responses.FunctionToolParam{Name: "query_target_state", Description: openai.String("Inspect state for a target by title text, or current selection when query omitted."), Parameters: map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}}}}},
		{OfFunction: &responses.FunctionToolParam{Name: "remember_alias", Description: openai.String("Store a natural-language alias for a specific target so future follow-ups can resolve quickly."), Parameters: map[string]any{"type": "object", "properties": map[string]any{"alias": map[string]any{"type": "string"}, "target_ref": map[string]any{"type": "string"}}, "required": []string{"alias"}}}},
		{OfFunction: &responses.FunctionToolParam{Name: "schedule_delete", Description: openai.String("Schedule deletion for operational targets (movie/season) by title text, alias, or target id."), Parameters: map[string]any{"type": "object", "properties": map[string]any{"target_ref": map[string]any{"type": "string"}}}}},
		{OfFunction: &responses.FunctionToolParam{Name: "schedule_delete_many", Description: openai.String("Schedule deletion for many projection targets at once (useful for deleting all seasons in a show)."), Parameters: map[string]any{"type": "object", "properties": map[string]any{"target_ids": map[string]any{"type": "array", "items": map[string]any{"type": "string"}}}, "required": []string{"target_ids"}}}},
		{OfFunction: &responses.FunctionToolParam{Name: "delay_target_days", Description: openai.String("Delay a projection target by a specific number of days by updating flow policy/schedule."), Parameters: map[string]any{"type": "object", "properties": map[string]any{"target_ref": map[string]any{"type": "string"}, "days": map[string]any{"type": "integer", "minimum": 1, "maximum": 3650}}, "required": []string{"days"}}}},
		{OfFunction: &responses.FunctionToolParam{Name: "archive_target", Description: openai.String("Start archive or unarchive flow for a target title."), Parameters: map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}, "archived": map[string]any{"type": "boolean"}}, "required": []string{"archived"}}}},
		{OfFunction: &responses.FunctionToolParam{Name: "choose_candidate", Description: openai.String("Choose one of the numbered candidates from the last disambiguation list."), Parameters: map[string]any{"type": "object", "properties": map[string]any{"number": map[string]any{"type": "integer", "minimum": 1}}, "required": []string{"number"}}}},
		{OfFunction: &responses.FunctionToolParam{Name: "confirm_pending", Description: openai.String("Confirm or cancel the pending archive/unarchive action."), Parameters: map[string]any{"type": "object", "properties": map[string]any{"confirmed": map[string]any{"type": "boolean"}}, "required": []string{"confirmed"}}}},
		{OfFunction: &responses.FunctionToolParam{Name: "set_review_days", Description: openai.String("Set policy review period in days for active flows."), Parameters: map[string]any{"type": "object", "properties": map[string]any{"days": map[string]any{"type": "integer", "minimum": 1, "maximum": 3650}}, "required": []string{"days"}}}},
		{OfFunction: &responses.FunctionToolParam{Name: "set_defer_days", Description: openai.String("Set default defer period in days."), Parameters: map[string]any{"type": "object", "properties": map[string]any{"days": map[string]any{"type": "integer", "minimum": 1, "maximum": 365}}, "required": []string{"days"}}}},
		{OfFunction: &responses.FunctionToolParam{Name: "set_hitl_timeout_hours", Description: openai.String("Set default HITL review timeout in hours before timeout action."), Parameters: map[string]any{"type": "object", "properties": map[string]any{"hours": map[string]any{"type": "integer", "minimum": 1, "maximum": 8760}}, "required": []string{"hours"}}}},
	}

	previousResponseID := ""
	pendingInput := []responses.ResponseInputItemUnionParam{responses.ResponseInputItemParamOfMessage(userPrompt, responses.EasyInputMessageRoleUser)}

	for i := 0; i < 6; i++ {
		params := responses.ResponseNewParams{
			Model:        shared.ResponsesModel(h.model),
			Instructions: openai.String(system),
			Tools:        tools,
			Input: responses.ResponseNewParamsInputUnion{
				OfInputItemList: pendingInput,
			},
		}
		if strings.TrimSpace(previousResponseID) != "" {
			params.PreviousResponseID = openai.String(previousResponseID)
		}
		resp, err := h.client.Responses.New(ctx, params)
		if err != nil {
			return "", err
		}
		previousResponseID = resp.ID

		toolCalls := make([]responses.ResponseFunctionToolCall, 0)
		for _, item := range resp.Output {
			if item.Type == "function_call" {
				toolCalls = append(toolCalls, item.AsFunctionCall())
			}
		}

		if len(toolCalls) == 0 {
			content := strings.TrimSpace(resp.OutputText())
			if content != "" {
				return content, nil
			}
			pendingInput = []responses.ResponseInputItemUnionParam{responses.ResponseInputItemParamOfMessage("Please respond with a concise Discord-markdown message for the user.", responses.EasyInputMessageRoleUser)}
			continue
		}

		toolOutputs := make([]responses.ResponseInputItemUnionParam, 0, len(toolCalls))
		for _, tc := range toolCalls {
			result, out, err := h.executeToolCall(ctx, threadID, tc.Name, tc.Arguments)
			if err != nil {
				result = "tool error: " + err.Error()
			}
			toolOutputs = append(toolOutputs, responses.ResponseInputItemParamOfFunctionCallOutput(tc.CallID, result))
			if strings.TrimSpace(out) != "" {
				return out, nil
			}
		}
		pendingInput = toolOutputs
	}

	return "I couldn't complete that yet. Could you share a bit more context and try again?", nil
}

func (h *Harness) ensureHistoryLoaded(ctx context.Context, threadID string) {
	if strings.TrimSpace(threadID) == "" {
		return
	}
	h.mu.Lock()
	if _, ok := h.history[threadID]; ok {
		h.touchThreadLocked(threadID)
		h.mu.Unlock()
		return
	}
	restorer := h.historyRestorer
	h.mu.Unlock()
	if restorer == nil {
		return
	}
	lines, err := restorer(ctx, threadID, historyLineCapacity)
	if err != nil || len(lines) == 0 {
		return
	}
	b := newRingBuffer(historyLineCapacity)
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		b.append(trimmed)
	}
	h.mu.Lock()
	if _, ok := h.history[threadID]; !ok {
		h.history[threadID] = b
		h.touchThreadLocked(threadID)
		h.pruneThreadContextsLocked()
	}
	h.mu.Unlock()
}

func (h *Harness) executeToolCall(ctx context.Context, threadID string, toolName string, rawArguments string) (string, string, error) {
	switch toolName {
	case "response":
		args, err := decodeToolArgs[toolResponseArgs](rawArguments)
		if err != nil {
			return "", "", err
		}
		msg := strings.TrimSpace(args.Message)
		if msg == "" {
			msg = "Could you share a little more detail so I can help?"
		}
		return `{"ok":true}`, msg, nil
	case "list_ready":
		args, err := decodeToolArgs[toolListReadyArgs](rawArguments)
		if err != nil {
			return "", "", err
		}
		return h.listReady(ctx, args.Limit)
	case "query_library":
		args, err := decodeToolArgs[toolQueryLibraryArgs](rawArguments)
		if err != nil {
			return "", "", err
		}
		if args.Limit <= 0 {
			args.Limit = 12
		}
		return h.queryLibrary(ctx, args.Query, args.Scope, args.SubjectType, args.Limit)
	case "fuzzy_search_targets":
		args, err := decodeToolArgs[toolFuzzySearchArgs](rawArguments)
		if err != nil {
			return "", "", err
		}
		limit := args.Limit
		if limit <= 0 {
			limit = 12
		}
		return h.fuzzySearchTargets(ctx, args.Query, args.SubjectType, limit)
	case "query_target_state":
		args, err := decodeToolArgs[toolQueryTargetStateArgs](rawArguments)
		if err != nil {
			return "", "", err
		}
		return h.queryTargetState(ctx, threadID, args.Query)
	case "remember_alias":
		args, err := decodeToolArgs[toolRememberAliasArgs](rawArguments)
		if err != nil {
			return "", "", err
		}
		return h.rememberAlias(ctx, threadID, args.Alias, args.TargetRef)
	case "schedule_delete":
		args, err := decodeToolArgs[toolScheduleDeleteArgs](rawArguments)
		if err != nil {
			return "", "", err
		}
		return h.setDeleteState(ctx, threadID, args.TargetRef)
	case "schedule_delete_many":
		args, err := decodeToolArgs[toolScheduleDeleteManyArgs](rawArguments)
		if err != nil {
			return "", "", err
		}
		return h.scheduleDeleteMany(ctx, args.TargetIDs)
	case "delay_target_days":
		args, err := decodeToolArgs[toolDelayTargetDaysArgs](rawArguments)
		if err != nil {
			return "", "", err
		}
		return h.delayTargetDays(ctx, threadID, args.TargetRef, args.Days)
	case "archive_target":
		args, err := decodeToolArgs[toolArchiveTargetArgs](rawArguments)
		if err != nil {
			return "", "", err
		}
		archived := true
		if args.Archived != nil {
			archived = *args.Archived
		}
		return h.setArchiveState(ctx, threadID, args.Query, archived)
	case "choose_candidate":
		args, err := decodeToolArgs[toolChooseCandidateArgs](rawArguments)
		if err != nil {
			return "", "", err
		}
		n := args.Number
		if n <= 0 {
			return "", "", fmt.Errorf("number must be >= 1")
		}
		return h.handleFollowUp(ctx, threadID, strconv.Itoa(n))
	case "confirm_pending":
		args, err := decodeToolArgs[toolConfirmPendingArgs](rawArguments)
		if err != nil {
			return "", "", err
		}
		if args.Confirmed {
			return h.handleFollowUp(ctx, threadID, "yes")
		}
		return h.handleFollowUp(ctx, threadID, "no")
	case "set_review_days":
		args, err := decodeToolArgs[toolSetReviewDaysArgs](rawArguments)
		if err != nil {
			return "", "", err
		}
		return h.setReviewDays(ctx, args.Days)
	case "set_defer_days":
		args, err := decodeToolArgs[toolSetDeferDaysArgs](rawArguments)
		if err != nil {
			return "", "", err
		}
		return h.setDeferDays(ctx, args.Days)
	case "set_hitl_timeout_hours":
		args, err := decodeToolArgs[toolSetHITLTimeoutHoursArgs](rawArguments)
		if err != nil {
			return "", "", err
		}
		return h.setHITLTimeoutHours(ctx, args.Hours)
	default:
		return "", "", fmt.Errorf("unknown tool: %s", toolName)
	}
}

type toolResponseArgs struct {
	Message string `json:"message"`
}

type toolListReadyArgs struct {
	Limit int `json:"limit"`
}

type toolQueryLibraryArgs struct {
	Query       string `json:"query"`
	Scope       string `json:"scope"`
	SubjectType string `json:"subject_type"`
	Limit       int    `json:"limit"`
}

type toolFuzzySearchArgs struct {
	Query       string `json:"query"`
	SubjectType string `json:"subject_type"`
	Limit       int    `json:"limit"`
}

type toolQueryTargetStateArgs struct {
	Query string `json:"query"`
}

type toolRememberAliasArgs struct {
	Alias     string `json:"alias"`
	TargetRef string `json:"target_ref"`
}

type toolScheduleDeleteArgs struct {
	TargetRef string `json:"target_ref"`
}

type toolScheduleDeleteManyArgs struct {
	TargetIDs []string `json:"target_ids"`
}

type toolDelayTargetDaysArgs struct {
	TargetRef string `json:"target_ref"`
	Days      int    `json:"days"`
}

type toolArchiveTargetArgs struct {
	Query    string `json:"query"`
	Archived *bool  `json:"archived"`
}

type toolChooseCandidateArgs struct {
	Number int `json:"number"`
}

type toolConfirmPendingArgs struct {
	Confirmed bool `json:"confirmed"`
}

type toolSetReviewDaysArgs struct {
	Days int `json:"days"`
}

type toolSetDeferDaysArgs struct {
	Days int `json:"days"`
}

type toolSetHITLTimeoutHoursArgs struct {
	Hours int `json:"hours"`
}

func decodeToolArgs[T any](raw string) (T, error) {
	var out T
	if strings.TrimSpace(raw) == "" {
		return out, nil
	}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return out, fmt.Errorf("decode tool args: %w", err)
	}
	return out, nil
}

func (h *Harness) listReady(ctx context.Context, limit int) (string, string, error) {
	type row struct {
		flow domain.Flow
		next time.Time
	}
	rows := make([]row, 0)
	now := time.Now().UTC()
	err := h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flows, err := tx.ListFlows(ctx)
		if err != nil {
			return err
		}
		for _, f := range flows {
			ready := f.State == domain.FlowStatePendingReview || (f.State == domain.FlowStateActive && !f.NextActionAt.IsZero() && !f.NextActionAt.After(now))
			if !ready {
				continue
			}
			title := f.DisplayName
			if strings.TrimSpace(title) == "" {
				title = f.ItemID
			}
			rows = append(rows, row{flow: f, next: f.NextActionAt})
		}
		return nil
	})
	if err != nil {
		return "", "", err
	}
	if len(rows) == 0 {
		return marshalToolResult(map[string]any{"status": "ok", "ready": []map[string]any{}}), "", nil
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].next.Before(rows[j].next) })
	if limit <= 0 {
		limit = 10
	}
	if limit > 20 {
		limit = 20
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}
	ready := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		ready = append(ready, h.flowToolPayload(ctx, r.flow))
	}
	return marshalToolResult(map[string]any{"status": "ok", "ready": ready}), "", nil
}

func (h *Harness) queryLibrary(ctx context.Context, query string, scope string, subjectType string, limit int) (string, string, error) {
	query = strings.TrimSpace(strings.ToLower(query))
	scope = strings.TrimSpace(strings.ToLower(scope))
	if scope == "" {
		scope = "both"
	}
	if scope != "flows" && scope != "media" && scope != "both" {
		return marshalToolResult(map[string]any{"status": "invalid_scope", "scope": scope}), "", nil
	}
	subjectType = strings.TrimSpace(strings.ToLower(subjectType))
	if subjectType != "" && subjectType != "movie" && subjectType != "season" && subjectType != "episode" {
		return marshalToolResult(map[string]any{"status": "invalid_subject_type", "subject_type": subjectType}), "", nil
	}
	if limit <= 0 {
		limit = 12
	}
	if limit > 50 {
		limit = 50
	}

	flowResults := make([]map[string]any, 0, limit)
	mediaResults := make([]map[string]any, 0, limit)
	flowTotal := 0
	mediaTotal := 0
	movieFlowCount := 0
	seasonFlowCount := 0
	showNames := map[string]struct{}{}

	err := h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flows, err := tx.ListFlows(ctx)
		if err != nil {
			return err
		}
		for _, flow := range flows {
			typeLower := strings.ToLower(strings.TrimSpace(flow.SubjectType))
			if !isProjectionSubjectType(typeLower) || flow.State == domain.FlowStateDeleteQueued {
				continue
			}
			switch typeLower {
			case "movie":
				movieFlowCount++
			case "season":
				seasonFlowCount++
				if name := h.seasonSeriesName(ctx, tx, flow); name != "" {
					showNames[name] = struct{}{}
				}
			}

			if subjectType != "" && subjectType != typeLower && !(subjectType == "episode" && typeLower == "season") {
				continue
			}
			if scope == "flows" || scope == "both" {
				title := strings.ToLower(strings.TrimSpace(flow.DisplayName))
				if query == "" || strings.Contains(title, query) {
					flowTotal++
					if len(flowResults) < limit {
						flowResults = append(flowResults, flowToolPayloadInTx(flow, h.seasonSeriesName(ctx, tx, flow)))
					}
				}
			}

			if scope == "media" || scope == "both" {
				if typeLower == "movie" {
					itemID := flowSubjectID(flow.ItemID)
					if itemID != "" {
						if m, ok, err := tx.GetMedia(ctx, itemID); err == nil && ok {
							if subjectType == "" || subjectType == "movie" {
								if mediaMatchesQuery(m, query) {
									mediaTotal++
									if len(mediaResults) < limit {
										mediaResults = append(mediaResults, mediaToolPayload(m))
									}
								}
							}
						}
					}
				}
				if typeLower == "season" {
					seasonID := flowSubjectID(flow.ItemID)
					if seasonID == "" {
						continue
					}
					media, err := tx.ListMediaBySubject(ctx, "season", seasonID)
					if err != nil {
						continue
					}
					for _, m := range media {
						itemType := strings.ToLower(strings.TrimSpace(m.ItemType))
						if subjectType != "" && subjectType != itemType && !(subjectType == "season" && itemType == "episode") {
							continue
						}
						if !mediaMatchesQuery(m, query) {
							continue
						}
						mediaTotal++
						if len(mediaResults) < limit {
							mediaResults = append(mediaResults, mediaToolPayload(m))
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		return "", "", err
	}

	payload := map[string]any{
		"status":                  "ok",
		"scope":                   scope,
		"query":                   query,
		"subject_type":            subjectType,
		"flow_total":              flowTotal,
		"media_total":             mediaTotal,
		"movie_projection_count":  movieFlowCount,
		"season_projection_count": seasonFlowCount,
		"tv_show_count":           len(showNames),
	}
	if scope == "flows" || scope == "both" {
		payload["flows"] = flowResults
	}
	if scope == "media" || scope == "both" {
		payload["media"] = mediaResults
	}
	return marshalToolResult(payload), "", nil
}

func (h *Harness) seasonSeriesName(ctx context.Context, tx repo.TxRepository, flow domain.Flow) string {
	if seasonID := flowSubjectID(flow.ItemID); seasonID != "" {
		if seasonID != "" {
			media, err := tx.ListMediaBySubject(ctx, "season", seasonID)
			if err == nil {
				for _, item := range media {
					name := strings.TrimSpace(item.SeriesName)
					if name != "" {
						return strings.ToLower(name)
					}
				}
			}
		}
	}
	label := strings.TrimSpace(flow.DisplayName)
	if label == "" {
		return ""
	}
	lower := strings.ToLower(label)
	if idx := strings.LastIndex(lower, " of "); idx >= 0 && idx+4 < len(lower) {
		return strings.TrimSpace(lower[idx+4:])
	}
	return ""
}

func flowSubjectID(itemID string) string {
	parts := strings.SplitN(strings.TrimSpace(itemID), ":", 3)
	if len(parts) != 3 || parts[0] != "target" {
		return ""
	}
	return domain.NormalizeID(parts[2])
}

func mediaMatchesQuery(m domain.MediaItem, query string) bool {
	if query == "" {
		return true
	}
	haystack := strings.ToLower(strings.TrimSpace(strings.Join([]string{m.Name, m.Title, m.SeriesName, m.SeasonName}, " ")))
	return strings.Contains(haystack, query)
}

func mediaToolPayload(m domain.MediaItem) map[string]any {
	payload := map[string]any{
		"name":      strings.TrimSpace(m.Name),
		"item_type": strings.ToLower(strings.TrimSpace(m.ItemType)),
	}
	if strings.TrimSpace(m.SeriesName) != "" {
		payload["series_name"] = strings.TrimSpace(m.SeriesName)
	}
	if strings.TrimSpace(m.SeasonName) != "" {
		payload["season_name"] = strings.TrimSpace(m.SeasonName)
	}
	if !m.LastPlayedAt.IsZero() {
		payload["last_played_at"] = m.LastPlayedAt.UTC().Format(time.RFC3339)
	}
	if !m.CreatedAt.IsZero() {
		payload["created_at"] = m.CreatedAt.UTC().Format(time.RFC3339)
	}
	return payload
}

func flowToolPayloadInTx(flow domain.Flow, seriesName string) map[string]any {
	title := strings.TrimSpace(flow.DisplayName)
	if title == "" {
		title = "Untitled target"
	}
	payload := map[string]any{
		"title":        title,
		"subject_type": strings.ToLower(strings.TrimSpace(flow.SubjectType)),
		"flow_state":   string(flow.State),
		"archived":     flow.State == domain.FlowStateArchived,
		"ready":        flow.State == domain.FlowStatePendingReview || (flow.State == domain.FlowStateActive && !flow.NextActionAt.IsZero() && !flow.NextActionAt.After(time.Now().UTC())),
	}
	if !flow.NextActionAt.IsZero() {
		payload["next_action_at"] = flow.NextActionAt.UTC().Format(time.RFC3339)
	}
	if strings.TrimSpace(seriesName) != "" {
		payload["series_name"] = strings.TrimSpace(seriesName)
	}
	return payload
}

func (h *Harness) setReviewDays(ctx context.Context, days int) (string, string, error) {
	if days <= 0 || days > 3650 {
		return "", "", fmt.Errorf("days must be between 1 and 3650")
	}
	decision := h.getDecisionService()
	if decision == nil {
		return "", "", fmt.Errorf("decision service is not configured")
	}
	err := decision.SetGlobalReviewDays(ctx, days)
	if err != nil {
		return "", "", err
	}
	return marshalToolResult(map[string]any{"status": "ok", "days": days}), "", nil
}

func (h *Harness) setDeferDays(ctx context.Context, days int) (string, string, error) {
	if days <= 0 || days > 3650 {
		return "", "", fmt.Errorf("days must be between 1 and 3650")
	}
	decision := h.getDecisionService()
	if decision == nil {
		return "", "", fmt.Errorf("decision service is not configured")
	}
	err := decision.SetGlobalDeferDays(ctx, days)
	if err != nil {
		return "", "", err
	}
	return marshalToolResult(map[string]any{"status": "ok", "days": days}), "", nil
}

func (h *Harness) setHITLTimeoutHours(ctx context.Context, hours int) (string, string, error) {
	if hours <= 0 || hours > 8760 {
		return "", "", fmt.Errorf("hours must be between 1 and 8760")
	}
	decision := h.getDecisionService()
	if decision == nil {
		return "", "", fmt.Errorf("decision service is not configured")
	}
	err := decision.SetGlobalHITLTimeoutHours(ctx, hours)
	if err != nil {
		return "", "", err
	}
	return marshalToolResult(map[string]any{"status": "ok", "hours": hours}), "", nil
}

func (h *Harness) setArchiveState(ctx context.Context, threadID string, query string, archived bool) (string, string, error) {
	query = strings.TrimSpace(strings.ToLower(query))
	if query != "" {
		if remembered := h.resolveAlias(threadID, query); remembered != "" {
			state := h.getThreadState(threadID)
			state.SelectedTargetID = remembered
			h.setThreadState(threadID, state)
			query = ""
		}
	}
	if query == "" {
		selected := h.getThreadState(threadID).SelectedTargetID
		if selected == "" {
			return "", "", fmt.Errorf("provide a title")
		}
		state := h.getThreadState(threadID)
		if archived {
			state.PendingAction = "archive"
		} else {
			state.PendingAction = "unarchive"
		}
		h.setThreadState(threadID, state)
		flow, ok, _ := h.getFlowByID(ctx, selected)
		payload := map[string]any{"status": "needs_confirmation", "action": state.PendingAction}
		if ok {
			payload["target"] = h.flowToolPayload(ctx, flow)
		}
		return marshalToolResult(payload), "", nil
	}

	matches, err := h.findFlowMatches(ctx, query)
	if err != nil {
		return "", "", err
	}
	if len(matches) == 0 {
		return marshalToolResult(map[string]any{"status": "not_found"}), "", nil
	}
	if len(matches) > 1 {
		state := h.getThreadState(threadID)
		state.CandidateIDs = make([]string, 0, len(matches))
		if archived {
			state.PendingAction = "archive"
		} else {
			state.PendingAction = "unarchive"
		}
		for _, m := range matches {
			state.CandidateIDs = append(state.CandidateIDs, m.ItemID)
			h.rememberFlowAlias(&state, m, "")
		}
		h.rememberCandidateLabels(&state, matches)
		h.setThreadState(threadID, state)

		limit := len(matches)
		if limit > 5 {
			limit = 5
		}
		options := make([]map[string]any, 0, limit)
		for i := 0; i < limit; i++ {
			option := h.flowToolPayload(ctx, matches[i])
			option["choice"] = i + 1
			options = append(options, option)
		}
		return marshalToolResult(map[string]any{"status": "needs_selection", "options": options}), "", nil
	}

	flow := matches[0]
	state := h.getThreadState(threadID)
	state.SelectedTargetID = flow.ItemID
	h.rememberFlowAlias(&state, flow, "")
	if archived {
		state.PendingAction = "archive"
	} else {
		state.PendingAction = "unarchive"
	}
	state.CandidateIDs = nil
	h.setThreadState(threadID, state)

	return marshalToolResult(map[string]any{"status": "needs_confirmation", "action": state.PendingAction, "target": h.flowToolPayload(ctx, flow)}), "", nil
}

func (h *Harness) setDeleteState(ctx context.Context, threadID string, query string) (string, string, error) {
	query = strings.TrimSpace(strings.ToLower(query))
	if query != "" {
		if remembered := h.resolveAlias(threadID, query); remembered != "" {
			state := h.getThreadState(threadID)
			state.SelectedTargetID = remembered
			h.setThreadState(threadID, state)
			query = ""
		}
	}
	if query == "" {
		selected := h.getThreadState(threadID).SelectedTargetID
		if selected == "" {
			return "", "", fmt.Errorf("provide a title")
		}
		if flow, ok, _ := h.getFlowByID(ctx, selected); !ok || !isProjectionSubjectType(flow.SubjectType) {
			return marshalToolResult(map[string]any{"status": "needs_projection_target"}), "", nil
		}
		state := h.getThreadState(threadID)
		state.PendingAction = "schedule_delete"
		h.setThreadState(threadID, state)
		flow, ok, _ := h.getFlowByID(ctx, selected)
		payload := map[string]any{"status": "needs_confirmation", "action": state.PendingAction}
		if ok {
			payload["target"] = h.flowToolPayload(ctx, flow)
		}
		return marshalToolResult(payload), "", nil
	}

	matches, err := h.findProjectionFlowMatches(ctx, query)
	if err != nil {
		return "", "", err
	}
	if len(matches) == 0 {
		return marshalToolResult(map[string]any{"status": "not_found"}), "", nil
	}
	if len(matches) > 1 {
		state := h.getThreadState(threadID)
		state.CandidateIDs = make([]string, 0, len(matches))
		state.PendingAction = "schedule_delete"
		for _, m := range matches {
			state.CandidateIDs = append(state.CandidateIDs, m.ItemID)
			h.rememberFlowAlias(&state, m, "")
		}
		h.rememberCandidateLabels(&state, matches)
		h.setThreadState(threadID, state)

		limit := len(matches)
		if limit > 5 {
			limit = 5
		}
		options := make([]map[string]any, 0, limit)
		for i := 0; i < limit; i++ {
			option := h.flowToolPayload(ctx, matches[i])
			option["choice"] = i + 1
			options = append(options, option)
		}
		return marshalToolResult(map[string]any{"status": "needs_selection", "options": options}), "", nil
	}

	flow := matches[0]
	state := h.getThreadState(threadID)
	state.SelectedTargetID = flow.ItemID
	h.rememberFlowAlias(&state, flow, "")
	state.PendingAction = "schedule_delete"
	state.CandidateIDs = nil
	h.setThreadState(threadID, state)

	return marshalToolResult(map[string]any{"status": "needs_confirmation", "action": state.PendingAction, "target": h.flowToolPayload(ctx, flow)}), "", nil
}

func (h *Harness) delayTargetDays(ctx context.Context, threadID string, targetRef string, days int) (string, string, error) {
	if days <= 0 || days > 3650 {
		return "", "", fmt.Errorf("days must be between 1 and 3650")
	}
	targetID, err := h.resolveTargetRef(ctx, threadID, targetRef)
	if err != nil {
		return "", "", err
	}
	if strings.TrimSpace(targetID) == "" {
		return marshalToolResult(map[string]any{"status": "needs_target"}), "", nil
	}
	flow, found, err := h.getFlowByID(ctx, targetID)
	if err != nil {
		return "", "", err
	}
	if !found || !isProjectionSubjectType(flow.SubjectType) {
		return marshalToolResult(map[string]any{"status": "needs_projection_target"}), "", nil
	}
	decision := h.getDecisionService()
	if decision == nil {
		return "", "", fmt.Errorf("decision service is not configured")
	}
	if err := decision.ApplyAIDelayDays(ctx, targetID, days); err != nil {
		return "", "", err
	}
	state := h.getThreadState(threadID)
	state.SelectedTargetID = targetID
	h.rememberFlowAlias(&state, flow, "")
	h.setThreadState(threadID, state)
	return marshalToolResult(map[string]any{"status": "done", "action": "delay", "days": days, "target": h.flowToolPayload(ctx, flow)}), "", nil
}

func (h *Harness) queryTargetState(ctx context.Context, threadID string, query string) (string, string, error) {
	query = strings.TrimSpace(strings.ToLower(query))
	if query != "" {
		if remembered := h.resolveAlias(threadID, query); remembered != "" {
			state := h.getThreadState(threadID)
			state.SelectedTargetID = remembered
			h.setThreadState(threadID, state)
			query = ""
		}
	}
	if query == "" {
		selected := h.getThreadState(threadID).SelectedTargetID
		if selected == "" {
			return marshalToolResult(map[string]any{"status": "needs_query"}), "", nil
		}
		flow, ok, err := h.getFlowByID(ctx, selected)
		if err != nil {
			return "", "", err
		}
		if !ok {
			return marshalToolResult(map[string]any{"status": "not_found"}), "", nil
		}
		return marshalToolResult(map[string]any{"status": "ok", "target": h.flowToolPayload(ctx, flow)}), "", nil
	}

	matches, err := h.findFlowMatches(ctx, query)
	if err != nil {
		return "", "", err
	}
	if len(matches) == 0 {
		return marshalToolResult(map[string]any{"status": "not_found"}), "", nil
	}
	if len(matches) > 1 {
		state := h.getThreadState(threadID)
		state.CandidateIDs = make([]string, 0, len(matches))
		state.PendingAction = ""
		for _, m := range matches {
			state.CandidateIDs = append(state.CandidateIDs, m.ItemID)
			h.rememberFlowAlias(&state, m, "")
		}
		h.rememberCandidateLabels(&state, matches)
		h.setThreadState(threadID, state)

		limit := len(matches)
		if limit > 5 {
			limit = 5
		}
		options := make([]map[string]any, 0, limit)
		for i := 0; i < limit; i++ {
			option := h.flowToolPayload(ctx, matches[i])
			option["choice"] = i + 1
			options = append(options, option)
		}
		return marshalToolResult(map[string]any{"status": "needs_selection", "options": options}), "", nil
	}

	state := h.getThreadState(threadID)
	state.SelectedTargetID = matches[0].ItemID
	h.rememberFlowAlias(&state, matches[0], "")
	state.CandidateIDs = nil
	h.setThreadState(threadID, state)
	return marshalToolResult(map[string]any{"status": "ok", "target": h.flowToolPayload(ctx, matches[0])}), "", nil
}

func (h *Harness) handleFollowUp(ctx context.Context, threadID string, input string) (string, string, error) {
	trimmed := strings.TrimSpace(strings.ToLower(input))
	state := h.getThreadState(threadID)

	if len(state.CandidateIDs) > 0 {
		if idx, err := strconv.Atoi(trimmed); err == nil && idx >= 1 && idx <= len(state.CandidateIDs) {
			chosenID := state.CandidateIDs[idx-1]
			state.CandidateIDs = nil
			state.SelectedTargetID = chosenID
			flow, ok, err := h.getFlowByID(ctx, chosenID)
			if err != nil {
				return "", "", err
			}
			if !ok {
				h.setThreadState(threadID, state)
				return marshalToolResult(map[string]any{"status": "not_found"}), "", nil
			}
			h.rememberFlowAlias(&state, flow, "")
			h.rememberFlowAlias(&state, flow, fmt.Sprintf("title %c", 'a'+idx-1))
			h.setThreadState(threadID, state)
			if state.PendingAction != "" {
				return marshalToolResult(map[string]any{"status": "needs_confirmation", "action": state.PendingAction, "target": h.flowToolPayload(ctx, flow)}), "", nil
			}
			return marshalToolResult(map[string]any{"status": "ok", "target": h.flowToolPayload(ctx, flow)}), "", nil
		}
		if remembered := h.resolveAlias(threadID, trimmed); remembered != "" {
			state.SelectedTargetID = remembered
			state.CandidateIDs = nil
			h.setThreadState(threadID, state)
			flow, ok, err := h.getFlowByID(ctx, remembered)
			if err != nil {
				return "", "", err
			}
			if !ok {
				return marshalToolResult(map[string]any{"status": "not_found"}), "", nil
			}
			if state.PendingAction != "" {
				return marshalToolResult(map[string]any{"status": "needs_confirmation", "action": state.PendingAction, "target": h.flowToolPayload(ctx, flow)}), "", nil
			}
			return marshalToolResult(map[string]any{"status": "ok", "target": h.flowToolPayload(ctx, flow)}), "", nil
		}
	}

	if trimmed == "yes" && state.PendingAction != "" {
		if state.SelectedTargetID == "" {
			state.PendingAction = ""
			h.setThreadState(threadID, state)
			return marshalToolResult(map[string]any{"status": "needs_selection"}), "", nil
		}
		var (
			out string
			err error
		)
		switch state.PendingAction {
		case "archive":
			out, err = h.applyArchiveToID(ctx, state.SelectedTargetID, true)
		case "unarchive":
			out, err = h.applyArchiveToID(ctx, state.SelectedTargetID, false)
		case "schedule_delete":
			out, err = h.applyScheduleDeleteToID(ctx, state.SelectedTargetID)
		default:
			err = fmt.Errorf("unsupported pending action %q", state.PendingAction)
		}
		state.PendingAction = ""
		h.setThreadState(threadID, state)
		if err != nil {
			return "", "", err
		}
		return out, "", nil
	}
	if trimmed == "no" && state.PendingAction != "" {
		state.PendingAction = ""
		h.setThreadState(threadID, state)
		return marshalToolResult(map[string]any{"status": "cancelled"}), "", nil
	}

	return marshalToolResult(map[string]any{"status": "ignored"}), "", nil
}

func (h *Harness) applyArchiveToID(ctx context.Context, itemID string, archived bool) (string, error) {
	err := h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flow, found, err := tx.GetFlow(ctx, itemID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("target not found")
		}
		expected := flow.Version
		if archived {
			flow.State = domain.FlowStateArchived
			flow.HITLOutcome = "archive"
		} else {
			flow.State = domain.FlowStateActive
			flow.HITLOutcome = ""
		}
		flow.Version = expected + 1
		flow.UpdatedAt = time.Now().UTC()
		return tx.UpsertFlowCAS(ctx, flow, expected)
	})
	if err != nil {
		return "", err
	}
	verb := "archived"
	if !archived {
		verb = "unarchived"
	}
	flow, ok, _ := h.getFlowByID(ctx, itemID)
	name := "selected target"
	if ok && strings.TrimSpace(flow.DisplayName) != "" {
		name = flow.DisplayName
	}
	return marshalToolResult(map[string]any{"status": "done", "action": verb, "title": name}), nil
}

func (h *Harness) applyScheduleDeleteToID(ctx context.Context, itemID string) (string, error) {
	decision := h.getDecisionService()
	if decision == nil {
		return "", fmt.Errorf("decision service is not configured")
	}
	if flow, ok, _ := h.getFlowByID(ctx, itemID); !ok || !isProjectionSubjectType(flow.SubjectType) {
		return marshalToolResult(map[string]any{"status": "needs_projection_target"}), nil
	}
	err := decision.ApplyAIDecision(ctx, itemID, "delete")
	if err != nil {
		return "", err
	}
	flow, ok, _ := h.getFlowByID(ctx, itemID)
	name := "selected target"
	if ok && strings.TrimSpace(flow.DisplayName) != "" {
		name = flow.DisplayName
	}
	return marshalToolResult(map[string]any{"status": "done", "action": "scheduled_delete", "title": name}), nil
}

func (h *Harness) scheduleDeleteMany(ctx context.Context, targetIDs []string) (string, string, error) {
	if len(targetIDs) == 0 {
		return marshalToolResult(map[string]any{"status": "needs_targets"}), "", nil
	}
	decision := h.getDecisionService()
	if decision == nil {
		return "", "", fmt.Errorf("decision service is not configured")
	}
	unique := make([]string, 0, len(targetIDs))
	seen := map[string]struct{}{}
	for _, id := range targetIDs {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		flow, found, err := h.getFlowByID(ctx, trimmed)
		if err != nil {
			return "", "", err
		}
		if !found || !isProjectionSubjectType(flow.SubjectType) {
			continue
		}
		seen[trimmed] = struct{}{}
		unique = append(unique, trimmed)
	}
	if len(unique) == 0 {
		return marshalToolResult(map[string]any{"status": "needs_projection_target"}), "", nil
	}
	if err := decision.ApplyAIDecisionBatch(ctx, unique, "delete"); err != nil {
		return "", "", err
	}
	return marshalToolResult(map[string]any{"status": "done", "action": "scheduled_delete_many", "count": len(unique)}), "", nil
}

func (h *Harness) fuzzySearchTargets(ctx context.Context, query string, subjectType string, limit int) (string, string, error) {
	query = strings.TrimSpace(strings.ToLower(query))
	subjectType = strings.TrimSpace(strings.ToLower(subjectType))
	if subjectType != "" && !isProjectionSubjectType(subjectType) {
		subjectType = ""
	}
	if query == "" {
		return marshalToolResult(map[string]any{"status": "needs_query"}), "", nil
	}
	if limit <= 0 || limit > 30 {
		limit = 12
	}
	matches, err := h.findProjectionFlowMatchesFiltered(ctx, query, subjectType)
	if err != nil {
		return "", "", err
	}
	if len(matches) > limit {
		matches = matches[:limit]
	}
	out := make([]map[string]any, 0, len(matches))
	for _, f := range matches {
		p := h.flowToolPayload(ctx, f)
		p["item_id"] = f.ItemID
		out = append(out, p)
	}
	return marshalToolResult(map[string]any{"status": "ok", "count": len(out), "results": out}), "", nil
}

func (h *Harness) findProjectionFlowMatches(ctx context.Context, query string) ([]domain.Flow, error) {
	return h.findProjectionFlowMatchesFiltered(ctx, query, "")
}

func (h *Harness) findProjectionFlowMatchesFiltered(ctx context.Context, query string, subjectType string) ([]domain.Flow, error) {
	matches, err := h.findFlowMatchesFiltered(ctx, query, subjectType)
	if err != nil {
		return nil, err
	}
	projected := make([]domain.Flow, 0, len(matches))
	for _, m := range matches {
		if isProjectionSubjectType(m.SubjectType) {
			projected = append(projected, m)
		}
	}
	return projected, nil
}

func isProjectionSubjectType(subjectType string) bool {
	s := strings.ToLower(strings.TrimSpace(subjectType))
	return s == "movie" || s == "season"
}

func (h *Harness) getDecisionService() DecisionService {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.decision
}

func (h *Harness) flowToolPayload(ctx context.Context, flow domain.Flow) map[string]any {
	title := strings.TrimSpace(flow.DisplayName)
	if title == "" {
		title = "Untitled target"
	}
	archived := flow.State == domain.FlowStateArchived
	ready := flow.State == domain.FlowStatePendingReview || (flow.State == domain.FlowStateActive && !flow.NextActionAt.IsZero() && !flow.NextActionAt.After(time.Now().UTC()))
	payload := map[string]any{
		"title":        title,
		"subject_type": strings.ToLower(flow.SubjectType),
		"flow_state":   string(flow.State),
		"archived":     archived,
		"ready":        ready,
	}
	if !flow.NextActionAt.IsZero() {
		payload["next_action_at"] = flow.NextActionAt.UTC().Format(time.RFC3339)
		payload["next_action_relative"] = h.humanizeWhen(flow.NextActionAt)
	}
	if media, hasMedia, _ := h.getMediaByID(ctx, flow.ItemID); hasMedia {
		if strings.TrimSpace(media.SeasonName) != "" {
			payload["season_name"] = media.SeasonName
		}
		if strings.TrimSpace(media.SeriesName) != "" {
			payload["series_name"] = media.SeriesName
		}
		if !media.LastPlayedAt.IsZero() {
			payload["last_played_at"] = media.LastPlayedAt.UTC().Format(time.RFC3339)
			payload["last_played_relative"] = h.humanizeWhen(media.LastPlayedAt)
		}
		if media.PlayCountTotal > 0 {
			payload["play_count_total"] = media.PlayCountTotal
		}
	}
	return payload
}

func (h *Harness) findFlowMatches(ctx context.Context, query string) ([]domain.Flow, error) {
	return h.findFlowMatchesFiltered(ctx, query, "")
}

func (h *Harness) findFlowMatchesFiltered(ctx context.Context, query string, subjectType string) ([]domain.Flow, error) {
	query = strings.TrimSpace(strings.ToLower(query))
	subjectType = strings.TrimSpace(strings.ToLower(subjectType))
	if query == "" {
		return []domain.Flow{}, nil
	}
	matches := make([]domain.Flow, 0)
	err := h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flows, err := tx.SearchFlows(ctx, query, subjectType, 50)
		if err != nil {
			return err
		}
		matches = append(matches, flows...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(matches, func(i, j int) bool {
		ai := strings.ToLower(matches[i].DisplayName)
		aj := strings.ToLower(matches[j].DisplayName)
		if ai == query && aj != query {
			return true
		}
		if aj == query && ai != query {
			return false
		}
		return len(ai) < len(aj)
	})
	return matches, nil
}

func (h *Harness) getFlowByID(ctx context.Context, itemID string) (domain.Flow, bool, error) {
	var out domain.Flow
	var found bool
	err := h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		f, ok, err := tx.GetFlow(ctx, itemID)
		if err != nil {
			return err
		}
		out, found = f, ok
		return nil
	})
	if err != nil {
		return domain.Flow{}, false, err
	}
	return out, found, nil
}

func (h *Harness) renderFlowSummary(ctx context.Context, flow domain.Flow) string {
	title := strings.TrimSpace(flow.DisplayName)
	if title == "" {
		title = "Untitled target"
	}
	archived := flow.State == domain.FlowStateArchived
	ready := flow.State == domain.FlowStatePendingReview || (flow.State == domain.FlowStateActive && !flow.NextActionAt.IsZero() && !flow.NextActionAt.After(time.Now().UTC()))
	media, hasMedia, _ := h.getMediaByID(ctx, flow.ItemID)
	b := strings.Builder{}
	b.WriteString("**")
	b.WriteString(title)
	b.WriteString("**")
	if hasMedia && strings.TrimSpace(media.SeasonName) != "" {
		b.WriteString(" — ")
		b.WriteString(media.SeasonName)
		if strings.TrimSpace(media.SeriesName) != "" {
			b.WriteString(" of ")
			b.WriteString(media.SeriesName)
		}
	}
	b.WriteString(" (")
	b.WriteString(strings.ToLower(flow.SubjectType))
	b.WriteString(")")
	b.WriteString(" | state: ")
	b.WriteString(string(flow.State))
	b.WriteString(" | archived: ")
	b.WriteString(strconv.FormatBool(archived))
	b.WriteString(" | ready: ")
	b.WriteString(strconv.FormatBool(ready))
	if !flow.NextActionAt.IsZero() {
		b.WriteString(" | next action: ")
		b.WriteString(h.humanizeWhen(flow.NextActionAt))
	}
	if strings.TrimSpace(flow.ImageURL) != "" {
		b.WriteString(" | has artwork")
	}
	return b.String()
}

func (h *Harness) humanizeWhen(at time.Time) string {
	if at.IsZero() {
		return "unknown"
	}
	now := time.Now().UTC()
	d := at.Sub(now)
	abs := d
	if abs < 0 {
		abs = -abs
	}
	unit := "minutes"
	value := int(abs.Minutes())
	if value >= 120 {
		unit = "hours"
		value = int(abs.Hours())
	}
	if value >= 48 && unit == "hours" {
		unit = "days"
		value = int(abs.Hours() / 24)
	}
	rel := fmt.Sprintf("in %d %s", value, unit)
	if d < 0 {
		rel = fmt.Sprintf("%d %s ago", value, unit)
	}
	return rel + " (" + at.UTC().Format("2006-01-02 15:04 UTC") + ")"
}

func (h *Harness) getMediaByID(ctx context.Context, itemID string) (domain.MediaItem, bool, error) {
	var media domain.MediaItem
	var found bool
	err := h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		m, ok, err := tx.GetMedia(ctx, itemID)
		if err != nil {
			return err
		}
		media = m
		found = ok
		return nil
	})
	if err != nil {
		return domain.MediaItem{}, false, err
	}
	return media, found, nil
}

func marshalToolResult(payload map[string]any) string {
	b, err := json.Marshal(payload)
	if err != nil {
		return `{"status":"error"}`
	}
	return string(b)
}

func (h *Harness) rememberFlowAlias(state *threadState, flow domain.Flow, extraAlias string) {
	if state == nil {
		return
	}
	if strings.TrimSpace(flow.ItemID) == "" {
		return
	}
	if state.AliasToItemID == nil {
		state.AliasToItemID = map[string]string{}
	}
	title := strings.TrimSpace(flow.DisplayName)
	if title != "" {
		state.AliasToItemID[normalizeAliasKey(title)] = flow.ItemID
	}
	if strings.TrimSpace(extraAlias) != "" {
		state.AliasToItemID[normalizeAliasKey(extraAlias)] = flow.ItemID
	}
}

func (h *Harness) rememberCandidateLabels(state *threadState, matches []domain.Flow) {
	if state == nil || len(matches) == 0 {
		return
	}
	for i, m := range matches {
		h.rememberFlowAlias(state, m, strconv.Itoa(i+1))
		if i < 26 {
			h.rememberFlowAlias(state, m, fmt.Sprintf("title %c", 'a'+i))
		} else {
			h.rememberFlowAlias(state, m, fmt.Sprintf("title %d", i+1))
		}
	}
}

func (h *Harness) rememberAlias(ctx context.Context, threadID string, alias string, targetRef string) (string, string, error) {
	aliasKey := normalizeAliasKey(alias)
	if aliasKey == "" {
		return "", "", fmt.Errorf("alias is required")
	}
	targetID, err := h.resolveTargetRef(ctx, threadID, targetRef)
	if err != nil {
		return "", "", err
	}
	if strings.TrimSpace(targetID) == "" {
		return marshalToolResult(map[string]any{"status": "needs_target"}), "", nil
	}
	flow, ok, err := h.getFlowByID(ctx, targetID)
	if err != nil {
		return "", "", err
	}
	if !ok {
		return marshalToolResult(map[string]any{"status": "not_found"}), "", nil
	}
	state := h.getThreadState(threadID)
	h.rememberFlowAlias(&state, flow, aliasKey)
	h.setThreadState(threadID, state)
	return marshalToolResult(map[string]any{"status": "ok", "alias": aliasKey, "target": h.flowToolPayload(ctx, flow)}), "", nil
}

func (h *Harness) resolveTargetRef(ctx context.Context, threadID string, targetRef string) (string, error) {
	trimmed := normalizeAliasKey(targetRef)
	state := h.getThreadState(threadID)
	if trimmed == "" {
		return state.SelectedTargetID, nil
	}
	if id := h.resolveAlias(threadID, trimmed); id != "" {
		return id, nil
	}
	if idx, err := strconv.Atoi(trimmed); err == nil && idx >= 1 && idx <= len(state.CandidateIDs) {
		return state.CandidateIDs[idx-1], nil
	}
	matches, err := h.findFlowMatches(ctx, trimmed)
	if err != nil {
		return "", err
	}
	if len(matches) == 1 {
		return matches[0].ItemID, nil
	}
	return "", nil
}

func (h *Harness) resolveAlias(threadID string, query string) string {
	key := normalizeAliasKey(query)
	if key == "" {
		return ""
	}
	state := h.getThreadState(threadID)
	if state.AliasToItemID == nil {
		return ""
	}
	if id, ok := state.AliasToItemID[key]; ok {
		return id
	}
	for alias, id := range state.AliasToItemID {
		if strings.Contains(alias, key) || strings.Contains(key, alias) {
			return id
		}
	}
	return ""
}

func (h *Harness) serializeThreadContext(ctx context.Context, state threadState) string {
	b := strings.Builder{}
	pending := state.PendingAction
	if pending == "" {
		pending = "none"
	}
	b.WriteString("pending_action=")
	b.WriteString(pending)
	b.WriteString("\n")

	if state.SelectedTargetID != "" {
		if flow, ok, _ := h.getFlowByID(ctx, state.SelectedTargetID); ok {
			b.WriteString("selected_target=")
			b.WriteString(strings.TrimSpace(flow.DisplayName))
			if strings.TrimSpace(flow.DisplayName) == "" {
				b.WriteString("Untitled target")
			}
			b.WriteString("\n")
		} else {
			b.WriteString("selected_target=unknown\n")
		}
	} else {
		b.WriteString("selected_target=none\n")
	}

	if len(state.CandidateIDs) > 0 {
		b.WriteString("candidate_targets=\n")
		for i, id := range state.CandidateIDs {
			label := strconv.Itoa(i + 1)
			title := "Unknown"
			if flow, ok, _ := h.getFlowByID(ctx, id); ok {
				if strings.TrimSpace(flow.DisplayName) != "" {
					title = strings.TrimSpace(flow.DisplayName)
				}
			}
			b.WriteString("- ")
			b.WriteString(label)
			b.WriteString(": ")
			b.WriteString(title)
			b.WriteString("\n")
		}
	} else {
		b.WriteString("candidate_targets=none\n")
	}

	if len(state.AliasToItemID) == 0 {
		b.WriteString("known_aliases=none")
		return b.String()
	}

	keys := make([]string, 0, len(state.AliasToItemID))
	for alias := range state.AliasToItemID {
		keys = append(keys, alias)
	}
	sort.Strings(keys)
	if len(keys) > 20 {
		keys = keys[:20]
	}
	b.WriteString("known_aliases=\n")
	for _, alias := range keys {
		title := "Unknown"
		if flow, ok, _ := h.getFlowByID(ctx, state.AliasToItemID[alias]); ok {
			if strings.TrimSpace(flow.DisplayName) != "" {
				title = strings.TrimSpace(flow.DisplayName)
			}
		}
		b.WriteString("- ")
		b.WriteString(alias)
		b.WriteString(" => ")
		b.WriteString(title)
		b.WriteString("\n")
	}
	return strings.TrimSpace(b.String())
}

func normalizeAliasKey(input string) string {
	trimmed := strings.TrimSpace(strings.ToLower(input))
	if trimmed == "" {
		return ""
	}
	return strings.Join(strings.Fields(trimmed), " ")
}

func (h *Harness) getThreadState(threadID string) threadState {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.touchThreadLocked(threadID)
	return h.state[threadID]
}

func (h *Harness) setThreadState(threadID string, state threadState) {
	if strings.TrimSpace(threadID) == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.touchThreadLocked(threadID)
	h.state[threadID] = state
	h.pruneThreadContextsLocked()
}

func (h *Harness) getHistory(threadID string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if threadID == "" {
		return nil
	}
	h.touchThreadLocked(threadID)
	b, ok := h.history[threadID]
	if !ok || b == nil {
		return nil
	}
	history := b.snapshot()
	if len(history) > 8 {
		history = history[len(history)-8:]
	}
	copyOut := make([]string, len(history))
	copy(copyOut, history)
	return copyOut
}

func (h *Harness) appendHistory(threadID string, line string) {
	if threadID == "" || strings.TrimSpace(line) == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.touchThreadLocked(threadID)
	b := h.history[threadID]
	if b == nil {
		b = newRingBuffer(historyLineCapacity)
		h.history[threadID] = b
	}
	b.append(line)
	h.pruneThreadContextsLocked()
}

func (h *Harness) touchThread(threadID string) {
	if strings.TrimSpace(threadID) == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.touchThreadLocked(threadID)
	h.pruneThreadContextsLocked()
}

func (h *Harness) touchThreadLocked(threadID string) {
	if strings.TrimSpace(threadID) == "" {
		return
	}
	if h.threadLRU == nil {
		h.threadLRU = list.New()
	}
	if h.threadIndex == nil {
		h.threadIndex = map[string]*list.Element{}
	}
	if el, ok := h.threadIndex[threadID]; ok {
		h.threadLRU.MoveToBack(el)
		return
	}
	el := h.threadLRU.PushBack(threadID)
	h.threadIndex[threadID] = el
}

func (h *Harness) pruneThreadContextsLocked() {
	max := h.maxThreads
	if max <= 0 {
		max = maxThreadContexts
	}
	for len(h.threadIndex) > max {
		front := h.threadLRU.Front()
		if front == nil {
			return
		}
		key, _ := front.Value.(string)
		h.threadLRU.Remove(front)
		delete(h.threadIndex, key)
		delete(h.history, key)
		delete(h.state, key)
	}
}
