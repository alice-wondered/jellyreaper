package ai

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/openai/openai-go/shared"

	"jellyreaper/internal/domain"
	"jellyreaper/internal/repo"
)

const (
	metaReviewDays = "settings.review_days"
	metaDeferDays  = "settings.defer_days"

	historyLineCapacity = 20
	maxThreadContexts   = 256

	storageSkill = "Storage/projection skill: flows are stored as targets with ids like target:<subject_type>:<subject_id> and states such as active, pending_review, archived, delete_queued. Media records hold item metadata and projection linkage (season/series ids and names). Season/series projections represent grouped cleanup scope; deleting a season/series projection deletes child items in normal workflow. Discovery strategy: first fuzzy-search targets by title text and optional subject type; inspect target state for specific candidates; when query suggests a series-level action, prefer projection targets (season/series) over individual episode items so scheduling can fan out correctly. Preserve follow-up semantics by asking for clarification when multiple candidates match, then confirm before destructive actions."
)

type Harness struct {
	repository repo.Repository
	client     openai.Client
	model      string
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

	ProjectionCandidateKeys []string
	ProjectionCandidateSet  map[string][]string
	PendingProjectionIDs    []string
	PendingProjectionLabel  string
}

type intent struct {
	Intent string `json:"intent"`
	Query  string `json:"query"`
	Days   int    `json:"days"`
}

func NewHarness(repository repo.Repository, apiKey string, model string) *Harness {
	if strings.TrimSpace(model) == "" {
		model = "gpt-4o-mini"
	}
	return &Harness{
		repository:  repository,
		client:      openai.NewClient(option.WithAPIKey(strings.TrimSpace(apiKey))),
		model:       model,
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
		return "I hit an issue while processing that request. Please try again with a little more detail.", nil
	}

	h.appendHistory(threadID, userName+": "+input)
	h.appendHistory(threadID, "assistant: "+out)
	return out, nil
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
		"Tool usage guide: list_ready for queue overviews; fuzzy_search_targets for discovery; query_target_state to inspect one target; remember_alias to bind user phrases to a target; archive_target for archive/unarchive intents; schedule_delete_target for single-target deletes; schedule_delete_projection for series/season projection deletes; choose_candidate when user provides a numbered option; confirm_pending for yes/no confirmations; set_review_days and set_defer_days for policy tuning.",
		"Use schedule_delete_target when the user asks to schedule a title for deletion; keep follow-up confirmation semantics consistent.",
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

	messages := []openai.ChatCompletionMessageParamUnion{
		openai.SystemMessage(system),
		openai.UserMessage(userPrompt),
	}

	tools := []openai.ChatCompletionToolParam{
		{Function: shared.FunctionDefinitionParam{Name: "response", Description: openai.String("Send a Discord markdown response back to the user."), Parameters: shared.FunctionParameters{"type": "object", "properties": map[string]any{"message": map[string]any{"type": "string"}}, "required": []string{"message"}}}},
		{Function: shared.FunctionDefinitionParam{Name: "list_ready", Description: openai.String("List targets currently ready for review."), Parameters: shared.FunctionParameters{"type": "object", "properties": map[string]any{"limit": map[string]any{"type": "integer", "minimum": 1, "maximum": 20}}}}},
		{Function: shared.FunctionDefinitionParam{Name: "fuzzy_search_targets", Description: openai.String("Find targets by fuzzy title/series/season text, optionally filtered by subject type."), Parameters: shared.FunctionParameters{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}, "subject_type": map[string]any{"type": "string", "enum": []string{"", "item", "movie", "season", "series"}}, "limit": map[string]any{"type": "integer", "minimum": 1, "maximum": 30}}, "required": []string{"query"}}}},
		{Function: shared.FunctionDefinitionParam{Name: "query_target_state", Description: openai.String("Inspect state for a target by title text, or current selection when query omitted."), Parameters: shared.FunctionParameters{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}}}}},
		{Function: shared.FunctionDefinitionParam{Name: "remember_alias", Description: openai.String("Store a natural-language alias for a specific target so future follow-ups can resolve quickly."), Parameters: shared.FunctionParameters{"type": "object", "properties": map[string]any{"alias": map[string]any{"type": "string"}, "target_ref": map[string]any{"type": "string"}}, "required": []string{"alias"}}}},
		{Function: shared.FunctionDefinitionParam{Name: "schedule_delete_target", Description: openai.String("Start schedule-for-deletion flow for a target title."), Parameters: shared.FunctionParameters{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}}}}},
		{Function: shared.FunctionDefinitionParam{Name: "schedule_delete_projection", Description: openai.String("Schedule deletion for projection targets (season or series), useful for series-level cleanup."), Parameters: shared.FunctionParameters{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}, "projection_scope": map[string]any{"type": "string", "enum": []string{"season", "series", "auto"}}}, "required": []string{"query"}}}},
		{Function: shared.FunctionDefinitionParam{Name: "archive_target", Description: openai.String("Start archive or unarchive flow for a target title."), Parameters: shared.FunctionParameters{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}, "archived": map[string]any{"type": "boolean"}}, "required": []string{"archived"}}}},
		{Function: shared.FunctionDefinitionParam{Name: "choose_candidate", Description: openai.String("Choose one of the numbered candidates from the last disambiguation list."), Parameters: shared.FunctionParameters{"type": "object", "properties": map[string]any{"number": map[string]any{"type": "integer", "minimum": 1}}, "required": []string{"number"}}}},
		{Function: shared.FunctionDefinitionParam{Name: "confirm_pending", Description: openai.String("Confirm or cancel the pending archive/unarchive action."), Parameters: shared.FunctionParameters{"type": "object", "properties": map[string]any{"confirmed": map[string]any{"type": "boolean"}}, "required": []string{"confirmed"}}}},
		{Function: shared.FunctionDefinitionParam{Name: "set_review_days", Description: openai.String("Set policy review period in days for active flows."), Parameters: shared.FunctionParameters{"type": "object", "properties": map[string]any{"days": map[string]any{"type": "integer", "minimum": 1, "maximum": 3650}}, "required": []string{"days"}}}},
		{Function: shared.FunctionDefinitionParam{Name: "set_defer_days", Description: openai.String("Set default defer period in days."), Parameters: shared.FunctionParameters{"type": "object", "properties": map[string]any{"days": map[string]any{"type": "integer", "minimum": 1, "maximum": 365}}, "required": []string{"days"}}}},
	}

	for i := 0; i < 6; i++ {
		resp, err := h.client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
			Model:      openai.ChatModel(h.model),
			Messages:   messages,
			Tools:      tools,
			ToolChoice: openai.ChatCompletionToolChoiceOptionUnionParam{OfAuto: openai.String("auto")},
		})
		if err != nil {
			return "", err
		}
		if len(resp.Choices) == 0 {
			return "", fmt.Errorf("no choices")
		}
		msg := resp.Choices[0].Message
		if len(msg.ToolCalls) == 0 {
			content := strings.TrimSpace(msg.Content)
			if content != "" {
				return content, nil
			}
			messages = append(messages, msg.ToParam())
			messages = append(messages, openai.SystemMessage("Please respond with a concise Discord-markdown message for the user."))
			continue
		}

		messages = append(messages, msg.ToParam())
		for _, tc := range msg.ToolCalls {
			result, out, err := h.executeToolCall(ctx, threadID, tc)
			if err != nil {
				result = "tool error: " + err.Error()
			}
			messages = append(messages, openai.ToolMessage(result, tc.ID))
			if strings.TrimSpace(out) != "" {
				return out, nil
			}
		}
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

func (h *Harness) executeToolCall(ctx context.Context, threadID string, tc openai.ChatCompletionMessageToolCall) (string, string, error) {
	args := map[string]any{}
	if strings.TrimSpace(tc.Function.Arguments) != "" {
		if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
			return "", "", err
		}
	}

	switch tc.Function.Name {
	case "response":
		msg := strings.TrimSpace(asString(args["message"]))
		if msg == "" {
			msg = "Could you share a little more detail so I can help?"
		}
		return `{"ok":true}`, msg, nil
	case "list_ready":
		return h.listReady(ctx)
	case "fuzzy_search_targets":
		return h.fuzzySearchTargets(ctx, asString(args["query"]), asString(args["subject_type"]), asInt(args["limit"], 12))
	case "query_target_state":
		return h.queryTargetState(ctx, threadID, asString(args["query"]))
	case "remember_alias":
		return h.rememberAlias(ctx, threadID, asString(args["alias"]), asString(args["target_ref"]))
	case "schedule_delete_target":
		return h.setDeleteState(ctx, threadID, asString(args["query"]))
	case "schedule_delete_projection":
		return h.setDeleteProjectionState(ctx, threadID, asString(args["query"]), asString(args["projection_scope"]))
	case "archive_target":
		archived := asBool(args["archived"], true)
		return h.setArchiveState(ctx, threadID, asString(args["query"]), archived)
	case "choose_candidate":
		n := asInt(args["number"], 0)
		if n <= 0 {
			return "", "", fmt.Errorf("number must be >= 1")
		}
		return h.handleFollowUp(ctx, threadID, strconv.Itoa(n))
	case "confirm_pending":
		confirmed := asBool(args["confirmed"], false)
		if confirmed {
			return h.handleFollowUp(ctx, threadID, "yes")
		}
		return h.handleFollowUp(ctx, threadID, "no")
	case "set_review_days":
		return h.setReviewDays(ctx, asInt(args["days"], 0))
	case "set_defer_days":
		return h.setDeferDays(ctx, asInt(args["days"], 0))
	default:
		return "", "", fmt.Errorf("unknown tool: %s", tc.Function.Name)
	}
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

func asInt(v any, fallback int) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	default:
		return fallback
	}
}

func asBool(v any, fallback bool) bool {
	b, ok := v.(bool)
	if !ok {
		return fallback
	}
	return b
}

func (h *Harness) listReady(ctx context.Context) (string, string, error) {
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
	if len(rows) > 10 {
		rows = rows[:10]
	}
	ready := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		ready = append(ready, h.flowToolPayload(ctx, r.flow))
	}
	return marshalToolResult(map[string]any{"status": "ok", "ready": ready}), "", nil
}

func (h *Harness) setReviewDays(ctx context.Context, days int) (string, string, error) {
	if days <= 0 || days > 3650 {
		return "", "", fmt.Errorf("days must be between 1 and 3650")
	}
	updated := 0
	err := h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		if err := tx.SetMeta(ctx, metaReviewDays, strconv.Itoa(days)); err != nil {
			return err
		}
		flows, err := tx.ListFlows(ctx)
		if err != nil {
			return err
		}
		for _, flow := range flows {
			if flow.State == domain.FlowStateDeleted {
				continue
			}
			if flow.PolicySnapshot.ExpireAfterDays == days {
				continue
			}
			expected := flow.Version
			flow.PolicySnapshot.ExpireAfterDays = days
			flow.Version = expected + 1
			flow.UpdatedAt = time.Now().UTC()
			if err := tx.UpsertFlowCAS(ctx, flow, expected); err != nil {
				return err
			}
			updated++
		}
		return nil
	})
	if err != nil {
		return "", "", err
	}
	return marshalToolResult(map[string]any{"status": "ok", "days": days, "updated": updated}), "", nil
}

func (h *Harness) setDeferDays(ctx context.Context, days int) (string, string, error) {
	if days <= 0 || days > 365 {
		return "", "", fmt.Errorf("days must be between 1 and 365")
	}
	err := h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		return tx.SetMeta(ctx, metaDeferDays, strconv.Itoa(days))
	})
	if err != nil {
		return "", "", err
	}
	return marshalToolResult(map[string]any{"status": "ok", "days": days}), "", nil
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
		h.clearProjectionPending(&state)
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
		h.clearProjectionPending(&state)
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
	h.clearProjectionPending(&state)
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
		state := h.getThreadState(threadID)
		state.PendingAction = "schedule_delete"
		h.clearProjectionPending(&state)
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
		state.PendingAction = "schedule_delete"
		h.clearProjectionPending(&state)
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
	h.clearProjectionPending(&state)
	state.CandidateIDs = nil
	h.setThreadState(threadID, state)

	return marshalToolResult(map[string]any{"status": "needs_confirmation", "action": state.PendingAction, "target": h.flowToolPayload(ctx, flow)}), "", nil
}

func (h *Harness) setDeleteProjectionState(ctx context.Context, threadID string, query string, scope string) (string, string, error) {
	query = strings.TrimSpace(strings.ToLower(query))
	if query == "" {
		return marshalToolResult(map[string]any{"status": "needs_query"}), "", nil
	}
	scope = strings.TrimSpace(strings.ToLower(scope))
	if scope == "" {
		scope = "auto"
	}

	candidates, err := h.findProjectionDeleteCandidates(ctx, query, scope)
	if err != nil {
		return "", "", err
	}
	if len(candidates) == 0 {
		return marshalToolResult(map[string]any{"status": "not_found"}), "", nil
	}
	if len(candidates) > 1 {
		state := h.getThreadState(threadID)
		state.PendingAction = "schedule_delete_projection"
		state.CandidateIDs = nil
		state.ProjectionCandidateSet = map[string][]string{}
		state.ProjectionCandidateKeys = make([]string, 0, len(candidates))
		for _, c := range candidates {
			state.ProjectionCandidateKeys = append(state.ProjectionCandidateKeys, c.Label)
			state.ProjectionCandidateSet[c.Label] = c.ItemIDs
		}
		h.setThreadState(threadID, state)

		options := make([]map[string]any, 0, len(candidates))
		for i, c := range candidates {
			options = append(options, map[string]any{
				"choice":       i + 1,
				"label":        c.Label,
				"target_count": len(c.ItemIDs),
				"subject_type": c.SubjectType,
			})
		}
		return marshalToolResult(map[string]any{"status": "needs_selection", "options": options}), "", nil
	}

	state := h.getThreadState(threadID)
	state.PendingAction = "schedule_delete_projection"
	state.CandidateIDs = nil
	state.PendingProjectionIDs = candidates[0].ItemIDs
	state.PendingProjectionLabel = candidates[0].Label
	state.ProjectionCandidateKeys = nil
	state.ProjectionCandidateSet = nil
	h.setThreadState(threadID, state)

	return marshalToolResult(map[string]any{
		"status": "needs_confirmation",
		"action": state.PendingAction,
		"projection": map[string]any{
			"label":        candidates[0].Label,
			"target_count": len(candidates[0].ItemIDs),
			"subject_type": candidates[0].SubjectType,
		},
	}), "", nil
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

	if len(state.ProjectionCandidateKeys) > 0 {
		if idx, err := strconv.Atoi(trimmed); err == nil && idx >= 1 && idx <= len(state.ProjectionCandidateKeys) {
			label := state.ProjectionCandidateKeys[idx-1]
			ids := state.ProjectionCandidateSet[label]
			state.PendingProjectionIDs = ids
			state.PendingProjectionLabel = label
			state.ProjectionCandidateKeys = nil
			state.ProjectionCandidateSet = nil
			h.setThreadState(threadID, state)
			return marshalToolResult(map[string]any{
				"status": "needs_confirmation",
				"action": "schedule_delete_projection",
				"projection": map[string]any{
					"label":        label,
					"target_count": len(ids),
				},
			}), "", nil
		}
	}

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
			if state.PendingAction != "schedule_delete_projection" || len(state.PendingProjectionIDs) == 0 {
				state.PendingAction = ""
				h.setThreadState(threadID, state)
				return marshalToolResult(map[string]any{"status": "needs_selection"}), "", nil
			}
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
		case "schedule_delete_projection":
			out, err = h.applyScheduleDeleteProjection(ctx, state.PendingProjectionIDs, state.PendingProjectionLabel)
		default:
			err = fmt.Errorf("unsupported pending action %q", state.PendingAction)
		}
		state.PendingAction = ""
		state.PendingProjectionIDs = nil
		state.PendingProjectionLabel = ""
		h.setThreadState(threadID, state)
		if err != nil {
			return "", "", err
		}
		return out, "", nil
	}
	if trimmed == "no" && state.PendingAction != "" {
		state.PendingAction = ""
		state.PendingProjectionIDs = nil
		state.PendingProjectionLabel = ""
		state.ProjectionCandidateKeys = nil
		state.ProjectionCandidateSet = nil
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

func (h *Harness) applyScheduleDeleteProjection(ctx context.Context, itemIDs []string, label string) (string, error) {
	decision := h.getDecisionService()
	if decision == nil {
		return "", fmt.Errorf("decision service is not configured")
	}
	if len(itemIDs) == 0 {
		return marshalToolResult(map[string]any{"status": "not_found"}), nil
	}
	applied := 0
	for _, itemID := range itemIDs {
		if strings.TrimSpace(itemID) == "" {
			continue
		}
		if err := decision.ApplyAIDecision(ctx, itemID, "delete"); err != nil {
			return "", err
		}
		applied++
	}
	if strings.TrimSpace(label) == "" {
		label = "projection"
	}
	return marshalToolResult(map[string]any{"status": "done", "action": "scheduled_delete_projection", "label": label, "target_count": applied}), nil
}

type projectionCandidate struct {
	Label       string
	SubjectType string
	ItemIDs     []string
}

func (h *Harness) findProjectionDeleteCandidates(ctx context.Context, query string, scope string) ([]projectionCandidate, error) {
	query = strings.TrimSpace(strings.ToLower(query))
	results := make([]projectionCandidate, 0)
	err := h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flows, err := tx.ListFlows(ctx)
		if err != nil {
			return err
		}

		if scope == "series" || scope == "auto" {
			groups := map[string]projectionCandidate{}
			for _, f := range flows {
				if strings.ToLower(strings.TrimSpace(f.SubjectType)) != "season" {
					continue
				}
				seasonID := projectionSubjectID(f.ItemID)
				if seasonID == "" {
					continue
				}
				episodes, err := tx.ListMediaBySubject(ctx, "season", seasonID)
				if err != nil {
					return err
				}
				seriesName := ""
				seriesID := ""
				for _, ep := range episodes {
					if strings.TrimSpace(ep.SeriesName) != "" {
						seriesName = strings.TrimSpace(ep.SeriesName)
					}
					if strings.TrimSpace(ep.SeriesID) != "" {
						seriesID = strings.TrimSpace(ep.SeriesID)
					}
					if seriesName != "" && seriesID != "" {
						break
					}
				}
				if seriesName == "" {
					continue
				}
				hay := strings.ToLower(seriesName + " " + f.DisplayName)
				if !strings.Contains(hay, query) {
					continue
				}
				key := strings.ToLower(seriesID)
				if key == "" {
					key = strings.ToLower(seriesName)
				}
				group := groups[key]
				if group.Label == "" {
					group.Label = seriesName
					group.SubjectType = "season"
				}
				group.ItemIDs = append(group.ItemIDs, f.ItemID)
				groups[key] = group
			}
			for _, g := range groups {
				results = append(results, g)
			}
			if len(results) > 0 {
				return nil
			}
		}

		for _, f := range flows {
			subject := strings.ToLower(strings.TrimSpace(f.SubjectType))
			if scope == "season" && subject != "season" {
				continue
			}
			if scope == "series" && subject != "series" {
				continue
			}
			if scope == "auto" && subject != "season" && subject != "series" {
				continue
			}
			hay := strings.ToLower(strings.TrimSpace(f.DisplayName + " " + f.ItemID))
			if strings.Contains(hay, query) {
				results = append(results, projectionCandidate{Label: strings.TrimSpace(f.DisplayName), SubjectType: subject, ItemIDs: []string{f.ItemID}})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(results, func(i, j int) bool {
		return strings.ToLower(results[i].Label) < strings.ToLower(results[j].Label)
	})
	return results, nil
}

func projectionSubjectID(targetID string) string {
	parts := strings.SplitN(strings.TrimSpace(targetID), ":", 3)
	if len(parts) != 3 || parts[0] != "target" {
		return ""
	}
	return parts[2]
}

func (h *Harness) fuzzySearchTargets(ctx context.Context, query string, subjectType string, limit int) (string, string, error) {
	query = strings.TrimSpace(strings.ToLower(query))
	subjectType = strings.TrimSpace(strings.ToLower(subjectType))
	if query == "" {
		return marshalToolResult(map[string]any{"status": "needs_query"}), "", nil
	}
	if limit <= 0 || limit > 30 {
		limit = 12
	}
	matches, err := h.findFlowMatchesFiltered(ctx, query, subjectType)
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
	matches := make([]domain.Flow, 0)
	query = strings.TrimSpace(strings.ToLower(query))
	subjectType = strings.TrimSpace(strings.ToLower(subjectType))
	err := h.repository.WithTx(ctx, func(tx repo.TxRepository) error {
		flows, err := tx.ListFlows(ctx)
		if err != nil {
			return err
		}
		for _, f := range flows {
			if subjectType != "" && strings.ToLower(strings.TrimSpace(f.SubjectType)) != subjectType {
				continue
			}
			hay := strings.ToLower(strings.TrimSpace(f.DisplayName + " " + f.ItemID))
			if strings.Contains(hay, query) {
				matches = append(matches, f)
			}
		}
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

func (h *Harness) clearProjectionPending(state *threadState) {
	if state == nil {
		return
	}
	state.PendingProjectionIDs = nil
	state.PendingProjectionLabel = ""
	state.ProjectionCandidateKeys = nil
	state.ProjectionCandidateSet = nil
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

	if len(state.ProjectionCandidateKeys) > 0 {
		b.WriteString("projection_candidates=\n")
		for i, label := range state.ProjectionCandidateKeys {
			b.WriteString("- ")
			b.WriteString(strconv.Itoa(i + 1))
			b.WriteString(": ")
			b.WriteString(label)
			b.WriteString(" (")
			b.WriteString(strconv.Itoa(len(state.ProjectionCandidateSet[label])))
			b.WriteString(" targets)\n")
		}
	} else {
		b.WriteString("projection_candidates=none\n")
	}
	if len(state.PendingProjectionIDs) > 0 {
		b.WriteString("pending_projection=")
		b.WriteString(strings.TrimSpace(state.PendingProjectionLabel))
		if strings.TrimSpace(state.PendingProjectionLabel) == "" {
			b.WriteString("projection")
		}
		b.WriteString(" (")
		b.WriteString(strconv.Itoa(len(state.PendingProjectionIDs)))
		b.WriteString(" targets)\n")
	} else {
		b.WriteString("pending_projection=none\n")
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
