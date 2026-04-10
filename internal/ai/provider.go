package ai

import (
	"context"
	"fmt"
	"strings"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/openai/openai-go/shared"
)

const (
	ProviderOpenAICompatible = "openai_compatible"
)

type ProviderConfig struct {
	Provider string
	APIKey   string
	BaseURL  string
}

type ToolDefinition struct {
	Name        string
	Description string
	Parameters  map[string]any
}

type ToolCall struct {
	ID        string
	Name      string
	Arguments string
}

type ChatMessage struct {
	Role       string
	Content    string
	ToolCallID string
	ToolCalls  []ToolCall
}

type ChatResponse struct {
	Content   string
	ToolCalls []ToolCall
}

type ChatProvider interface {
	Complete(context.Context, string, []ChatMessage, []ToolDefinition) (ChatResponse, error)
}

func NewProvider(cfg ProviderConfig) (ChatProvider, error) {
	provider := strings.TrimSpace(cfg.Provider)
	if provider == "" {
		provider = ProviderOpenAICompatible
	}
	switch provider {
	case ProviderOpenAICompatible:
		return NewOpenAICompatibleProvider(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported AI provider: %s", provider)
	}
}

type OpenAICompatibleProvider struct {
	client openai.Client
}

func NewOpenAICompatibleProvider(cfg ProviderConfig) *OpenAICompatibleProvider {
	opts := make([]option.RequestOption, 0, 2)
	if strings.TrimSpace(cfg.APIKey) != "" {
		opts = append(opts, option.WithAPIKey(strings.TrimSpace(cfg.APIKey)))
	}
	if strings.TrimSpace(cfg.BaseURL) != "" {
		opts = append(opts, option.WithBaseURL(strings.TrimSpace(cfg.BaseURL)))
	}
	return &OpenAICompatibleProvider{client: openai.NewClient(opts...)}
}

func (p *OpenAICompatibleProvider) Complete(ctx context.Context, model string, messages []ChatMessage, tools []ToolDefinition) (ChatResponse, error) {
	if strings.TrimSpace(model) == "" {
		return ChatResponse{}, fmt.Errorf("model is required")
	}

	requestMessages := make([]openai.ChatCompletionMessageParamUnion, 0, len(messages))
	for _, msg := range messages {
		role := strings.TrimSpace(msg.Role)
		switch role {
		case "system":
			requestMessages = append(requestMessages, openai.SystemMessage(msg.Content))
		case "user":
			requestMessages = append(requestMessages, openai.UserMessage(msg.Content))
		case "tool":
			requestMessages = append(requestMessages, openai.ToolMessage(msg.Content, msg.ToolCallID))
		case "assistant":
			if len(msg.ToolCalls) == 0 {
				requestMessages = append(requestMessages, openai.AssistantMessage(msg.Content))
				continue
			}
			toolCalls := make([]openai.ChatCompletionMessageToolCallParam, 0, len(msg.ToolCalls))
			for _, tc := range msg.ToolCalls {
				toolCalls = append(toolCalls, openai.ChatCompletionMessageToolCallParam{
					ID: tc.ID,
					Function: openai.ChatCompletionMessageToolCallFunctionParam{
						Name:      tc.Name,
						Arguments: tc.Arguments,
					},
				})
			}
			assistant := openai.ChatCompletionAssistantMessageParam{ToolCalls: toolCalls}
			if strings.TrimSpace(msg.Content) != "" {
				assistant.Content = openai.ChatCompletionAssistantMessageParamContentUnion{OfString: openai.String(msg.Content)}
			}
			requestMessages = append(requestMessages, openai.ChatCompletionMessageParamUnion{OfAssistant: &assistant})
		default:
			return ChatResponse{}, fmt.Errorf("unsupported chat message role: %s", role)
		}
	}

	requestTools := make([]openai.ChatCompletionToolParam, 0, len(tools))
	for _, tool := range tools {
		requestTools = append(requestTools, openai.ChatCompletionToolParam{
			Function: shared.FunctionDefinitionParam{
				Name:        tool.Name,
				Description: openai.String(tool.Description),
				Parameters:  shared.FunctionParameters(tool.Parameters),
			},
		})
	}

	resp, err := p.client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model:    shared.ChatModel(model),
		Messages: requestMessages,
		Tools:    requestTools,
	})
	if err != nil {
		return ChatResponse{}, err
	}
	if len(resp.Choices) == 0 {
		return ChatResponse{}, fmt.Errorf("chat completion returned no choices")
	}

	choice := resp.Choices[0]
	toolCalls := make([]ToolCall, 0, len(choice.Message.ToolCalls))
	for _, tc := range choice.Message.ToolCalls {
		toolCalls = append(toolCalls, ToolCall{ID: tc.ID, Name: tc.Function.Name, Arguments: tc.Function.Arguments})
	}

	return ChatResponse{Content: strings.TrimSpace(choice.Message.Content), ToolCalls: toolCalls}, nil
}
