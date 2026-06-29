package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	anthropicoption "github.com/anthropics/anthropic-sdk-go/option"
	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/openai/openai-go/shared"
)

const (
	ProviderOpenAICompatible = "openai_compatible"
	ProviderAnthropic        = "anthropic"
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
	case ProviderAnthropic:
		return NewAnthropicProvider(cfg), nil
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

type AnthropicProvider struct {
	client anthropic.Client
}

func NewAnthropicProvider(cfg ProviderConfig) *AnthropicProvider {
	opts := make([]anthropicoption.RequestOption, 0, 1)
	if strings.TrimSpace(cfg.APIKey) != "" {
		opts = append(opts, anthropicoption.WithAPIKey(strings.TrimSpace(cfg.APIKey)))
	}
	return &AnthropicProvider{client: anthropic.NewClient(opts...)}
}

func (p *AnthropicProvider) Complete(ctx context.Context, model string, messages []ChatMessage, tools []ToolDefinition) (ChatResponse, error) {
	if strings.TrimSpace(model) == "" {
		return ChatResponse{}, fmt.Errorf("model is required")
	}

	var systemBlocks []anthropic.TextBlockParam
	var msgParams []anthropic.MessageParam

	for _, msg := range messages {
		role := strings.TrimSpace(msg.Role)
		switch role {
		case "system":
			systemBlocks = append(systemBlocks, anthropic.TextBlockParam{Text: msg.Content})
		case "user":
			msgParams = append(msgParams, anthropic.NewUserMessage(anthropic.NewTextBlock(msg.Content)))
		case "tool":
			block := anthropic.ToolResultBlockParam{
				ToolUseID: msg.ToolCallID,
				Content: []anthropic.ToolResultBlockParamContentUnion{
					{OfText: &anthropic.TextBlockParam{Text: msg.Content}},
				},
			}
			msgParams = append(msgParams, anthropic.NewUserMessage(
				anthropic.ContentBlockParamUnion{OfToolResult: &block},
			))
		case "assistant":
			var blocks []anthropic.ContentBlockParamUnion
			if strings.TrimSpace(msg.Content) != "" {
				blocks = append(blocks, anthropic.NewTextBlock(msg.Content))
			}
			for _, tc := range msg.ToolCalls {
				var input any
				if err := json.Unmarshal([]byte(tc.Arguments), &input); err != nil {
					input = map[string]any{}
				}
				blocks = append(blocks, anthropic.ContentBlockParamUnion{
					OfToolUse: &anthropic.ToolUseBlockParam{ID: tc.ID, Name: tc.Name, Input: input},
				})
			}
			msgParams = append(msgParams, anthropic.NewAssistantMessage(blocks...))
		default:
			return ChatResponse{}, fmt.Errorf("unsupported chat message role: %s", role)
		}
	}

	toolParams := make([]anthropic.ToolUnionParam, 0, len(tools))
	for _, t := range tools {
		props, _ := t.Parameters["properties"]
		var required []string
		if r, ok := t.Parameters["required"].([]any); ok {
			for _, v := range r {
				if s, ok := v.(string); ok {
					required = append(required, s)
				}
			}
		}
		tp := anthropic.ToolParam{
			Name:        t.Name,
			Description: anthropic.String(t.Description),
			InputSchema: anthropic.ToolInputSchemaParam{Properties: props, Required: required},
		}
		toolParams = append(toolParams, anthropic.ToolUnionParam{OfTool: &tp})
	}

	resp, err := p.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     anthropic.Model(model),
		MaxTokens: 16000,
		Messages:  msgParams,
		System:    systemBlocks,
		Tools:     toolParams,
		Thinking:  anthropic.ThinkingConfigParamUnion{OfAdaptive: &anthropic.ThinkingConfigAdaptiveParam{}},
	})
	if err != nil {
		return ChatResponse{}, err
	}

	var content strings.Builder
	var toolCalls []ToolCall
	for _, block := range resp.Content {
		switch b := block.AsAny().(type) {
		case anthropic.TextBlock:
			content.WriteString(b.Text)
		case anthropic.ToolUseBlock:
			args, _ := json.Marshal(b.Input)
			toolCalls = append(toolCalls, ToolCall{ID: b.ID, Name: b.Name, Arguments: string(args)})
		}
	}

	return ChatResponse{Content: strings.TrimSpace(content.String()), ToolCalls: toolCalls}, nil
}
