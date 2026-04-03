package expert

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Slach/clickhouse-timeline/pkg/client"
	"github.com/Slach/clickhouse-timeline/pkg/config"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/teilomillet/gollm"
	gollmutils "github.com/teilomillet/gollm/utils"
)

// ChatMessage represents a message in the conversation history.
type ChatMessage struct {
	Role    string // "user", "assistant", "system", "tool"
	Content string
	Skill   string // skill name if invoked via /skillname
	Model   string // actual model that responded (from API)
	// Tool call fields (when Role == "tool")
	ToolName   string
	ToolQuery  string // SQL query or args summary
	ToolResult string // truncated result
	ToolError  string
}

// ChatEvent is sent during chat for live UI updates.
type ChatEvent struct {
	Type       string // "tool_start", "tool_done", "model_info", "retry"
	Tool       string
	Query      string
	Result     string
	Error      string
	Model      string
	Attempt    int // current retry attempt (1-based)
	MaxRetries int // total retries configured
}

// ExpertAgent manages LLM interactions with skills and ClickHouse tools.
type ExpertAgent struct {
	llm      gollm.LLM
	skills   []Skill
	chClient *client.Client
	history  []ChatMessage
	cfg      config.ExpertConfig
}

const systemPrompt = `You are a ClickHouse expert assistant. You help users diagnose and optimize their ClickHouse installations.

You have access to the following tools:
- run_clickhouse_query: Execute a read-only SQL query against the connected ClickHouse server

When you need to investigate an issue, use the run_clickhouse_query tool to query system tables.
Always explain your findings clearly and provide actionable recommendations.
Format SQL queries and results for readability.
IMPORTANT: When you need data from ClickHouse, ALWAYS use the run_clickhouse_query tool. Do NOT ask the user to run queries manually or via clickhouse-client.`

// zerologAdapter wraps zerolog to implement gollm utils.Logger interface.
type zerologAdapter struct {
	logger zerolog.Logger
}

func (z *zerologAdapter) Debug(msg string, keysAndValues ...interface{}) {
	z.logger.Debug().Fields(kvToMap(keysAndValues)).Msg(msg)
}

func (z *zerologAdapter) Info(msg string, keysAndValues ...interface{}) {
	z.logger.Info().Fields(kvToMap(keysAndValues)).Msg(msg)
}

func (z *zerologAdapter) Warn(msg string, keysAndValues ...interface{}) {
	z.logger.Warn().Fields(kvToMap(keysAndValues)).Msg(msg)
}

func (z *zerologAdapter) Error(msg string, keysAndValues ...interface{}) {
	z.logger.Error().Fields(kvToMap(keysAndValues)).Msg(msg)
}

func (z *zerologAdapter) SetLevel(_ gollmutils.LogLevel) {}

func kvToMap(keysAndValues []interface{}) map[string]interface{} {
	m := make(map[string]interface{}, len(keysAndValues)/2)
	for i := 0; i+1 < len(keysAndValues); i += 2 {
		if key, ok := keysAndValues[i].(string); ok {
			m[key] = keysAndValues[i+1]
		}
	}
	return m
}

func resolveGollmLogLevel(cfg config.ExpertConfig) gollm.LogLevel {
	if cfg.LlmLogLevel != "" {
		switch strings.ToLower(cfg.LlmLogLevel) {
		case "debug":
			return gollm.LogLevelDebug
		case "info":
			return gollm.LogLevelInfo
		case "warn", "warning":
			return gollm.LogLevelWarn
		case "error":
			return gollm.LogLevelError
		}
	}
	// Inherit from app-level log.Logger level
	appLevel := log.Logger.GetLevel()
	switch {
	case appLevel <= zerolog.DebugLevel:
		return gollm.LogLevelDebug
	case appLevel <= zerolog.InfoLevel:
		return gollm.LogLevelInfo
	case appLevel <= zerolog.WarnLevel:
		return gollm.LogLevelWarn
	default:
		return gollm.LogLevelError
	}
}

// NewExpertAgent creates a new expert agent with the given configuration.
func NewExpertAgent(cfg config.ExpertConfig, skills []Skill, chClient *client.Client) (*ExpertAgent, error) {
	cfg.ExpertDefaults()

	apiKey := cfg.ResolveAPIKey()
	if apiKey == "" {
		return nil, fmt.Errorf("no API key configured for provider %q. Set it in config (expert.api_key), env var (expert.api_key_env), or standard env vars (OPENAI_API_KEY, ANTHROPIC_API_KEY)", cfg.Provider)
	}

	opts := []gollm.ConfigOption{
		gollm.SetProvider(cfg.Provider),
		gollm.SetModel(cfg.Model),
		gollm.SetAPIKey(apiKey),
		gollm.SetMaxTokens(4096),
		gollm.SetLogLevel(resolveGollmLogLevel(cfg)),
		gollm.SetMemory(32000),
		gollm.SetLogger(&zerologAdapter{logger: log.Logger}),
	}

	llm, err := gollm.NewLLM(opts...)
	if err != nil {
		return nil, fmt.Errorf("create LLM: %w", err)
	}

	llm.SetSystemPrompt(systemPrompt, gollm.CacheTypeEphemeral)

	return &ExpertAgent{
		llm:      llm,
		skills:   skills,
		chClient: chClient,
		cfg:      cfg,
	}, nil
}

// Provider returns the configured LLM provider name.
func (a *ExpertAgent) Provider() string { return a.cfg.Provider }

// Model returns the configured LLM model name.
func (a *ExpertAgent) Model() string { return a.cfg.Model }

// SetSkills updates the agent's available skills.
func (a *ExpertAgent) SetSkills(skills []Skill) {
	a.skills = skills
}

// SetClickHouseClient sets or updates the ClickHouse client.
func (a *ExpertAgent) SetClickHouseClient(c *client.Client) {
	a.chClient = c
}

// ChatResult holds the final response and events from a chat call.
type ChatResult struct {
	Content string
	Model   string
	Events  []ChatEvent
	Err     error
}

// Chat sends a message to the agent and returns the result with tool events.
func (a *ExpertAgent) Chat(ctx context.Context, userMsg string, skill *Skill) (*ChatResult, error) {
	return a.chatSync(ctx, userMsg, skill)
}

// openAIResponse is the raw response from OpenAI-compatible APIs.
type openAIResponse struct {
	Choices []struct {
		Message struct {
			Content   string `json:"content"`
			ToolCalls []struct {
				ID       string `json:"id"`
				Type     string `json:"type"`
				Function struct {
					Name      string          `json:"name"`
					Arguments json.RawMessage `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Model string `json:"model"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// openAIRequest builds an OpenAI-compatible request body for tool calling.
type openAIMessage struct {
	Role       string          `json:"role"`
	Content    string          `json:"content,omitempty"`
	ToolCalls  json.RawMessage `json:"tool_calls,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
}

func (a *ExpertAgent) chatSync(ctx context.Context, userMsg string, skill *Skill) (*ChatResult, error) {
	promptText := userMsg
	if skill != nil {
		promptText = fmt.Sprintf("Using the following expert knowledge about %s:\n\n%s\n\nUser question: %s",
			skill.DisplayName, skill.Content, userMsg)
	}

	skillName := ""
	if skill != nil {
		skillName = skill.Name
	}
	a.history = append(a.history, ChatMessage{
		Role:    "user",
		Content: userMsg,
		Skill:   skillName,
	})

	messages := []openAIMessage{
		{Role: "system", Content: systemPrompt},
	}
	for _, msg := range a.llm.GetMemory() {
		messages = append(messages, openAIMessage{
			Role:    msg.Role,
			Content: msg.Content,
		})
	}
	messages = append(messages, openAIMessage{Role: "user", Content: promptText})

	tools := a.buildToolsJSON()
	result := &ChatResult{}

	onRetry := func(attempt int) {
		result.Events = append(result.Events, ChatEvent{
			Type:       "retry",
			Attempt:    attempt,
			MaxRetries: a.cfg.LlmRetriesCount,
		})
	}

	const maxIterations = 10
	var finalContent strings.Builder

	for i := 0; i < maxIterations; i++ {
		resp, err := a.callAPI(ctx, messages, tools, onRetry)
		if err != nil {
			return nil, err
		}

		if resp.Error != nil && resp.Error.Message != "" {
			return nil, fmt.Errorf("API error: %s", resp.Error.Message)
		}

		if len(resp.Choices) == 0 {
			return nil, fmt.Errorf("empty response from LLM")
		}

		// Capture model info
		if resp.Model != "" {
			result.Model = resp.Model
			result.Events = append(result.Events, ChatEvent{
				Type:  "model_info",
				Model: resp.Model,
			})
		}

		choice := resp.Choices[0]

		if choice.Message.Content != "" {
			finalContent.WriteString(choice.Message.Content)
		}

		if len(choice.Message.ToolCalls) == 0 {
			break
		}

		log.Info().Int("tool_calls", len(choice.Message.ToolCalls)).Msg("LLM requested tool calls")

		toolCallsJSON, _ := json.Marshal(choice.Message.ToolCalls)
		messages = append(messages, openAIMessage{
			Role:      "assistant",
			Content:   choice.Message.Content,
			ToolCalls: toolCallsJSON,
		})

		for _, tc := range choice.Message.ToolCalls {
			args, err := parseToolArguments(tc.Function.Arguments)
			queryStr := ""
			if q, ok := args["query"]; ok {
				queryStr = fmt.Sprintf("%v", q)
			}

			result.Events = append(result.Events, ChatEvent{
				Type:  "tool_start",
				Tool:  tc.Function.Name,
				Query: queryStr,
			})

			log.Info().
				Str("tool", tc.Function.Name).
				Str("id", tc.ID).
				Str("query", queryStr).
				Msg("Executing tool call")

			if err != nil {
				log.Error().Err(err).Str("tool", tc.Function.Name).Msg("Failed to parse tool args")
				result.Events = append(result.Events, ChatEvent{
					Type:  "tool_done",
					Tool:  tc.Function.Name,
					Error: fmt.Sprintf("parse args: %v", err),
				})
				messages = append(messages, openAIMessage{
					Role:       "tool",
					Content:    fmt.Sprintf("Error parsing arguments: %v", err),
					ToolCallID: tc.ID,
				})
				continue
			}

			toolResult, err := a.ExecuteToolCall(ctx, tc.Function.Name, args)
			if err != nil {
				log.Error().Err(err).Str("tool", tc.Function.Name).Msg("Tool execution failed")
				result.Events = append(result.Events, ChatEvent{
					Type:  "tool_done",
					Tool:  tc.Function.Name,
					Error: err.Error(),
				})
				messages = append(messages, openAIMessage{
					Role:       "tool",
					Content:    fmt.Sprintf("Error: %v", err),
					ToolCallID: tc.ID,
				})
			} else {
				log.Info().
					Str("tool", tc.Function.Name).
					Int("result_len", len(toolResult)).
					Msg("Tool call succeeded")
				result.Events = append(result.Events, ChatEvent{
					Type:   "tool_done",
					Tool:   tc.Function.Name,
					Result: toolResult,
				})
				messages = append(messages, openAIMessage{
					Role:       "tool",
					Content:    toolResult,
					ToolCallID: tc.ID,
				})
			}

			// Add to visible history
			a.history = append(a.history, ChatMessage{
				Role:       "tool",
				ToolName:   tc.Function.Name,
				ToolQuery:  queryStr,
				ToolResult: truncate(toolResult, 500),
				ToolError:  errStr(err),
			})
		}
		finalContent.Reset()
	}

	result.Content = finalContent.String()

	a.history = append(a.history, ChatMessage{
		Role:    "assistant",
		Content: result.Content,
		Model:   result.Model,
	})
	a.llm.AddToMemory("user", promptText)
	a.llm.AddToMemory("assistant", result.Content)

	return result, nil
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// errRateLimited is returned when the API responds with HTTP 429.
type errRateLimited struct {
	body string
}

func (e *errRateLimited) Error() string {
	return fmt.Sprintf("API rate limited (429): %s", e.body)
}

// rateLimitClassifier retries only on *errRateLimited, fails on everything else.
type rateLimitClassifier struct{}

func (rateLimitClassifier) Classify(err error) retrier.Action {
	if err == nil {
		return retrier.Succeed
	}
	var rl *errRateLimited
	if errors.As(err, &rl) {
		return retrier.Retry
	}
	return retrier.Fail
}

func (a *ExpertAgent) callAPI(ctx context.Context, messages []openAIMessage, tools json.RawMessage, onRetry func(attempt int)) (*openAIResponse, error) {
	// Build request body
	body := map[string]interface{}{
		"model":    a.cfg.Model,
		"messages": messages,
	}
	if tools != nil {
		body["tools"] = tools
		body["tool_choice"] = "auto"
	}
	body["max_tokens"] = 4096

	reqBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	log.Debug().RawJSON("request", reqBody).Msg("LLM API request")

	endpoint := providerEndpoint(a.cfg)
	apiKey := a.cfg.ResolveAPIKey()

	var result *openAIResponse
	attempt := 0
	r := retrier.New(
		retrier.ExponentialBackoff(a.cfg.LlmRetriesCount, a.cfg.LlmRetriesPause),
		rateLimitClassifier{},
	)

	err = r.RunCtx(ctx, func(ctx context.Context) error {
		attempt++
		if attempt > 1 {
			log.Warn().Int("attempt", attempt).Int("max_retries", a.cfg.LlmRetriesCount).Msg("Retrying LLM API call after 429")
			if onRetry != nil {
				onRetry(attempt)
			}
		}

		req, reqErr := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(reqBody))
		if reqErr != nil {
			return fmt.Errorf("create request: %w", reqErr)
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+apiKey)
		if a.cfg.Provider == "openrouter" {
			req.Header.Set("HTTP-Referer", "https://github.com/Slach/clickhouse-timeline")
			req.Header.Set("X-Title", "clickhouse-timeline expert")
		}

		resp, doErr := http.DefaultClient.Do(req)
		if doErr != nil {
			log.Error().Stack().Err(doErr).Str("endpoint", endpoint).Msg("LLM API request failed")
			return fmt.Errorf("API request failed: %w", doErr)
		}
		defer func() {
			if closeErr := resp.Body.Close(); closeErr != nil {
				log.Error().Stack().Err(closeErr).Msg("can't close callAPI resp.Body")
			}
		}()

		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			log.Error().Stack().Err(readErr).Str("endpoint", endpoint).Msg("Failed to read LLM API response")
			return fmt.Errorf("read response: %w", readErr)
		}

		log.Debug().RawJSON("response", respBody).Int("status", resp.StatusCode).Msg("LLM API response")

		if resp.StatusCode == http.StatusTooManyRequests {
			log.Warn().Int("attempt", attempt).Str("endpoint", endpoint).Str("body", string(respBody)).Msg("LLM API rate limited (429), will retry")
			return &errRateLimited{body: string(respBody)}
		}

		if resp.StatusCode != http.StatusOK {
			log.Error().Stack().Int("status", resp.StatusCode).Str("body", string(respBody)).Str("endpoint", endpoint).Msg("LLM API error")
			return fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(respBody))
		}

		var parsed openAIResponse
		if unmarshalErr := json.Unmarshal(respBody, &parsed); unmarshalErr != nil {
			return fmt.Errorf("parse response: %w", unmarshalErr)
		}
		result = &parsed
		return nil
	})

	if err != nil {
		return nil, err
	}
	return result, nil
}

// parseToolArguments handles both forms of tool call arguments:
// - JSON object: {"query": "SELECT 1"}
// - JSON string: "{\"query\": \"SELECT 1\"}" (OpenAI-compatible APIs often return this)
func parseToolArguments(raw json.RawMessage) (map[string]interface{}, error) {
	// Try as object first
	var args map[string]interface{}
	if err := json.Unmarshal(raw, &args); err == nil {
		return args, nil
	}

	// Try as JSON-encoded string
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if err := json.Unmarshal([]byte(s), &args); err == nil {
			return args, nil
		}
		return nil, fmt.Errorf("arguments string is not valid JSON: %s", s)
	}

	return nil, fmt.Errorf("cannot parse arguments: %s", string(raw))
}

func providerEndpoint(cfg config.ExpertConfig) string {
	if cfg.BaseURL != "" {
		return strings.TrimRight(cfg.BaseURL, "/") + "/chat/completions"
	}
	switch cfg.Provider {
	case "openrouter":
		return "https://openrouter.ai/api/v1/chat/completions"
	case "openai":
		return "https://api.openai.com/v1/chat/completions"
	case "anthropic":
		return "https://api.anthropic.com/v1/messages"
	case "groq":
		return "https://api.groq.com/openai/v1/chat/completions"
	case "deepseek":
		return "https://api.deepseek.com/v1/chat/completions"
	case "mistral":
		return "https://api.mistral.ai/v1/chat/completions"
	default:
		return "https://openrouter.ai/api/v1/chat/completions"
	}
}

func (a *ExpertAgent) buildToolsJSON() json.RawMessage {
	if a.chClient == nil {
		return nil
	}

	tools := []map[string]interface{}{
		{
			"type": "function",
			"function": map[string]interface{}{
				"name":        "run_clickhouse_query",
				"description": "Execute a read-only SQL query against the connected ClickHouse server. Only SELECT, SHOW, DESCRIBE, EXISTS queries are allowed. Returns tab-separated results.",
				"parameters": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"query": map[string]interface{}{
							"type":        "string",
							"description": "The SQL SELECT query to execute",
						},
					},
					"required": []string{"query"},
				},
			},
		},
	}

	data, _ := json.Marshal(tools)
	return data
}

func (a *ExpertAgent) buildTools() []gollm.Tool {
	var tools []gollm.Tool
	if a.chClient != nil {
		tools = append(tools, gollm.Tool{
			Type: "function",
			Function: gollm.Function{
				Name:        "run_clickhouse_query",
				Description: "Execute a read-only SQL query against the connected ClickHouse server. Only SELECT, SHOW, DESCRIBE, EXISTS queries are allowed.",
				Parameters: map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"query": map[string]interface{}{
							"type":        "string",
							"description": "The SQL SELECT query to execute",
						},
					},
					"required": []string{"query"},
				},
			},
		})
	}
	return tools
}

// ExecuteToolCall handles a tool call from the LLM.
func (a *ExpertAgent) ExecuteToolCall(_ context.Context, toolName string, args map[string]interface{}) (string, error) {
	switch toolName {
	case "run_clickhouse_query":
		return a.executeClickHouseQuery(args)
	default:
		return "", fmt.Errorf("unknown tool: %s", toolName)
	}
}

func (a *ExpertAgent) executeClickHouseQuery(args map[string]interface{}) (string, error) {
	if a.chClient == nil {
		return "", fmt.Errorf("no ClickHouse connection available")
	}

	query, ok := args["query"].(string)
	if !ok {
		return "", fmt.Errorf("query argument must be a string")
	}

	trimmed := strings.TrimSpace(strings.ToUpper(query))
	if !strings.HasPrefix(trimmed, "SELECT") && !strings.HasPrefix(trimmed, "SHOW") &&
		!strings.HasPrefix(trimmed, "DESCRIBE") && !strings.HasPrefix(trimmed, "EXISTS") &&
		!strings.HasPrefix(trimmed, "WITH") {
		return "", fmt.Errorf("only SELECT, SHOW, DESCRIBE, EXISTS, and WITH (CTE) queries are allowed")
	}

	log.Info().Str("query", query).Msg("Expert agent executing ClickHouse query")

	rows, err := a.chClient.Query(query)
	if err != nil {
		return "", fmt.Errorf("execute query: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error().Err(closeErr).Stack().Msg("can't close rows")
		}
	}()

	return formatRows(rows)
}

func formatRows(rows *sql.Rows) (string, error) {
	columns, err := rows.Columns()
	if err != nil {
		return "", fmt.Errorf("get columns: %w", err)
	}

	var result strings.Builder
	result.WriteString(strings.Join(columns, "\t"))
	result.WriteString("\n")

	rowCount := 0
	for rows.Next() {
		if rowCount >= 100 {
			result.WriteString("\n... (truncated, showing first 100 rows)")
			break
		}

		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return result.String(), fmt.Errorf("scan row: %w", err)
		}

		strs := make([]string, len(values))
		for i, v := range values {
			strs[i] = fmt.Sprintf("%v", v)
		}
		result.WriteString(strings.Join(strs, "\t"))
		result.WriteString("\n")
		rowCount++
	}

	return result.String(), nil
}

// History returns the conversation history.
func (a *ExpertAgent) History() []ChatMessage {
	return a.history
}

// ClearHistory clears the conversation history and LLM memory.
func (a *ExpertAgent) ClearHistory() {
	a.history = nil
	a.llm.ClearMemory()
}
