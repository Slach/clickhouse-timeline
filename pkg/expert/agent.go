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
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/Slach/clickhouse-timeline/pkg/client"
	"github.com/Slach/clickhouse-timeline/pkg/config"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/teilomillet/gollm"
	"github.com/teilomillet/gollm/types"
	gollmutils "github.com/teilomillet/gollm/utils"
)

// ChatMessage represents a message in the conversation history.
type ChatMessage struct {
	Role              string // "user", "assistant", "system", "tool", "reasoning"
	Content           string
	Skill             string // skill name if invoked via /skillname
	Model             string // actual model that responded (from API)
	Reasoning         string // chain-of-thought reasoning (hidden by default)
	ReasonLen         int    // length of reasoning text for display
	ToolTokens        int    // approximate reasoning tokens
	ReasoningExpanded bool   // UI state: is reasoning block expanded?
	// Tool call fields (when Role == "tool")
	ToolName   string
	ToolQuery  string // SQL query or args summary
	ToolResult string // truncated result
	ToolError  string
}

// ChatEvent is sent during chat for live UI updates.
type ChatEvent struct {
	Type       string // "tool_start", "tool_done", "model_info", "retry", "reasoning"
	Tool       string
	Query      string
	Result     string
	Error      string
	Model      string
	Attempt    int    // current retry attempt (1-based)
	MaxRetries int    // total retries configured
	Reasoning  string // chain-of-thought text
	Tokens     int    // approximate reasoning tokens
}

// ExpertAgent manages LLM interactions with skills and ClickHouse tools.
type ExpertAgent struct {
	llm            gollm.LLM
	skills         []Skill
	chClient       *client.Client
	history        []ChatMessage
	cfg            config.ExpertConfig
	activeSkillDir string // path to current skill directory for read_skill_sql
}

const systemPrompt = `You are a ClickHouse expert assistant. You help users diagnose and optimize their ClickHouse installations.

You have access to the following tools:
- run_clickhouse_query: Execute a read-only SQL query against the connected ClickHouse server
- read_skill_sql: Read a .sql file from the current skill directory to get predefined diagnostic queries

When you need to investigate an issue, use the run_clickhouse_query tool to query system tables.
When a skill references SQL files (e.g. "Run reporting SQL queries from files: checks.sql"), use read_skill_sql to load them first, then execute the queries.
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
	Content   string
	Model     string
	Reasoning string // accumulated chain-of-thought
	Events    []ChatEvent
	Err       error
}

// Chat sends a message to the agent and returns the result with tool events.
func (a *ExpertAgent) Chat(ctx context.Context, userMsg string, skill *Skill) (*ChatResult, error) {
	return a.chatSync(ctx, userMsg, skill, nil)
}

// ChatWithProgress is like Chat but sends events to progressCh as they happen.
// The channel is NOT closed by this method; the caller should close it.
func (a *ExpertAgent) ChatWithProgress(ctx context.Context, userMsg string, skill *Skill, progressCh chan<- ChatEvent) (*ChatResult, error) {
	return a.chatSync(ctx, userMsg, skill, progressCh)
}

// openAIResponse is the raw response from OpenAI-compatible APIs.
// We use raw HTTP because gollm.Generate() returns only a string and
// doesn't expose tool calls, reasoning, or model info from the response.
type openAIResponse struct {
	Choices []struct {
		Message struct {
			Content          string           `json:"content"`
			Reasoning        string           `json:"reasoning"`
			ReasoningDetails []struct {
				Format string `json:"format"`
				Index  int    `json:"index"`
				Text   string `json:"text"`
				Type   string `json:"type"`
			} `json:"reasoning_details"`
			ToolCalls []types.ToolCall `json:"tool_calls"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Model string `json:"model"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// openAIMessage is used to build the messages array for the API request.
type openAIMessage struct {
	Role       string          `json:"role"`
	Content    string          `json:"content,omitempty"`
	ToolCalls  json.RawMessage `json:"tool_calls,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
}

func (a *ExpertAgent) chatSync(ctx context.Context, userMsg string, skill *Skill, progressCh chan<- ChatEvent) (*ChatResult, error) {
	promptText := userMsg
	if skill != nil {
		promptText = fmt.Sprintf("Using the following expert knowledge about %s:\n\n%s\n\nUser question: %s",
			skill.DisplayName, skill.Content, userMsg)
		a.activeSkillDir = skill.Path
	} else {
		a.activeSkillDir = ""
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

	tools := a.buildTools()
	var toolsJSON json.RawMessage
	if len(tools) > 0 {
		toolsJSON, _ = json.Marshal(tools)
	}
	result := &ChatResult{}

	sendProgress := func(ev ChatEvent) {
		result.Events = append(result.Events, ev)
		if progressCh != nil {
			select {
			case progressCh <- ev:
			case <-ctx.Done():
			}
		}
	}

	onRetry := func(attempt int) {
		sendProgress(ChatEvent{
			Type:       "retry",
			Attempt:    attempt,
			MaxRetries: a.cfg.LlmRetriesCount,
		})
	}

	maxIterations := a.cfg.MaxIterations
	var finalContent strings.Builder
	var finalReasoning strings.Builder
	hadToolCalls := false

	for i := 0; i < maxIterations; i++ {
		resp, err := a.callAPI(ctx, messages, toolsJSON, onRetry)
		if err != nil {
			return nil, err
		}

		if resp.Error != nil && resp.Error.Message != "" {
			return nil, fmt.Errorf("API error: %s", resp.Error.Message)
		}

		if len(resp.Choices) == 0 {
			return nil, fmt.Errorf("empty response from LLM")
		}

		if resp.Model != "" {
			result.Model = resp.Model
			sendProgress(ChatEvent{
				Type:  "model_info",
				Model: resp.Model,
			})
		}

		choice := resp.Choices[0]

		// Capture reasoning (from top-level field or first reasoning_detail)
		reasoning := choice.Message.Reasoning
		if reasoning == "" && len(choice.Message.ReasoningDetails) > 0 {
			reasoning = choice.Message.ReasoningDetails[0].Text
		}
		if reasoning != "" {
			finalReasoning.WriteString(reasoning)
			finalReasoning.WriteString("\n\n")
			sendProgress(ChatEvent{
				Type:      "reasoning",
				Reasoning: reasoning,
			})
		}

		// Some models embed tool calls as XML tags in content instead of
		// using the proper tool_calls JSON format. Parse them out.
		if len(choice.Message.ToolCalls) == 0 && choice.Message.Content != "" {
			parsed := parseInlineToolCalls(choice.Message.Content)
			if len(parsed) > 0 {
				choice.Message.ToolCalls = parsed
				choice.Message.Content = stripInlineToolCalls(choice.Message.Content)
			}
		}

		if choice.Message.Content != "" {
			finalContent.WriteString(choice.Message.Content)
		}

		if len(choice.Message.ToolCalls) == 0 {
			break
		}

		hadToolCalls = true
		log.Info().Int("tool_calls", len(choice.Message.ToolCalls)).Msg("LLM requested tool calls")

		toolCallsJSON, _ := json.Marshal(choice.Message.ToolCalls)
		messages = append(messages, openAIMessage{
			Role:      "assistant",
			Content:   choice.Message.Content,
			ToolCalls: toolCallsJSON,
		})

		for _, tc := range choice.Message.ToolCalls {
			args, err := getToolArguments(&tc)
			queryStr := ""
			if q, ok := args["query"]; ok {
				queryStr = fmt.Sprintf("%v", q)
			}
			if tc.GetName() == "read_skill_sql" {
				if fn, ok := args["filename"]; ok {
					skillName := filepath.Base(a.activeSkillDir)
					queryStr = fmt.Sprintf("%s/%v", skillName, fn)
				}
			}

			sendProgress(ChatEvent{
				Type:  "tool_start",
				Tool:  tc.GetName(),
				Query: queryStr,
			})

			log.Info().
				Str("tool", tc.GetName()).
				Str("id", tc.ID).
				Str("query", queryStr).
				Msg("Executing tool call")

			if err != nil {
				log.Error().Err(err).Str("tool", tc.GetName()).Msg("Failed to parse tool args")
				sendProgress(ChatEvent{
					Type:  "tool_done",
					Tool:  tc.GetName(),
					Error: fmt.Sprintf("parse args: %v", err),
				})
				messages = append(messages, openAIMessage{
					Role:       "tool",
					Content:    fmt.Sprintf("Error parsing arguments: %v", err),
					ToolCallID: tc.ID,
				})
				continue
			}

			toolResult, err := a.ExecuteToolCall(ctx, tc.GetName(), args)
			if err != nil {
				log.Error().Err(err).Str("tool", tc.GetName()).Msg("Tool execution failed")
				sendProgress(ChatEvent{
					Type:  "tool_done",
					Tool:  tc.GetName(),
					Error: err.Error(),
				})
				messages = append(messages, openAIMessage{
					Role:       "tool",
					Content:    fmt.Sprintf("Error: %v", err),
					ToolCallID: tc.ID,
				})
			} else {
				log.Info().
					Str("tool", tc.GetName()).
					Int("result_len", len(toolResult)).
					Msg("Tool call succeeded")
				sendProgress(ChatEvent{
					Type:   "tool_done",
					Tool:   tc.GetName(),
					Result: toolResult,
				})
				messages = append(messages, openAIMessage{
					Role:       "tool",
					Content:    toolResult,
					ToolCallID: tc.ID,
				})
			}

			a.history = append(a.history, ChatMessage{
				Role:       "tool",
				ToolName:   tc.GetName(),
				ToolQuery:  queryStr,
				ToolResult: truncate(toolResult, 500),
				ToolError:  errStr(err),
			})
		}
		finalContent.Reset()
	}

	// If the loop exhausted iterations with tool calls but no final text,
	// make one more API call asking the LLM to summarize its findings.
	if finalContent.Len() == 0 && hadToolCalls {
		log.Warn().Int("max_iterations", maxIterations).Msg("Tool-call loop exhausted without final response, requesting summary")
		messages = append(messages, openAIMessage{
			Role:    "user",
			Content: "You have gathered enough data. Now summarize your findings and provide actionable recommendations based on all the data you've collected above. Do NOT make any more tool calls.",
		})
		resp, err := a.callAPI(ctx, messages, nil, onRetry) // no tools — force text response
		if err == nil && len(resp.Choices) > 0 {
			choice := resp.Choices[0]
			if choice.Message.Content != "" {
				finalContent.WriteString(choice.Message.Content)
			}
			if resp.Model != "" {
				result.Model = resp.Model
			}
			reasoning := choice.Message.Reasoning
			if reasoning == "" && len(choice.Message.ReasoningDetails) > 0 {
				reasoning = choice.Message.ReasoningDetails[0].Text
			}
			if reasoning != "" {
				finalReasoning.WriteString(reasoning)
				finalReasoning.WriteString("\n\n")
			}
		}
	}

	result.Content = finalContent.String()
	reasoningText := strings.TrimSpace(finalReasoning.String())
	result.Reasoning = reasoningText

	a.history = append(a.history, ChatMessage{
		Role:      "assistant",
		Content:   result.Content,
		Model:     result.Model,
		Reasoning: reasoningText,
		ReasonLen: len(reasoningText),
	})
	a.llm.AddToMemory("user", promptText)
	a.llm.AddToMemory("assistant", result.Content)

	return result, nil
}

// Regex patterns for inline tool call formats that some models use
// instead of the proper tool_calls JSON response format.
var (
	// <tool_call>{"name": "...", "arguments": {...}}</tool_call>
	toolCallTagRe = regexp.MustCompile(`(?s)<tool_call>\s*(\{.*?\})\s*(?:</tool_call>)?`)
	// <run_clickhouse_query>SELECT ... </run_clickhouse_query> or with <sql> wrapper
	toolNameTagRe = regexp.MustCompile(`(?s)<run_clickhouse_query>\s*(?:<sql>\s*)?(.*?)(?:\s*</sql>)?\s*</run_clickhouse_query>`)
)

// parseInlineToolCalls extracts tool calls from XML tags in LLM content.
func parseInlineToolCalls(content string) []types.ToolCall {
	var result []types.ToolCall

	// Format 1: <tool_call>{"name": "...", "arguments": {...}}</tool_call>
	for i, match := range toolCallTagRe.FindAllStringSubmatch(content, -1) {
		if len(match) < 2 {
			continue
		}
		var parsed struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal([]byte(match[1]), &parsed); err != nil {
			log.Warn().Err(err).Str("raw", match[1]).Msg("Failed to parse <tool_call> JSON")
			continue
		}
		tc := types.ToolCall{
			ID:   fmt.Sprintf("inline_call_%d", i),
			Type: "function",
		}
		tc.Function.Name = parsed.Name
		tc.Function.Arguments = parsed.Arguments
		result = append(result, tc)
	}

	// Format 2: <run_clickhouse_query>SQL</run_clickhouse_query>
	for i, match := range toolNameTagRe.FindAllStringSubmatch(content, -1) {
		if len(match) < 2 {
			continue
		}
		toolName := "run_clickhouse_query"
		query := strings.TrimSpace(match[1])
		args, _ := json.Marshal(map[string]string{"query": query})
		tc := types.ToolCall{
			ID:   fmt.Sprintf("inline_named_%d", i),
			Type: "function",
		}
		tc.Function.Name = toolName
		tc.Function.Arguments = args
		result = append(result, tc)
	}

	return result
}

// stripInlineToolCalls removes all tool call XML blocks from content.
func stripInlineToolCalls(content string) string {
	cleaned := toolCallTagRe.ReplaceAllString(content, "")
	cleaned = toolNameTagRe.ReplaceAllString(cleaned, "")
	return strings.TrimSpace(cleaned)
}

// getToolArguments extracts arguments from a tool call, handling both
// direct JSON objects and double-encoded JSON strings (common in OpenAI-compatible APIs).
func getToolArguments(tc *types.ToolCall) (map[string]interface{}, error) {
	args, err := tc.GetArguments()
	if err == nil {
		return args, nil
	}
	// Try double-encoded string: "{\"query\": \"...\"}"
	var s string
	if jsonErr := json.Unmarshal(tc.Function.Arguments, &s); jsonErr == nil {
		if jsonErr = json.Unmarshal([]byte(s), &args); jsonErr == nil {
			return args, nil
		}
	}
	return nil, err
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

// rateLimitClassifier retries only on *errRateLimited.
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
	body := map[string]interface{}{
		"model":      a.cfg.Model,
		"messages":   messages,
		"max_tokens": 4096,
	}
	if tools != nil {
		body["tools"] = json.RawMessage(tools)
		body["tool_choice"] = "auto"
	}

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
			return fmt.Errorf("API request failed: %w", doErr)
		}
		defer func() {
			if closeErr := resp.Body.Close(); closeErr != nil {
				log.Error().Stack().Err(closeErr).Msg("can't close callAPI resp.Body")
			}
		}()

		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("read response: %w", readErr)
		}

		log.Debug().RawJSON("response", respBody).Int("status", resp.StatusCode).Msg("LLM API response")

		if resp.StatusCode == http.StatusTooManyRequests {
			log.Warn().Int("attempt", attempt).Str("body", string(respBody)).Msg("LLM API rate limited (429), will retry")
			return &errRateLimited{body: string(respBody)}
		}

		if resp.StatusCode != http.StatusOK {
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

// buildTools returns tool definitions using gollm types.
// Tools are always declared so the model knows they exist;
// ExecuteToolCall will return an error if the ClickHouse client is not connected.
func (a *ExpertAgent) buildTools() []gollm.Tool {
	tools := []gollm.Tool{
		{
			Type: "function",
			Function: gollm.Function{
				Name:        "run_clickhouse_query",
				Description: "Execute a read-only SQL query against the connected ClickHouse server. Only SELECT, SHOW, DESCRIBE, EXISTS queries are allowed. Returns tab-separated results.",
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
		},
	}
	if a.activeSkillDir != "" {
		tools = append(tools, gollm.Tool{
			Type: "function",
			Function: gollm.Function{
				Name:        "read_skill_sql",
				Description: "Read a .sql file from the current skill directory. Use this to load predefined diagnostic queries referenced in the skill description (e.g. checks.sql, metrics.sql).",
				Parameters: map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"filename": map[string]interface{}{
							"type":        "string",
							"description": "The .sql filename to read (e.g. 'checks.sql', 'metrics.sql')",
						},
					},
					"required": []string{"filename"},
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
	case "read_skill_sql":
		return a.executeReadSkillSQL(args)
	default:
		return "", fmt.Errorf("unknown tool: %s", toolName)
	}
}

func (a *ExpertAgent) executeReadSkillSQL(args map[string]interface{}) (string, error) {
	if a.activeSkillDir == "" {
		return "", fmt.Errorf("no active skill directory")
	}
	filename, ok := args["filename"].(string)
	if !ok {
		return "", fmt.Errorf("filename argument must be a string")
	}
	// Sanitize: only allow simple filenames with .sql extension
	if strings.Contains(filename, "/") || strings.Contains(filename, "\\") || strings.Contains(filename, "..") {
		return "", fmt.Errorf("invalid filename: path traversal not allowed")
	}
	if !strings.HasSuffix(filename, ".sql") {
		return "", fmt.Errorf("only .sql files can be read")
	}
	filePath := filepath.Join(a.activeSkillDir, filename)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", filename, err)
	}
	log.Info().Str("file", filename).Int("bytes", len(data)).Msg("Read skill SQL file")
	return string(data), nil
}

func (a *ExpertAgent) executeClickHouseQuery(args map[string]interface{}) (string, error) {
	if a.chClient == nil {
		return "", fmt.Errorf("no ClickHouse connection available")
	}

	query, ok := args["query"].(string)
	if !ok {
		return "", fmt.Errorf("query argument must be a string")
	}

	trimmed := strings.TrimSpace(query)
	// Strip leading SQL comments (-- line comments and /* block comments */)
	for {
		if strings.HasPrefix(trimmed, "--") {
			if idx := strings.Index(trimmed, "\n"); idx >= 0 {
				trimmed = strings.TrimSpace(trimmed[idx+1:])
			} else {
				trimmed = ""
			}
		} else if strings.HasPrefix(trimmed, "/*") {
			if idx := strings.Index(trimmed, "*/"); idx >= 0 {
				trimmed = strings.TrimSpace(trimmed[idx+2:])
			} else {
				trimmed = ""
			}
		} else {
			break
		}
	}
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "SELECT") && !strings.HasPrefix(upper, "SHOW") &&
		!strings.HasPrefix(upper, "DESCRIBE") && !strings.HasPrefix(upper, "EXISTS") &&
		!strings.HasPrefix(upper, "WITH") {
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
