package config

import (
	"os"
	"path/filepath"
	"time"

	"github.com/Slach/clickhouse-timeline/pkg/types"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type Context struct {
	Name      string `yaml:"name"`
	Host      string `yaml:"host"`
	Port      int    `yaml:"port"`
	Database  string `yaml:"database"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	Protocol  string `yaml:"protocol"` // http or native
	Secure    bool   `yaml:"secure"`
	TLSVerify bool   `yaml:"tls_verify"`
	TLSCert   string `yaml:"tls_cert"`
	TLSKey    string `yaml:"tls_key"`
	TLSCa     string `yaml:"tls_ca"`
}

type Logging struct {
	Level string `yaml:"level"` // debug, info, warn, error
}

type UI struct {
	UsingMouse bool `yaml:"using_mouse"` // Enable mouse support (default: true)
}

// ExpertConfig holds configuration for the expert LLM agent.
type ExpertConfig struct {
	Provider           string        `yaml:"provider"`          // "openai", "anthropic", "ollama", "groq"
	Model              string        `yaml:"model"`             // e.g. "gpt-4o", "claude-sonnet-4-20250514"
	APIKey             string        `yaml:"api_key"`           // direct API key
	APIKeyEnv          string        `yaml:"api_key_env"`       // env var name for API key
	SkillsRepo         string        `yaml:"skills_repo"`       // default: https://github.com/Altinity/Skills
	BaseURL            string        `yaml:"base_url"`          // custom endpoint URL
	LlmLogLevel        string        `yaml:"llm_log_level"`     // gollm log level: debug, info, warn, error
	LlmTimeoutRaw      string        `yaml:"llm_timeout"`       // LLM request timeout, e.g. "5m", "120s" (default: 5m)
	LlmTimeout         time.Duration `yaml:"-"`                 // parsed from LlmTimeoutRaw
	LlmRetriesCount    int           `yaml:"llm_retries"`       // number of retries on HTTP 429 (default: 4)
	LlmRetriesPauseRaw string        `yaml:"llm_retries_pause"` // initial pause between retries, e.g. "1s" (default: 1s)
	LlmRetriesPause    time.Duration `yaml:"-"`                 // parsed from LlmRetriesPauseRaw
}

// ResolveAPIKey returns the API key from config or environment.
func (c *ExpertConfig) ResolveAPIKey() string {
	if c.APIKey != "" {
		return c.APIKey
	}
	if c.APIKeyEnv != "" {
		if v := os.Getenv(c.APIKeyEnv); v != "" {
			return v
		}
	}
	// Fallback to standard env vars by provider name
	providerEnvMap := map[string]string{
		"anthropic":  "ANTHROPIC_API_KEY",
		"openai":     "OPENAI_API_KEY",
		"groq":       "GROQ_API_KEY",
		"openrouter": "OPENROUTER_API_KEY",
		"deepseek":   "DEEPSEEK_API_KEY",
		"mistral":    "MISTRAL_API_KEY",
		"cohere":     "COHERE_API_KEY",
		"google":     "GOOGLE_API_KEY",
	}
	if envName, ok := providerEnvMap[c.Provider]; ok {
		if v := os.Getenv(envName); v != "" {
			return v
		}
	}
	// Last resort: try common env vars
	for _, envName := range []string{"OPENAI_API_KEY", "ANTHROPIC_API_KEY", "OPENROUTER_API_KEY"} {
		if v := os.Getenv(envName); v != "" {
			return v
		}
	}
	return ""
}

// GetSkillsRepo returns the skills repo URL with a default fallback.
func (c *ExpertConfig) GetSkillsRepo() string {
	if c.SkillsRepo != "" {
		return c.SkillsRepo
	}
	return "https://github.com/Altinity/Skills"
}

// ExpertDefaults fills in missing config values with defaults.
func (c *ExpertConfig) ExpertDefaults() {
	if c.Provider == "" {
		c.Provider = "openai"
	}
	if c.Model == "" {
		switch c.Provider {
		case "anthropic":
			c.Model = "claude-sonnet-4-20250514"
		case "openai":
			c.Model = "gpt-4o"
		default:
			c.Model = "gpt-4o"
		}
	}
}

type Config struct {
	Contexts []Context    `yaml:"contexts"`
	Logging  Logging      `yaml:"logging"`
	UI       UI           `yaml:"ui"`
	Expert   ExpertConfig `yaml:"expert"`
}

func Load(cliInstance *types.CLI, home string) (*Config, error) {
	configPath := filepath.Join(home, "clickhouse-timeline.yml")
	if cliInstance != nil && cliInstance.ConfigPath != "" {
		configPath = cliInstance.ConfigPath
	}

	data, readErr := os.ReadFile(configPath)
	if readErr != nil {
		return nil, errors.WithStack(readErr)
	}

	var cfg Config
	cfg.UI.UsingMouse = true

	if unmarshalErr := yaml.Unmarshal(data, &cfg); unmarshalErr != nil {
		return nil, errors.WithStack(unmarshalErr)
	}

	// Parse LLM timeout
	if cfg.Expert.LlmTimeoutRaw != "" {
		dur, err := time.ParseDuration(cfg.Expert.LlmTimeoutRaw)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid expert.llm_timeout %q", cfg.Expert.LlmTimeoutRaw)
		}
		cfg.Expert.LlmTimeout = dur
	} else {
		cfg.Expert.LlmTimeout = 5 * time.Minute
	}

	// Parse LLM retries
	if cfg.Expert.LlmRetriesCount == 0 {
		cfg.Expert.LlmRetriesCount = 4
	}
	if cfg.Expert.LlmRetriesPauseRaw != "" {
		dur, err := time.ParseDuration(cfg.Expert.LlmRetriesPauseRaw)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid expert.llm_retries_pause %q", cfg.Expert.LlmRetriesPauseRaw)
		}
		cfg.Expert.LlmRetriesPause = dur
	} else {
		cfg.Expert.LlmRetriesPause = 1 * time.Second
	}

	return &cfg, nil
}
