package config

import (
	"github.com/Slach/clickhouse-timeline/pkg/types"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
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

type Config struct {
	Contexts []Context `yaml:"contexts"`
	Logging  Logging   `yaml:"logging"`
	UI       UI        `yaml:"ui"`
}

func Load(cliInstance *types.CLI, home string) (*Config, error) {
	configPath := filepath.Join(home, "clickhouse-timeline.yml")
	if cliInstance != nil && cliInstance.ConfigPath != "" {
		configPath = cliInstance.ConfigPath
	}

	data, readErr := os.ReadFile(configPath)
	if readErr != nil {
		return nil, errors.WithStack(readErr) // Wrap with stack trace
	}

	var cfg Config
	// Set default values
	cfg.UI.UsingMouse = true

	if unmarshalErr := yaml.Unmarshal(data, &cfg); unmarshalErr != nil {
		return nil, errors.WithStack(unmarshalErr) // Wrap with stack trace
	}

	return &cfg, nil
}
