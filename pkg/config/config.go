package config

import (
	"os"

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

type Config struct {
	Contexts []Context `yaml:"contexts"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
