package mcp

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	MCP    MCPSettings   `yaml:"mcp"`
	Tools  []ToolConfig  `yaml:"tools"`
	Budget BudgetConfig  `yaml:"budget"`
}

type MCPSettings struct {
	ServerName string `yaml:"server_name"`
	Version    string `yaml:"version"`
	Transport  string `yaml:"transport"`
}

type ToolConfig struct {
	PRXSService string `yaml:"prxs_service"`
	MCPName     string `yaml:"mcp_name"`
	Description string `yaml:"description"`
	Enabled     bool   `yaml:"enabled"`
}

type BudgetConfig struct {
	Global  BudgetLimits            `yaml:"global"`
	PerTool map[string]BudgetLimits `yaml:"per_tool"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %v", err)
	}

	if config.MCP.ServerName == "" {
		config.MCP.ServerName = "PRXS MCP Server"
	}
	if config.MCP.Version == "" {
		config.MCP.Version = "0.1.0"
	}
	if config.MCP.Transport == "" {
		config.MCP.Transport = "stdio"
	}

	return &config, nil
}
