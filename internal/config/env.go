package config

import (
	"bufio"
	"os"
	"strings"
)

// Config holds runtime configuration for Neo4j AuraDB, Daytona Cloud, and Nosana GPU endpoints.
type Config struct {
	Neo4jURI      string `json:"neo4j_uri"`
	Neo4jUsername string `json:"neo4j_username"`
	Neo4jPassword string `json:"-"`
	Neo4jDatabase string `json:"neo4j_database"`

	DaytonaAPIKey      string `json:"-"`
	DaytonaAPIURL      string `json:"daytona_api_url"`
	DaytonaTarget      string `json:"daytona_target"`
	DaytonaSandboxID   string `json:"daytona_sandbox_id"`
	DaytonaSandboxName string `json:"daytona_sandbox_name"`
	DaytonaSSHUser     string `json:"-"`
	DaytonaSSHHost     string `json:"daytona_ssh_host"`

	NosanaBaseURL string `json:"nosana_base_url"`
	NosanaAPIKey  string `json:"-"`
	NosanaModel   string `json:"nosana_model"`

	Port string `json:"port"`
}

// Load reads .env (if present) and populates Config from environment variables.
func Load(envPaths ...string) Config {
	paths := append(envPaths, ".env", "../plugfactor/.env", "/Users/whatevergeek/Laboratory/Sandbox/go_stuff/plugfactor/.env")
	for _, p := range paths {
		loadDotEnvFile(p)
	}

	cfg := Config{
		Neo4jURI:      strings.TrimSpace(os.Getenv("NEO4J_URI")),
		Neo4jUsername: envOrDefault("NEO4J_USERNAME", "neo4j"),
		Neo4jPassword: strings.TrimSpace(os.Getenv("NEO4J_PASSWORD")),
		Neo4jDatabase: envOrDefault("NEO4J_DATABASE", "neo4j"),

		DaytonaAPIKey:      strings.TrimSpace(os.Getenv("DAYTONA_API_KEY")),
		DaytonaAPIURL:      envOrDefault("DAYTONA_API_URL", "https://app.daytona.io/api"),
		DaytonaTarget:      envOrDefault("DAYTONA_TARGET", "eu"),
		DaytonaSandboxID:   strings.TrimSpace(os.Getenv("DAYTONA_SANDBOX_ID")),
		DaytonaSandboxName: envOrDefault("DAYTONA_SANDBOX_NAME", "hacksprint"),
		DaytonaSSHUser:     strings.TrimSpace(os.Getenv("DAYTONA_SSH_USER")),
		DaytonaSSHHost:     envOrDefault("DAYTONA_SSH_HOST", "ssh.app.daytona.io"),

		NosanaBaseURL: strings.TrimSpace(os.Getenv("NOSANA_BASE_URL")),
		NosanaAPIKey:  strings.TrimSpace(os.Getenv("NOSANA_API_KEY")),
		NosanaModel:   envOrDefault("NOSANA_MODEL", "Qwen/Qwen2.5-Coder-32B-Instruct"),

		Port: envOrDefault("PORT", "8080"),
	}
	return cfg
}

// Neo4jConfigured returns true if a valid non-placeholder NEO4J_URI and password are set.
func (c Config) Neo4jConfigured() bool {
	return c.Neo4jURI != "" &&
		!strings.Contains(c.Neo4jURI, "REPLACE") &&
		c.Neo4jPassword != "" &&
		!strings.Contains(c.Neo4jPassword, "REPLACE")
}

// DaytonaConfigured returns true if DAYTONA_API_KEY, DAYTONA_SANDBOX_ID, or DAYTONA_SSH_USER is set.
func (c Config) DaytonaConfigured() bool {
	return (c.DaytonaAPIKey != "" && !strings.Contains(c.DaytonaAPIKey, "REPLACE")) ||
		(c.DaytonaSandboxID != "" && !strings.Contains(c.DaytonaSandboxID, "REPLACE")) ||
		(c.DaytonaSSHUser != "" && !strings.Contains(c.DaytonaSSHUser, "REPLACE"))
}

// NosanaConfigured returns true if NOSANA_API_KEY or NOSANA_BASE_URL is set.
func (c Config) NosanaConfigured() bool {
	return (c.NosanaAPIKey != "" && !strings.Contains(c.NosanaAPIKey, "REPLACE")) ||
		(c.NosanaBaseURL != "" && !strings.Contains(c.NosanaBaseURL, "REPLACE"))
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func loadDotEnvFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		k := strings.TrimSpace(parts[0])
		v := strings.Trim(strings.TrimSpace(parts[1]), `"'`)
		if os.Getenv(k) == "" {
			_ = os.Setenv(k, v)
		}
	}
}
