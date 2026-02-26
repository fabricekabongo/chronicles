package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadYAMLWithEnvOverride(t *testing.T) {
	t.Setenv("CHRONICLES_INGEST_KAFKA_ENABLED", "true")

	path := filepath.Join(t.TempDir(), "chronicles.yaml")
	content := []byte(`
server:
  node_id: n1
ingest:
  socket:
    enabled: true
  kafka:
    enabled: false
  rabbitmq:
    enabled: true
backup:
  s3:
    enabled: true
    provider: minio
`)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load yaml: %v", err)
	}
	if !cfg.Ingest.Kafka.Enabled {
		t.Fatalf("expected env override to enable kafka")
	}
	if !cfg.Ingest.Socket.Enabled || !cfg.Ingest.RabbitMQ.Enabled {
		t.Fatalf("expected multiple adapters enabled")
	}
}

func TestLoadTOML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chronicles.toml")
	content := []byte(`
[server]
node_id = "n2"

[ingest.socket]
enabled = true

[ingest.kafka]
enabled = false

[ingest.rabbitmq]
enabled = false
`)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load toml: %v", err)
	}
	if cfg.Server.NodeID != "n2" {
		t.Fatalf("unexpected node id: %q", cfg.Server.NodeID)
	}
}

func TestValidateDisallowMultipleAdapters(t *testing.T) {
	cfg := Config{
		Server: ServerConfig{NodeID: "n1"},
		Ingest: IngestConfig{
			Socket:   AdapterConfig{Enabled: true},
			Kafka:    AdapterConfig{Enabled: true},
			RabbitMQ: AdapterConfig{Enabled: false},
		},
		Feature: FeatureConfig{AllowMultipleAdapters: false},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error when multiple adapters are enabled")
	}
}
