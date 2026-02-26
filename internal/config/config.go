package config

import (
	"fmt"
	"github.com/spf13/viper"
	"strings"
)

type Config struct {
	Server  ServerConfig  `mapstructure:"server"`
	Ingest  IngestConfig  `mapstructure:"ingest"`
	Backup  BackupConfig  `mapstructure:"backup"`
	Feature FeatureConfig `mapstructure:"feature"`
}

type ServerConfig struct {
	NodeID string `mapstructure:"node_id"`
}

type IngestConfig struct {
	Socket   AdapterConfig `mapstructure:"socket"`
	Kafka    KafkaConfig   `mapstructure:"kafka"`
	RabbitMQ AdapterConfig `mapstructure:"rabbitmq"`
}

type AdapterConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

type KafkaConfig struct {
	Enabled        bool             `mapstructure:"enabled"`
	Brokers        []string         `mapstructure:"brokers"`
	Topics         []string         `mapstructure:"topics"`
	GroupID        string           `mapstructure:"group_id"`
	ClientID       string           `mapstructure:"client_id"`
	WorkerCount    int              `mapstructure:"worker_count"`
	MaxPollRecords int              `mapstructure:"max_poll_records"`
	CommitMode     string           `mapstructure:"commit_mode"`
	ParsingMode    string           `mapstructure:"parsing_mode"`
	Fetch          KafkaFetchConfig `mapstructure:"fetch"`
	Auth           KafkaAuthConfig  `mapstructure:"auth"`
}

type KafkaFetchConfig struct {
	MinBytes  int `mapstructure:"min_bytes"`
	MaxBytes  int `mapstructure:"max_bytes"`
	MaxWaitMS int `mapstructure:"max_wait_ms"`
}

type KafkaAuthConfig struct {
	SASL KafkaSASLConfig `mapstructure:"sasl"`
	TLS  KafkaTLSConfig  `mapstructure:"tls"`
}

type KafkaSASLConfig struct {
	Enabled   bool   `mapstructure:"enabled"`
	Mechanism string `mapstructure:"mechanism"`
	Username  string `mapstructure:"username"`
	Password  string `mapstructure:"password"`
}

type KafkaTLSConfig struct {
	Enabled            bool `mapstructure:"enabled"`
	InsecureSkipVerify bool `mapstructure:"insecure_skip_verify"`
}

type BackupConfig struct {
	S3 S3BackupConfig `mapstructure:"s3"`
}

type S3BackupConfig struct {
	Enabled  bool   `mapstructure:"enabled"`
	Provider string `mapstructure:"provider"`
}

type FeatureConfig struct {
	AllowMultipleAdapters bool `mapstructure:"allow_multiple_adapters"`
}

func Load(path string) (Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetEnvPrefix("chronicles")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	setDefaults(v)

	if err := v.ReadInConfig(); err != nil {
		return Config{}, err
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, fmt.Errorf("unmarshal config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("feature.allow_multiple_adapters", true)
	v.SetDefault("backup.s3.provider", "aws-sdk-v2")
	v.SetDefault("ingest.kafka.worker_count", 4)
	v.SetDefault("ingest.kafka.max_poll_records", 500)
	v.SetDefault("ingest.kafka.commit_mode", "after_quorum_commit")
	v.SetDefault("ingest.kafka.parsing_mode", "json_envelope")
}

func (c Config) Validate() error {
	if c.Server.NodeID == "" {
		return fmt.Errorf("server.node_id is required")
	}
	if !c.Feature.AllowMultipleAdapters {
		enabled := 0
		if c.Ingest.Socket.Enabled {
			enabled++
		}
		if c.Ingest.Kafka.Enabled {
			enabled++
		}
		if c.Ingest.RabbitMQ.Enabled {
			enabled++
		}
		if enabled > 1 {
			return fmt.Errorf("multiple adapters enabled while feature.allow_multiple_adapters=false")
		}
	}
	if c.Ingest.Kafka.Enabled {
		if len(c.Ingest.Kafka.Brokers) == 0 {
			return fmt.Errorf("ingest.kafka.brokers is required")
		}
		if len(c.Ingest.Kafka.Topics) == 0 {
			return fmt.Errorf("ingest.kafka.topics is required")
		}
		if c.Ingest.Kafka.GroupID == "" {
			return fmt.Errorf("ingest.kafka.group_id is required")
		}
		if c.Ingest.Kafka.CommitMode != "after_quorum_commit" {
			return fmt.Errorf("ingest.kafka.commit_mode must be after_quorum_commit")
		}
	}
	return nil
}
