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
	Kafka    AdapterConfig `mapstructure:"kafka"`
	RabbitMQ AdapterConfig `mapstructure:"rabbitmq"`
}

type AdapterConfig struct {
	Enabled bool `mapstructure:"enabled"`
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
	return nil
}
