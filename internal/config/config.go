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
	Socket   AdapterConfig  `mapstructure:"socket"`
	Kafka    AdapterConfig  `mapstructure:"kafka"`
	RabbitMQ RabbitMQConfig `mapstructure:"rabbitmq"`
}

type AdapterConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

type RabbitMQConfig struct {
	Enabled       bool                 `mapstructure:"enabled"`
	URL           string               `mapstructure:"url"`
	Endpoints     []string             `mapstructure:"endpoints"`
	Exchange      string               `mapstructure:"exchange"`
	Queue         string               `mapstructure:"queue"`
	RoutingKeys   []string             `mapstructure:"routing_keys"`
	ConsumerTag   string               `mapstructure:"consumer_tag"`
	PrefetchCount int                  `mapstructure:"prefetch_count"`
	ManualAck     bool                 `mapstructure:"manual_ack"`
	TLS           RabbitMQTLSConfig    `mapstructure:"tls"`
	Auth          RabbitMQAuthConfig   `mapstructure:"auth"`
	Parser        RabbitMQParserConfig `mapstructure:"parser"`
	Workers       int                  `mapstructure:"workers"`
	DeliveryQueue int                  `mapstructure:"delivery_queue"`
}

type RabbitMQTLSConfig struct {
	Enabled            bool   `mapstructure:"enabled"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
	ServerName         string `mapstructure:"server_name"`
	CAFile             string `mapstructure:"ca_file"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
}

type RabbitMQAuthConfig struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type RabbitMQParserConfig struct {
	RequireTenantFields bool `mapstructure:"require_tenant_fields"`
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
	v.SetDefault("ingest.rabbitmq.prefetch_count", 32)
	v.SetDefault("ingest.rabbitmq.manual_ack", true)
	v.SetDefault("ingest.rabbitmq.workers", 4)
	v.SetDefault("ingest.rabbitmq.delivery_queue", 256)
	v.SetDefault("ingest.rabbitmq.parser.require_tenant_fields", true)
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
	if c.Ingest.RabbitMQ.Enabled {
		if !c.Ingest.RabbitMQ.ManualAck {
			return fmt.Errorf("ingest.rabbitmq.manual_ack must be true")
		}
		if c.Ingest.RabbitMQ.Queue == "" {
			return fmt.Errorf("ingest.rabbitmq.queue is required when enabled")
		}
		if c.Ingest.RabbitMQ.Exchange == "" {
			return fmt.Errorf("ingest.rabbitmq.exchange is required when enabled")
		}
		if c.Ingest.RabbitMQ.PrefetchCount < 1 {
			return fmt.Errorf("ingest.rabbitmq.prefetch_count must be >= 1")
		}
	}
	return nil
}
