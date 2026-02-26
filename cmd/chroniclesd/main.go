package main

import (
	"flag"
	"fmt"
	"log"

	"chronicles/internal/config"
)

func main() {
	cfgPath := flag.String("config", "chronicles.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	fmt.Printf("chroniclesd node=%s adapters(socket=%t kafka=%t rabbitmq=%t) s3_backup=%t\n",
		cfg.Server.NodeID,
		cfg.Ingest.Socket.Enabled,
		cfg.Ingest.Kafka.Enabled,
		cfg.Ingest.RabbitMQ.Enabled,
		cfg.Backup.S3.Enabled,
	)
}
