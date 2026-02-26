package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"chronicles/internal/domain"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"
)

type captureAppender struct {
	mu     sync.Mutex
	events []domain.EventEnvelope
}

func (c *captureAppender) Append(_ context.Context, ev domain.EventEnvelope) (domain.CommitMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, ev)
	return domain.CommitMetadata{LSN: uint64(len(c.events))}, nil
}

func TestKafkaContainerIntegration(t *testing.T) {
	ctx := context.Background()
	defer func() {
		if r := recover(); r != nil {
			t.Skipf("docker/container runtime unavailable: %v", r)
		}
	}()

	req := testcontainers.ContainerRequest{
		Image:        "docker.redpanda.com/redpandadata/redpanda:v24.1.8",
		ExposedPorts: []string{"9092/tcp"},
		Cmd:          []string{"redpanda", "start", "--overprovisioned", "--smp", "1", "--memory", "512M", "--reserve-memory", "0M", "--check=false", "--node-id", "0", "--kafka-addr", "0.0.0.0:9092", "--advertise-kafka-addr", "127.0.0.1:9092"},
		WaitingFor:   wait.ForLog("Successfully started Redpanda"),
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Skipf("docker/container runtime unavailable: %v", err)
	}
	defer func() { _ = ctr.Terminate(ctx) }()

	host, _ := ctr.Host(ctx)
	port, _ := ctr.MappedPort(ctx, "9092")
	broker := fmt.Sprintf("%s:%s", host, port.Port())

	producer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.DefaultProduceTopic("events"))
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	recBody, _ := json.Marshal(map[string]any{"tenant_id": "t1", "subject_type": "order", "stream_key": "s1", "event_id": "e1"})
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: "events", Value: recBody}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	app := &captureAppender{}
	adapter, err := NewAdapter(Config{Enabled: true, Brokers: []string{broker}, Topics: []string{"events"}, GroupID: "chronicles-it", ParseMode: ParseModeJSON}, app)
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	consumeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	go func() { _ = adapter.Start(consumeCtx) }()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-consumeCtx.Done():
			t.Fatalf("timed out waiting for consumed event")
		case <-ticker.C:
			app.mu.Lock()
			count := len(app.events)
			app.mu.Unlock()
			if count > 0 {
				return
			}
		}
	}
}
