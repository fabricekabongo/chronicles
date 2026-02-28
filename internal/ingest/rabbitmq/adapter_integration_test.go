package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"chronicles/internal/domain"

	"github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type recordingEngine struct {
	mu      sync.Mutex
	applied []domain.EventEnvelope
	fn      func(domain.EventEnvelope) error
}

func (r *recordingEngine) Append(_ context.Context, e domain.EventEnvelope) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied = append(r.applied, e)
	if r.fn != nil {
		return r.fn(e)
	}
	return nil
}

func (r *recordingEngine) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.applied)
}

func runRabbitMQ(t *testing.T) (string, func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:3.13-alpine",
		ExposedPorts: []string{"5672/tcp"},
		WaitingFor:   wait.ForListeningPort("5672/tcp").WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Skipf("rabbitmq container unavailable: %v", err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		t.Fatalf("container host: %v", err)
	}
	port, err := c.MappedPort(ctx, "5672")
	if err != nil {
		_ = c.Terminate(ctx)
		t.Fatalf("mapped port: %v", err)
	}
	url := fmt.Sprintf("amqp://guest:guest@%s:%s/", host, port.Port())
	cleanup := func() { _ = c.Terminate(ctx) }
	return url, cleanup
}

func publish(t *testing.T, ch *amqp091.Channel, exchange, key string, body []byte) {
	t.Helper()
	if err := ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp091.Publishing{ContentType: "application/json", Body: body}); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func openChannel(t *testing.T, url string) (*amqp091.Connection, *amqp091.Channel) {
	t.Helper()
	conn, err := amqp091.Dial(url)
	if err != nil {
		t.Fatalf("dial amqp: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		t.Fatalf("channel: %v", err)
	}
	return conn, ch
}

func TestAdapterIntegration_AckAndRedeliveryAndDrop(t *testing.T) {
	url, cleanup := runRabbitMQ(t)
	defer cleanup()

	retryOnce := true
	engine := &recordingEngine{fn: func(domain.EventEnvelope) error {
		if retryOnce {
			retryOnce = false
			return temporaryError{errors.New("retry me")}
		}
		return nil
	}}
	cfg := Config{Enabled: true, URL: url, Exchange: "chronicles.events", Queue: "chronicles.ingest", RoutingKeys: []string{"events.*"}, ConsumerTag: "chronicles-it", PrefetchCount: 2, ManualAck: true, Workers: 2, DeliveryQueue: 32, Parser: ParserConfig{RequireTenantFields: true}}
	adapter, err := NewAdapter(cfg, engine)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := adapter.Start(ctx); err != nil {
		t.Fatalf("adapter start: %v", err)
	}
	defer adapter.Close()

	conn, ch := openChannel(t, url)
	defer conn.Close()
	defer ch.Close()

	good, _ := json.Marshal(map[string]any{"tenant_id": "t", "subject_type": "order", "stream_key": "s-1", "event_id": "e1", "payload": map[string]any{"ok": true}})
	publish(t, ch, cfg.Exchange, "events.order", good)
	publish(t, ch, cfg.Exchange, "events.order", []byte(`{"tenant_id":"t","subject_type":"order"`))

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if engine.count() >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if engine.count() < 2 {
		t.Fatalf("expected redelivery after retryable nack, got appends=%d", engine.count())
	}

	out, err := ch.Consume("chronicles.ingest", "verify-empty", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume verify queue: %v", err)
	}
	select {
	case d := <-out:
		_ = d.Nack(false, true)
		t.Fatalf("expected malformed message to be nacked drop (not requeued)")
	case <-time.After(700 * time.Millisecond):
	}
}

func TestAdapterIntegration_BackpressurePrefetchOne(t *testing.T) {
	url, cleanup := runRabbitMQ(t)
	defer cleanup()

	release := make(chan struct{})
	engine := &recordingEngine{fn: func(domain.EventEnvelope) error {
		<-release
		return nil
	}}
	cfg := Config{Enabled: true, URL: url, Exchange: "chronicles.events2", Queue: "chronicles.prefetch", RoutingKeys: []string{"events.prefetch"}, ConsumerTag: "chronicles-prefetch", PrefetchCount: 1, ManualAck: true, Workers: 1, DeliveryQueue: 1}
	adapter, err := NewAdapter(cfg, engine)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := adapter.Start(ctx); err != nil {
		t.Fatalf("adapter start: %v", err)
	}
	defer adapter.Close()

	conn, ch := openChannel(t, url)
	defer conn.Close()
	defer ch.Close()

	m1 := []byte(`{"tenant_id":"t","subject_type":"order","stream_key":"one","event_id":"e1"}`)
	m2 := []byte(`{"tenant_id":"t","subject_type":"order","stream_key":"two","event_id":"e2"}`)
	publish(t, ch, cfg.Exchange, "events.prefetch", m1)
	publish(t, ch, cfg.Exchange, "events.prefetch", m2)

	time.Sleep(400 * time.Millisecond)
	if got := engine.count(); got != 1 {
		t.Fatalf("expected only one inflight append with prefetch=1, got %d", got)
	}
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if engine.count() >= 2 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("expected second delivery after first ack, got appends=%d", engine.count())
}
