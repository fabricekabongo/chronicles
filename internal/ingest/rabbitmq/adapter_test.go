package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	"chronicles/internal/domain"

	"github.com/rabbitmq/amqp091-go"
)

type ackRecorder struct {
	ack  int
	nack int
	req  bool
}

func (a *ackRecorder) Ack(tag uint64, multiple bool) error {
	a.ack++
	return nil
}
func (a *ackRecorder) Nack(tag uint64, multiple bool, requeue bool) error {
	a.nack++
	a.req = requeue
	return nil
}
func (a *ackRecorder) Reject(tag uint64, requeue bool) error { return nil }

type fakeEngine struct {
	err error
}

func (f *fakeEngine) Append(context.Context, domain.EventEnvelope) error { return f.err }

type temporaryError struct{ error }

func (temporaryError) Temporary() bool { return true }

func TestProcessDeliveryAckOnSuccess(t *testing.T) {
	adapter, err := NewAdapter(Config{Enabled: true, URL: "amqp://guest:guest@localhost:5672/", Exchange: "x", Queue: "q", PrefetchCount: 1, ManualAck: true, Workers: 1, DeliveryQueue: 1}, &fakeEngine{})
	if err != nil {
		t.Fatal(err)
	}
	rec := &ackRecorder{}
	d := amqp091.Delivery{Acknowledger: rec, Body: []byte(`{"tenant_id":"t","subject_type":"order","stream_key":"s1","event_id":"e1"}`), Exchange: "x", RoutingKey: "k", DeliveryTag: 9}
	adapter.processDelivery(context.Background(), d)
	if rec.ack != 1 || rec.nack != 0 {
		t.Fatalf("expected ack once, got ack=%d nack=%d", rec.ack, rec.nack)
	}
}

func TestProcessDeliveryNackRequeueOnRetryable(t *testing.T) {
	adapter, err := NewAdapter(Config{Enabled: true, URL: "amqp://guest:guest@localhost:5672/", Exchange: "x", Queue: "q", PrefetchCount: 1, ManualAck: true, Workers: 1, DeliveryQueue: 1}, &fakeEngine{err: temporaryError{errors.New("transient")}})
	if err != nil {
		t.Fatal(err)
	}
	rec := &ackRecorder{}
	d := amqp091.Delivery{Acknowledger: rec, Body: []byte(`{"tenant_id":"t","subject_type":"order","stream_key":"s1","event_id":"e1"}`), Exchange: "x", RoutingKey: "k", DeliveryTag: 9}
	adapter.processDelivery(context.Background(), d)
	if rec.nack != 1 || !rec.req {
		t.Fatalf("expected nack requeue true, got nack=%d requeue=%t", rec.nack, rec.req)
	}
}

func TestProcessDeliveryNackDropOnParseFailure(t *testing.T) {
	adapter, err := NewAdapter(Config{Enabled: true, URL: "amqp://guest:guest@localhost:5672/", Exchange: "x", Queue: "q", PrefetchCount: 1, ManualAck: true, Workers: 1, DeliveryQueue: 1}, &fakeEngine{})
	if err != nil {
		t.Fatal(err)
	}
	rec := &ackRecorder{}
	d := amqp091.Delivery{Acknowledger: rec, Body: []byte(`{not-json`), DeliveryTag: 9}
	adapter.processDelivery(context.Background(), d)
	if rec.nack != 1 || rec.req {
		t.Fatalf("expected nack requeue false, got nack=%d requeue=%t", rec.nack, rec.req)
	}
}

func TestParseDeliveryHeaderFallbacks(t *testing.T) {
	adapter, err := NewAdapter(Config{Enabled: true, URL: "amqp://guest:guest@localhost:5672/", Exchange: "x", Queue: "q", PrefetchCount: 1, ManualAck: true, Workers: 1, DeliveryQueue: 1}, &fakeEngine{})
	if err != nil {
		t.Fatal(err)
	}
	d := amqp091.Delivery{
		Body:        []byte(`{"tenant_id":"t","subject_type":"order","stream_key":"s1","payload":{"x":1}}`),
		Exchange:    "chronicles.events",
		RoutingKey:  "events.order",
		DeliveryTag: 11,
		Headers: amqp091.Table{
			"event_id":       "e-header",
			"event_time_utc": time.Now().UTC().Format(time.RFC3339Nano),
		},
	}
	env, err := adapter.parseDelivery(d)
	if err != nil {
		t.Fatal(err)
	}
	if env.Source != "rabbitmq" || env.EventID != "e-header" {
		t.Fatalf("unexpected envelope mapping: %+v", env)
	}
	if env.SourceRef != "chronicles.events/q/11" {
		t.Fatalf("unexpected source ref: %s", env.SourceRef)
	}
}
