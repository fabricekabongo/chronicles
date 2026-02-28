package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"chronicles/internal/domain"

	"github.com/twmb/franz-go/pkg/kgo"
)

type stubAppender struct {
	mu      sync.Mutex
	events  []domain.EventEnvelope
	errByID map[string]error
	waitCh  chan struct{}
}

func (s *stubAppender) Append(_ context.Context, ev domain.EventEnvelope) (domain.CommitMetadata, error) {
	if s.waitCh != nil {
		<-s.waitCh
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
	if err := s.errByID[ev.EventID]; err != nil {
		return domain.CommitMetadata{}, err
	}
	return domain.CommitMetadata{LSN: 1}, nil
}

func TestConfigValidate(t *testing.T) {
	cfg := Config{Enabled: true, Brokers: []string{"127.0.0.1:9092"}, Topics: []string{"events"}, GroupID: "g1"}
	cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.CommitMode != CommitModeAfterQuorum {
		t.Fatalf("default commit mode = %q", cfg.CommitMode)
	}
}

func TestNormalizeJSONEnvelope(t *testing.T) {
	a := &Adapter{cfg: Config{ParseMode: ParseModeJSON}}
	rec := &kgo.Record{Topic: "events", Partition: 2, Offset: 7, Value: []byte(`{"tenant_id":"t1","subject_type":"order","stream_key":"s1","event_id":"e1","event_type":"created","event_time_utc":"2026-01-01T00:00:00Z","payload":{"ok":true}}`)}
	env, err := a.normalizeRecord(rec)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if env.Source != "kafka" || env.SourceRef != "events/2/7" {
		t.Fatalf("unexpected source fields: %+v", env)
	}
	if env.EventID != "e1" || env.StreamKey != "s1" {
		t.Fatalf("unexpected event normalization: %+v", env)
	}
}

func TestOffsetCommitOnlyAfterQuorumAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wait := make(chan struct{})
	app := &stubAppender{waitCh: wait, errByID: map[string]error{}}
	a := &Adapter{
		cfg:      Config{ParseMode: ParseModeJSON, Topics: []string{"events"}},
		appender: app,
		records:  make(chan *kgo.Record, 1),
		acks:     make(chan recordAck, 1),
	}

	committed := make(chan struct{}, 1)
	a.markCommit = func(*kgo.Record) { committed <- struct{}{} }
	a.commitMarked = func(context.Context) error { return nil }
	a.pauseFetch = func(...string) {}
	a.resumeFetch = func(...string) {}

	go a.handleAcks(ctx)
	go a.runWorker(ctx)

	a.records <- &kgo.Record{Topic: "events", Partition: 0, Offset: 1, Value: []byte(`{"tenant_id":"t","subject_type":"s","stream_key":"k","event_id":"id1"}`)}

	select {
	case <-committed:
		t.Fatalf("offset committed before quorum append ack")
	case <-time.After(75 * time.Millisecond):
	}
	close(wait)
	select {
	case <-committed:
	case <-time.After(time.Second):
		t.Fatalf("expected commit after ack")
	}
}

func TestDuplicateDeliveryIsCommitted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &Adapter{cfg: Config{ParseMode: ParseModeJSON}, acks: make(chan recordAck, 1)}
	commits := 0
	a.markCommit = func(*kgo.Record) { commits++ }
	a.commitMarked = func(context.Context) error { return nil }

	go a.handleAcks(ctx)
	a.acks <- recordAck{record: &kgo.Record{Topic: "events", Partition: 0, Offset: 2}, err: ErrDuplicateEvent}
	time.Sleep(40 * time.Millisecond)
	if commits != 1 {
		t.Fatalf("expected duplicate to be committed, got %d", commits)
	}
}

func TestBackpressurePauseAndResume(t *testing.T) {
	a := &Adapter{cfg: Config{Topics: []string{"events"}}, records: make(chan *kgo.Record, 2)}
	paused := 0
	resumed := 0
	a.pauseFetch = func(...string) { paused++ }
	a.resumeFetch = func(...string) { resumed++ }

	a.records <- &kgo.Record{}
	a.records <- &kgo.Record{}
	a.maybePause()
	if paused != 1 {
		t.Fatalf("expected pause, got %d", paused)
	}
	<-a.records
	a.maybeResume()
	if resumed != 1 {
		t.Fatalf("expected resume, got %d", resumed)
	}
}

func TestCommitSkipsOnAppendFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app := &stubAppender{errByID: map[string]error{"id1": errors.New("quorum failed")}}
	a := &Adapter{
		cfg:      Config{ParseMode: ParseModeJSON},
		appender: app,
		records:  make(chan *kgo.Record, 1),
		acks:     make(chan recordAck, 1),
	}
	commits := 0
	a.markCommit = func(*kgo.Record) { commits++ }
	a.commitMarked = func(context.Context) error { return nil }
	a.pauseFetch = func(...string) {}
	a.resumeFetch = func(...string) {}
	go a.handleAcks(ctx)
	go a.runWorker(ctx)
	a.records <- &kgo.Record{Topic: "events", Partition: 0, Offset: 1, Value: []byte(`{"tenant_id":"t","subject_type":"s","stream_key":"k","event_id":"id1"}`)}
	time.Sleep(60 * time.Millisecond)
	if commits != 0 {
		t.Fatalf("expected no offset commit on append failure")
	}
}
