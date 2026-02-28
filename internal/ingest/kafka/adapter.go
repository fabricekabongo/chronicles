package kafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"chronicles/internal/domain"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	CommitModeAfterQuorum = "after_quorum_commit"
	ParseModeJSON         = "json_envelope"
	ParseModeProtobuf     = "protobuf_envelope"
	ParseModeCustom       = "custom_mapper"
)

var ErrDuplicateEvent = errors.New("kafka duplicate event")

type Appender interface {
	Append(context.Context, domain.EventEnvelope) (domain.CommitMetadata, error)
}

type Mapper interface {
	MapKafkaRecord(*kgo.Record) (domain.EventEnvelope, error)
}

type Config struct {
	Enabled        bool
	Brokers        []string
	Topics         []string
	GroupID        string
	ClientID       string
	WorkerCount    int
	MaxPollRecords int
	QueueCapacity  int
	CommitMode     string
	ParseMode      string
	Auth           AuthConfig
	Fetch          FetchConfig

	CustomMapper      Mapper
	ProtobufUnmarshal func([]byte) (domain.EventEnvelope, error)
}

type AuthConfig struct {
	SASL SASLConfig
	TLS  TLSConfig
}

type SASLConfig struct {
	Enabled   bool
	Mechanism string
	Username  string
	Password  string
}

type TLSConfig struct {
	Enabled            bool
	InsecureSkipVerify bool
}

type FetchConfig struct {
	MinBytes int32
	MaxBytes int32
	MaxWait  time.Duration
}

type jsonEnvelope struct {
	TenantID     string            `json:"tenant_id"`
	SubjectType  string            `json:"subject_type"`
	StreamKey    string            `json:"stream_key"`
	EventID      string            `json:"event_id"`
	EventType    string            `json:"event_type"`
	EventTimeUTC string            `json:"event_time_utc"`
	Payload      json.RawMessage   `json:"payload"`
	Metadata     map[string]string `json:"metadata"`
}

type Adapter struct {
	cfg Config

	client  *kgo.Client
	records chan *kgo.Record
	acks    chan recordAck
	closed  atomic.Bool

	pauseMux sync.Mutex
	paused   bool

	appender     Appender
	markCommit   func(*kgo.Record)
	commitMarked func(context.Context) error
	pauseFetch   func(...string)
	resumeFetch  func(...string)
}

type recordAck struct {
	record *kgo.Record
	err    error
}

func NewAdapter(cfg Config, appender Appender, opts ...kgo.Opt) (*Adapter, error) {
	cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	kopts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxWait(cfg.Fetch.MaxWait),
		kgo.FetchMinBytes(cfg.Fetch.MinBytes),
		kgo.FetchMaxBytes(cfg.Fetch.MaxBytes),
	}
	if cfg.ClientID != "" {
		kopts = append(kopts, kgo.ClientID(cfg.ClientID))
	}
	if cfg.Auth.TLS.Enabled {
		kopts = append(kopts, kgo.DialTLSConfig(&tls.Config{InsecureSkipVerify: cfg.Auth.TLS.InsecureSkipVerify}))
	}
	kopts = append(kopts, opts...)

	cl, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, fmt.Errorf("new kafka client: %w", err)
	}

	a := &Adapter{
		cfg:      cfg,
		client:   cl,
		appender: appender,
		records:  make(chan *kgo.Record, cfg.QueueCapacity),
		acks:     make(chan recordAck, cfg.QueueCapacity),
	}
	a.markCommit = func(r *kgo.Record) { cl.MarkCommitRecords(r) }
	a.commitMarked = func(ctx context.Context) error { return cl.CommitMarkedOffsets(ctx) }
	a.pauseFetch = func(topics ...string) { _ = cl.PauseFetchTopics(topics...) }
	a.resumeFetch = func(topics ...string) { cl.ResumeFetchTopics(topics...) }
	return a, nil
}

func (c *Config) withDefaults() {
	if c.WorkerCount <= 0 {
		c.WorkerCount = 4
	}
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = 1024
	}
	if c.MaxPollRecords <= 0 {
		c.MaxPollRecords = 500
	}
	if c.CommitMode == "" {
		c.CommitMode = CommitModeAfterQuorum
	}
	if c.ParseMode == "" {
		c.ParseMode = ParseModeJSON
	}
	if c.Fetch.MaxWait <= 0 {
		c.Fetch.MaxWait = time.Second
	}
	if c.Fetch.MinBytes <= 0 {
		c.Fetch.MinBytes = 1
	}
	if c.Fetch.MaxBytes <= 0 {
		c.Fetch.MaxBytes = 50 << 20
	}
}

func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if len(c.Brokers) == 0 {
		return errors.New("kafka.brokers is required")
	}
	if len(c.Topics) == 0 {
		return errors.New("kafka.topics is required")
	}
	if c.GroupID == "" {
		return errors.New("kafka.group_id is required")
	}
	if c.CommitMode != CommitModeAfterQuorum {
		return fmt.Errorf("unsupported commit mode %q", c.CommitMode)
	}
	return nil
}

func (a *Adapter) Start(ctx context.Context) error {
	defer a.client.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		a.handleAcks(ctx)
	}()

	for i := 0; i < a.cfg.WorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.runWorker(ctx)
		}()
	}

	for {
		if ctx.Err() != nil || a.closed.Load() {
			close(a.records)
			wg.Wait()
			return ctx.Err()
		}
		fetches := a.client.PollRecords(ctx, a.cfg.MaxPollRecords)
		if errs := fetches.Errors(); len(errs) > 0 {
			return errs[0].Err
		}
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for _, rec := range p.Records {
				for {
					select {
					case a.records <- rec:
						a.maybeResume()
						goto next
					default:
						a.maybePause()
						time.Sleep(5 * time.Millisecond)
					}
				}
			next:
			}
		})
		a.client.AllowRebalance()
	}
}

func (a *Adapter) runWorker(ctx context.Context) {
	for rec := range a.records {
		env, err := a.normalizeRecord(rec)
		if err != nil {
			a.acks <- recordAck{record: rec, err: err}
			continue
		}
		_, err = a.appender.Append(ctx, env)
		a.acks <- recordAck{record: rec, err: err}
	}
}

func (a *Adapter) handleAcks(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ack := <-a.acks:
			if ack.record == nil {
				continue
			}
			if ack.err != nil && !errors.Is(ack.err, ErrDuplicateEvent) {
				continue
			}
			a.markCommit(ack.record)
			_ = a.commitMarked(ctx)
		}
	}
}

func (a *Adapter) normalizeRecord(rec *kgo.Record) (domain.EventEnvelope, error) {
	var env domain.EventEnvelope
	switch a.cfg.ParseMode {
	case ParseModeJSON:
		decoded, err := parseJSONEnvelope(rec.Value)
		if err != nil {
			return env, err
		}
		env = decoded
	case ParseModeProtobuf:
		if a.cfg.ProtobufUnmarshal == nil {
			return env, errors.New("protobuf parser not configured")
		}
		decoded, err := a.cfg.ProtobufUnmarshal(rec.Value)
		if err != nil {
			return env, err
		}
		env = decoded
	case ParseModeCustom:
		if a.cfg.CustomMapper == nil {
			return env, errors.New("custom mapper not configured")
		}
		decoded, err := a.cfg.CustomMapper.MapKafkaRecord(rec)
		if err != nil {
			return env, err
		}
		env = decoded
	default:
		return env, fmt.Errorf("unsupported parse mode %q", a.cfg.ParseMode)
	}
	env.Source = "kafka"
	env.SourceRef = fmt.Sprintf("%s/%d/%d", rec.Topic, rec.Partition, rec.Offset)
	if env.ReceivedAtUTC.IsZero() {
		env.ReceivedAtUTC = time.Now().UTC()
	}
	return env, validateEnvelope(env)
}

func parseJSONEnvelope(payload []byte) (domain.EventEnvelope, error) {
	var in jsonEnvelope
	if err := json.Unmarshal(payload, &in); err != nil {
		return domain.EventEnvelope{}, fmt.Errorf("parse json envelope: %w", err)
	}
	et := time.Now().UTC()
	if in.EventTimeUTC != "" {
		parsed, err := time.Parse(time.RFC3339Nano, in.EventTimeUTC)
		if err != nil {
			return domain.EventEnvelope{}, fmt.Errorf("parse event_time_utc: %w", err)
		}
		et = parsed.UTC()
	}
	return domain.EventEnvelope{
		TenantID:       in.TenantID,
		SubjectType:    in.SubjectType,
		StreamKey:      in.StreamKey,
		EventID:        in.EventID,
		EventType:      in.EventType,
		EventTimeUTCNs: et.UnixNano(),
		Payload:        append([]byte(nil), in.Payload...),
		Metadata:       in.Metadata,
	}, nil
}

func validateEnvelope(env domain.EventEnvelope) error {
	if strings.TrimSpace(env.EventID) == "" {
		return errors.New("event_id is required")
	}
	if strings.TrimSpace(env.StreamKey) == "" {
		return errors.New("stream_key is required")
	}
	if strings.TrimSpace(env.TenantID) == "" {
		return errors.New("tenant_id is required")
	}
	if strings.TrimSpace(env.SubjectType) == "" {
		return errors.New("subject_type is required")
	}
	return nil
}

func (a *Adapter) maybePause() {
	a.pauseMux.Lock()
	defer a.pauseMux.Unlock()
	if a.paused {
		return
	}
	if len(a.records) < cap(a.records) {
		return
	}
	a.pauseFetch(a.cfg.Topics...)
	a.paused = true
}

func (a *Adapter) maybeResume() {
	a.pauseMux.Lock()
	defer a.pauseMux.Unlock()
	if !a.paused {
		return
	}
	if len(a.records) > cap(a.records)/2 {
		return
	}
	a.resumeFetch(a.cfg.Topics...)
	a.paused = false
}
