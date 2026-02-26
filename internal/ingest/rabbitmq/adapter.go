package rabbitmq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"chronicles/internal/domain"

	"github.com/rabbitmq/amqp091-go"
)

type Appender interface {
	Append(context.Context, domain.EventEnvelope) error
}

type Config struct {
	Enabled       bool
	URL           string
	Endpoints     []string
	Exchange      string
	Queue         string
	RoutingKeys   []string
	ConsumerTag   string
	PrefetchCount int
	ManualAck     bool
	TLS           TLSConfig
	Auth          AuthConfig
	Parser        ParserConfig
	Workers       int
	DeliveryQueue int
}

type TLSConfig struct {
	Enabled            bool
	InsecureSkipVerify bool
	ServerName         string
	CAFile             string
	CertFile           string
	KeyFile            string
}

type AuthConfig struct {
	Username string
	Password string
}

type ParserConfig struct {
	RequireTenantFields bool
}

type Adapter struct {
	cfg      Config
	engine   Appender
	conn     *amqp091.Connection
	ch       *amqp091.Channel
	deliver  <-chan amqp091.Delivery
	ops      chan deliveryTask
	closed   chan struct{}
	closeErr atomic.Value
	wg       sync.WaitGroup
}

type deliveryTask struct {
	ctx      context.Context
	delivery amqp091.Delivery
}

type envelopePayload struct {
	TenantID    string          `json:"tenant_id"`
	SubjectType string          `json:"subject_type"`
	StreamKey   string          `json:"stream_key"`
	EventID     string          `json:"event_id"`
	EventType   string          `json:"event_type"`
	EventTime   string          `json:"event_time_utc"`
	Payload     json.RawMessage `json:"payload"`
	Metadata    map[string]any  `json:"metadata"`
}

func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if !c.ManualAck {
		return fmt.Errorf("rabbitmq manual_ack must be true")
	}
	if c.Queue == "" {
		return fmt.Errorf("rabbitmq queue is required")
	}
	if c.Exchange == "" {
		return fmt.Errorf("rabbitmq exchange is required")
	}
	if c.PrefetchCount < 1 {
		return fmt.Errorf("rabbitmq prefetch_count must be >= 1")
	}
	if c.Workers < 1 {
		return fmt.Errorf("rabbitmq workers must be >= 1")
	}
	if c.DeliveryQueue < 1 {
		return fmt.Errorf("rabbitmq delivery_queue must be >= 1")
	}
	if c.endpoint() == "" {
		return fmt.Errorf("rabbitmq url or endpoints is required")
	}
	return nil
}

func (c Config) endpoint() string {
	if strings.TrimSpace(c.URL) != "" {
		return strings.TrimSpace(c.URL)
	}
	for _, e := range c.Endpoints {
		if strings.TrimSpace(e) != "" {
			return strings.TrimSpace(e)
		}
	}
	return ""
}

func NewAdapter(cfg Config, engine Appender) (*Adapter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if engine == nil {
		return nil, fmt.Errorf("engine is required")
	}
	if cfg.ConsumerTag == "" {
		cfg.ConsumerTag = "chronicles-rabbitmq"
	}
	return &Adapter{cfg: cfg, engine: engine, closed: make(chan struct{}), ops: make(chan deliveryTask, cfg.DeliveryQueue)}, nil
}

func (a *Adapter) Start(ctx context.Context) error {
	dialCfg := amqp091.Config{}
	if a.cfg.Auth.Username != "" {
		dialCfg.SASL = []amqp091.Authentication{&amqp091.PlainAuth{Username: a.cfg.Auth.Username, Password: a.cfg.Auth.Password}}
	}
	if tlsCfg, err := a.buildTLSConfig(); err != nil {
		return err
	} else if tlsCfg != nil {
		dialCfg.TLSClientConfig = tlsCfg
	}
	conn, err := amqp091.DialConfig(a.cfg.endpoint(), dialCfg)
	if err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("open rabbitmq channel: %w", err)
	}
	if err := ch.Qos(a.cfg.PrefetchCount, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("set prefetch: %w", err)
	}
	if err := ch.ExchangeDeclare(a.cfg.Exchange, "topic", true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("declare exchange: %w", err)
	}
	if _, err := ch.QueueDeclare(a.cfg.Queue, true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("declare queue: %w", err)
	}
	routingKeys := a.cfg.RoutingKeys
	if len(routingKeys) == 0 {
		routingKeys = []string{"#"}
	}
	for _, key := range routingKeys {
		if err := ch.QueueBind(a.cfg.Queue, key, a.cfg.Exchange, false, nil); err != nil {
			ch.Close()
			conn.Close()
			return fmt.Errorf("bind queue key=%s: %w", key, err)
		}
	}
	deliveries, err := ch.Consume(a.cfg.Queue, a.cfg.ConsumerTag, false, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("consume queue: %w", err)
	}
	a.conn, a.ch, a.deliver = conn, ch, deliveries

	a.wg.Add(1)
	go a.readLoop(ctx)
	for i := 0; i < a.cfg.Workers; i++ {
		a.wg.Add(1)
		go a.workerLoop(ctx)
	}
	return nil
}

func (a *Adapter) Close() error {
	select {
	case <-a.closed:
		if v := a.closeErr.Load(); v != nil {
			return v.(error)
		}
		return nil
	default:
		close(a.closed)
	}
	if a.ch != nil {
		_ = a.ch.Cancel(a.cfg.ConsumerTag, false)
	}
	close(a.ops)
	a.wg.Wait()
	var errs []error
	if a.ch != nil {
		if err := a.ch.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if a.conn != nil {
		if err := a.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	err := errors.Join(errs...)
	a.closeErr.Store(err)
	return err
}

func (a *Adapter) readLoop(ctx context.Context) {
	defer a.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.closed:
			return
		case d, ok := <-a.deliver:
			if !ok {
				return
			}
			task := deliveryTask{ctx: ctx, delivery: d}
			select {
			case a.ops <- task:
			case <-ctx.Done():
				return
			case <-a.closed:
				return
			}
		}
	}
}

func (a *Adapter) workerLoop(ctx context.Context) {
	defer a.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.closed:
			return
		case task, ok := <-a.ops:
			if !ok {
				return
			}
			a.processDelivery(task.ctx, task.delivery)
		}
	}
}

func (a *Adapter) processDelivery(ctx context.Context, d amqp091.Delivery) {
	env, err := a.parseDelivery(d)
	if err != nil {
		_ = d.Nack(false, false)
		return
	}
	if err := a.engine.Append(ctx, env); err != nil {
		if isRetryable(err) {
			_ = d.Nack(false, true)
			return
		}
		_ = d.Nack(false, false)
		return
	}
	_ = d.Ack(false)
}

func (a *Adapter) parseDelivery(d amqp091.Delivery) (domain.EventEnvelope, error) {
	var msg envelopePayload
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		return domain.EventEnvelope{}, fmt.Errorf("unmarshal delivery body: %w", err)
	}
	if a.cfg.Parser.RequireTenantFields {
		if msg.TenantID == "" || msg.SubjectType == "" || msg.StreamKey == "" {
			return domain.EventEnvelope{}, fmt.Errorf("missing required tenant stream fields")
		}
	}
	eventID := msg.EventID
	if eventID == "" {
		eventID = headerString(d.Headers, "event_id")
	}
	eventTimeNs, err := parseEventTime(msg.EventTime, d.Headers)
	if err != nil {
		return domain.EventEnvelope{}, err
	}
	meta := map[string]string{}
	for k, v := range d.Headers {
		meta[k] = fmt.Sprint(v)
	}
	for k, v := range msg.Metadata {
		meta[k] = fmt.Sprint(v)
	}
	sourceRef := fmt.Sprintf("%s/%s/%d", d.Exchange, d.RoutingKey, d.DeliveryTag)
	payload := msg.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(d.Body)
	}
	return domain.EventEnvelope{
		TenantID:       msg.TenantID,
		SubjectType:    msg.SubjectType,
		StreamKey:      msg.StreamKey,
		EventID:        eventID,
		EventType:      msg.EventType,
		EventTimeUTCNs: eventTimeNs,
		Payload:        payload,
		Source:         "rabbitmq",
		SourceRef:      sourceRef,
		ReceivedAtUTC:  time.Now().UTC(),
		Metadata:       meta,
	}, nil
}

func parseEventTime(raw string, headers amqp091.Table) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = headerString(headers, "event_time_utc")
	}
	if raw == "" {
		return 0, nil
	}
	if unixNs, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return unixNs, nil
	}
	tm, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return 0, fmt.Errorf("invalid event_time_utc: %w", err)
	}
	return tm.UnixNano(), nil
}

func headerString(table amqp091.Table, key string) string {
	if table == nil {
		return ""
	}
	v, ok := table[key]
	if !ok {
		return ""
	}
	return fmt.Sprint(v)
}

func (a *Adapter) buildTLSConfig() (*tls.Config, error) {
	if !a.cfg.TLS.Enabled {
		return nil, nil
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: a.cfg.TLS.InsecureSkipVerify, ServerName: a.cfg.TLS.ServerName}
	if a.cfg.TLS.CAFile != "" {
		pemBytes, err := os.ReadFile(a.cfg.TLS.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read rabbitmq ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("parse rabbitmq ca_file")
		}
		tlsCfg.RootCAs = pool
	}
	if a.cfg.TLS.CertFile != "" || a.cfg.TLS.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(a.cfg.TLS.CertFile, a.cfg.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load rabbitmq cert/key: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

type retryable interface{ Temporary() bool }

func isRetryable(err error) bool {
	var te retryable
	if errors.As(err, &te) {
		return te.Temporary()
	}
	return false
}
