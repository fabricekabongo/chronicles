package socket

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"chronicles/internal/domain"
	"chronicles/internal/hashroute"
)

type ChronicleRecord struct {
	EventID string
	LSN     uint64
}
type CommitResult struct {
	Route    domain.ChronicleRoute
	Metadata domain.CommitMetadata
}

type Engine interface {
	Append(context.Context, domain.EventEnvelope) (CommitResult, error)
	AppendBatch(context.Context, []domain.EventEnvelope) (CommitResult, error)
	GetChronicle(context.Context, domain.StreamRef) (domain.ChronicleRoute, []ChronicleRecord, bool)
	GetChronicleVisualOrder(context.Context, domain.StreamRef) (domain.ChronicleRoute, []ChronicleRecord, bool)
	GetRoute(context.Context, domain.StreamRef) (domain.ChronicleRoute, bool)
	Health(context.Context) (bool, string)
}

type Config struct {
	Network, Address, UnixSocketPath, AuthToken string
	MaxInflight, GlobalQueueLimit               int
	TLSConfig                                   *tls.Config
}

type Server struct {
	cfg     Config
	engine  Engine
	ln      net.Listener
	addr    atomic.Value
	globalQ chan struct{}
	partQ   []chan queuedRequest
	closed  atomic.Bool
	wg      sync.WaitGroup
}

type queuedRequest struct {
	ctx     context.Context
	req     *SocketRequest
	conn    *connection
	release func()
}
type connection struct {
	c        net.Conn
	writerQ  chan *SocketResponse
	inflight chan struct{}
}

func NewServer(cfg Config, engine Engine) *Server {
	if cfg.MaxInflight <= 0 {
		cfg.MaxInflight = 64
	}
	if cfg.GlobalQueueLimit <= 0 {
		cfg.GlobalQueueLimit = 4096
	}
	if cfg.Network == "" {
		cfg.Network = "tcp"
	}
	s := &Server{cfg: cfg, engine: engine, globalQ: make(chan struct{}, cfg.GlobalQueueLimit), partQ: make([]chan queuedRequest, hashroute.PartitionCount)}
	for i := range s.partQ {
		s.partQ[i] = make(chan queuedRequest, 128)
	}
	return s
}

func (s *Server) Addr() string {
	if v := s.addr.Load(); v != nil {
		return v.(string)
	}
	return ""
}

func (s *Server) Start(ctx context.Context) error {
	addr := s.cfg.Address
	if s.cfg.Network == "unix" {
		addr = s.cfg.UnixSocketPath
	}
	ln, err := net.Listen(s.cfg.Network, addr)
	if err != nil {
		return err
	}
	if s.cfg.TLSConfig != nil {
		ln = tls.NewListener(ln, s.cfg.TLSConfig)
	}
	s.ln = ln
	s.addr.Store(ln.Addr().String())

	for i := range s.partQ {
		s.wg.Add(1)
		go s.runPartitionWorker(s.partQ[i])
	}
	go func() { <-ctx.Done(); _ = s.Close() }()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.closed.Load() {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Temporary() {
				continue
			}
			return err
		}
		s.handleConn(ctx, conn)
	}
}

func (s *Server) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	if s.ln != nil {
		_ = s.ln.Close()
	}
	for _, q := range s.partQ {
		close(q)
	}
	s.wg.Wait()
	return nil
}

func (s *Server) handleConn(ctx context.Context, raw net.Conn) {
	conn := &connection{c: raw, writerQ: make(chan *SocketResponse, 256), inflight: make(chan struct{}, s.cfg.MaxInflight)}
	s.wg.Add(2)
	go func() { defer s.wg.Done(); s.writeLoop(conn) }()
	go func() { defer s.wg.Done(); defer raw.Close(); defer close(conn.writerQ); s.readLoop(ctx, conn) }()
}

func (s *Server) writeLoop(conn *connection) {
	w := bufio.NewWriter(conn.c)
	for res := range conn.writerQ {
		payload, err := MarshalMessage(res)
		if err != nil {
			continue
		}
		if err := WriteFrame(w, payload); err != nil {
			return
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}

func (s *Server) readLoop(ctx context.Context, conn *connection) {
	r := bufio.NewReader(conn.c)
	for {
		payload, err := ReadFrame(r)
		if err != nil {
			return
		}
		req, err := UnmarshalRequest(payload)
		if err != nil {
			s.send(conn, &SocketResponse{ErrorCode: int32(ErrorCodeBadRequest), ErrorMessage: err.Error()})
			continue
		}
		if err := ValidateRequest(req); err != nil {
			s.send(conn, &SocketResponse{RequestId: req.RequestId, ErrorCode: int32(ErrorCodeBadRequest), ErrorMessage: err.Error()})
			continue
		}
		if s.cfg.AuthToken != "" && req.AuthToken != s.cfg.AuthToken {
			s.send(conn, &SocketResponse{RequestId: req.RequestId, ErrorCode: int32(ErrorCodeUnauthenticated), ErrorMessage: "invalid auth token"})
			continue
		}

		select {
		case conn.inflight <- struct{}{}:
		default:
			s.send(conn, &SocketResponse{RequestId: req.RequestId, ErrorCode: int32(ErrorCodeOverloaded), ErrorMessage: "connection inflight limit exceeded"})
			continue
		}
		releaseInflight := func() { <-conn.inflight }
		select {
		case s.globalQ <- struct{}{}:
		default:
			releaseInflight()
			s.send(conn, &SocketResponse{RequestId: req.RequestId, ErrorCode: int32(ErrorCodeOverloaded), ErrorMessage: "adapter queue overloaded"})
			continue
		}

		qr := queuedRequest{ctx: ctx, req: req, conn: conn, release: func() { <-s.globalQ; releaseInflight() }}
		q := s.partQ[partitionFor(req)]
		select {
		case q <- qr:
		default:
			qr.release()
			s.send(conn, &SocketResponse{RequestId: req.RequestId, ErrorCode: int32(ErrorCodeOverloaded), ErrorMessage: "partition queue overloaded"})
		}
	}
}

func (s *Server) runPartitionWorker(q chan queuedRequest) {
	defer s.wg.Done()
	for req := range q {
		res := s.handleRequest(req.ctx, req.req)
		req.release()
		s.send(req.conn, res)
	}
}

func (s *Server) send(conn *connection, res *SocketResponse) {
	select {
	case conn.writerQ <- res:
	default:
	}
}

func partitionFor(req *SocketRequest) int {
	if req.Append != nil && req.Append.Event != nil {
		return hashroute.PartitionForStreamKey(req.Append.Event.StreamKey)
	}
	if req.AppendBatch != nil && len(req.AppendBatch.Events) > 0 {
		return hashroute.PartitionForStreamKey(req.AppendBatch.Events[0].StreamKey)
	}
	return 0
}

func (s *Server) handleRequest(ctx context.Context, req *SocketRequest) *SocketResponse {
	res := &SocketResponse{RequestId: req.RequestId, ErrorCode: int32(ErrorCodeOK)}
	switch Operation(req.Operation) {
	case OperationPing:
		res.Pong = &PongResponse{UnixTimeNs: time.Now().UTC().UnixNano()}
	case OperationHealth:
		ok, msg := s.engine.Health(ctx)
		res.Health = &HealthResponse{Ok: ok, Message: msg}
	case OperationAppend:
		return s.handleAppend(ctx, req, res)
	case OperationAppendBatch:
		return s.handleAppendBatch(ctx, req, res)
	case OperationGetRoute:
		if req.GetRoute == nil {
			return badReq(req, "get_route query required")
		}
		r, found := s.engine.GetRoute(ctx, streamRef(req.GetRoute.TenantId, req.GetRoute.SubjectType, req.GetRoute.StreamKey))
		res.Route = &RouteResponse{Found: found}
		if found {
			res.Route.PartitionId, res.Route.CreationDayUtc = uint32(r.PartitionID), r.CreationDayUTC
		}
	case OperationGetChronicle:
		if req.GetChronicle == nil {
			return badReq(req, "get_chronicle query required")
		}
		r, e, found := s.engine.GetChronicle(ctx, streamRef(req.GetChronicle.TenantId, req.GetChronicle.SubjectType, req.GetChronicle.StreamKey))
		res.Chronicle = toChronicleResponse(found, r, e)
	case OperationGetChronicleVisualOrder:
		if req.GetChronicle == nil {
			return badReq(req, "get_chronicle query required")
		}
		r, e, found := s.engine.GetChronicleVisualOrder(ctx, streamRef(req.GetChronicle.TenantId, req.GetChronicle.SubjectType, req.GetChronicle.StreamKey))
		res.Chronicle = toChronicleResponse(found, r, e)
	default:
		return badReq(req, "unknown operation")
	}
	return res
}

func badReq(req *SocketRequest, msg string) *SocketResponse {
	return &SocketResponse{RequestId: req.RequestId, ErrorCode: int32(ErrorCodeBadRequest), ErrorMessage: msg}
}
func (s *Server) handleAppend(ctx context.Context, req *SocketRequest, res *SocketResponse) *SocketResponse {
	if req.Append == nil || req.Append.Event == nil {
		return badReq(req, "append event required")
	}
	cr, err := s.engine.Append(ctx, toDomain(req.Append.Event))
	if err != nil {
		res.ErrorCode, res.ErrorMessage = int32(ErrorCodeInternal), err.Error()
		return res
	}
	res.Append = appendResponse(cr)
	return res
}
func (s *Server) handleAppendBatch(ctx context.Context, req *SocketRequest, res *SocketResponse) *SocketResponse {
	if req.AppendBatch == nil || len(req.AppendBatch.Events) == 0 {
		return badReq(req, "append_batch events required")
	}
	events := make([]domain.EventEnvelope, 0, len(req.AppendBatch.Events))
	for _, e := range req.AppendBatch.Events {
		events = append(events, toDomain(e))
	}
	cr, err := s.engine.AppendBatch(ctx, events)
	if err != nil {
		res.ErrorCode, res.ErrorMessage = int32(ErrorCodeInternal), err.Error()
		return res
	}
	res.Append = appendResponse(cr)
	return res
}
func appendResponse(cr CommitResult) *AppendResponse {
	return &AppendResponse{Accepted: true, PartitionId: uint32(cr.Route.PartitionID), CreationDayUtc: cr.Route.CreationDayUTC, Lsn: cr.Metadata.LSN, CommittedAtUtcNs: cr.Metadata.CommittedAtUTC.UnixNano(), LeaderNodeId: cr.Metadata.LeaderNodeID}
}
func toChronicleResponse(found bool, route domain.ChronicleRoute, events []ChronicleRecord) *ChronicleResponse {
	out := &ChronicleResponse{Found: found}
	if !found {
		return out
	}
	out.PartitionId, out.CreationDayUtc = uint32(route.PartitionID), route.CreationDayUTC
	for _, e := range events {
		out.Events = append(out.Events, &ChronicleEvent{EventId: e.EventID, Lsn: e.LSN})
	}
	return out
}
func toDomain(e *Event) domain.EventEnvelope {
	if e == nil {
		return domain.EventEnvelope{}
	}
	return domain.EventEnvelope{TenantID: e.TenantId, SubjectType: e.SubjectType, StreamKey: e.StreamKey, EventID: e.EventId, EventType: e.EventType, EventTimeUTCNs: e.EventTimeUtcNs, Payload: e.Payload, Source: e.Source, SourceRef: e.SourceRef, ReceivedAtUTC: time.Now().UTC()}
}
func streamRef(tenantID, subjectType, streamKey string) domain.StreamRef {
	return domain.StreamRef{TenantID: tenantID, SubjectType: subjectType, StreamKey: streamKey}
}

func DialAndRequest(ctx context.Context, network, address string, req *SocketRequest) (*SocketResponse, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	payload, err := MarshalMessage(req)
	if err != nil {
		return nil, err
	}
	if err := WriteFrame(conn, payload); err != nil {
		return nil, err
	}
	frame, err := ReadFrame(bufio.NewReader(conn))
	if err != nil {
		return nil, err
	}
	return UnmarshalResponse(frame)
}

func Retryable(code int32) bool              { return ErrorCode(code) == ErrorCodeOverloaded }
func Error(code ErrorCode, msg string) error { return fmt.Errorf("%d:%s", code, msg) }
