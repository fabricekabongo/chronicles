package socket

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"chronicles/internal/domain"
	"chronicles/internal/hashroute"
)

type InMemoryEngine struct {
	mu       sync.Mutex
	router   *hashroute.Router
	streams  map[string][]ChronicleRecord
	ids      map[string]CommitResult
	lsn      map[domain.PartitionID]uint64
	nodeID   string
	quorumMs time.Duration
}

func NewInMemoryEngine(nodeID string, quorumDelay time.Duration) *InMemoryEngine {
	return &InMemoryEngine{router: hashroute.NewRouter(), streams: map[string][]ChronicleRecord{}, ids: map[string]CommitResult{}, lsn: map[domain.PartitionID]uint64{}, nodeID: nodeID, quorumMs: quorumDelay}
}

func (m *InMemoryEngine) Append(ctx context.Context, event domain.EventEnvelope) (CommitResult, error) {
	return m.AppendBatch(ctx, []domain.EventEnvelope{event})
}

func (m *InMemoryEngine) AppendBatch(ctx context.Context, events []domain.EventEnvelope) (CommitResult, error) {
	if len(events) == 0 {
		return CommitResult{}, fmt.Errorf("empty batch")
	}
	select {
	case <-ctx.Done():
		return CommitResult{}, ctx.Err()
	case <-time.After(m.quorumMs):
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	last := CommitResult{}
	for _, evt := range events {
		stream := domain.StreamRef{TenantID: evt.TenantID, SubjectType: evt.SubjectType, StreamKey: evt.StreamKey}
		idKey := dedupeKey(stream, evt.EventID)
		if existing, ok := m.ids[idKey]; ok {
			last = existing
			continue
		}
		route := m.router.EnsureRoute(stream, time.Now().UTC())
		m.lsn[route.PartitionID]++
		lsn := m.lsn[route.PartitionID]
		key := streamKey(stream)
		m.streams[key] = append(m.streams[key], ChronicleRecord{EventID: evt.EventID, LSN: lsn})
		cr := CommitResult{Route: route, Metadata: domain.CommitMetadata{PartitionID: route.PartitionID, LSN: lsn, CommittedAtUTC: time.Now().UTC(), LeaderNodeID: m.nodeID, AckedToUpstream: true}}
		m.ids[idKey] = cr
		last = cr
	}
	return last, nil
}

func (m *InMemoryEngine) GetChronicle(_ context.Context, stream domain.StreamRef) (domain.ChronicleRoute, []ChronicleRecord, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.router.GetRoute(stream)
	if !ok {
		return domain.ChronicleRoute{}, nil, false
	}
	events := append([]ChronicleRecord(nil), m.streams[streamKey(stream)]...)
	return r, events, true
}

func (m *InMemoryEngine) GetChronicleVisualOrder(ctx context.Context, stream domain.StreamRef) (domain.ChronicleRoute, []ChronicleRecord, bool) {
	r, events, ok := m.GetChronicle(ctx, stream)
	if !ok {
		return domain.ChronicleRoute{}, nil, false
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].LSN < events[j].LSN })
	return r, events, true
}

func (m *InMemoryEngine) GetRoute(_ context.Context, stream domain.StreamRef) (domain.ChronicleRoute, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.router.GetRoute(stream)
}

func (m *InMemoryEngine) Health(context.Context) (bool, string) { return true, "ok" }

func streamKey(stream domain.StreamRef) string {
	return stream.TenantID + "::" + stream.SubjectType + "::" + hashroute.CanonicalizeStreamKey(stream.StreamKey)
}

func dedupeKey(stream domain.StreamRef, eventID string) string {
	return streamKey(stream) + "::" + eventID
}
