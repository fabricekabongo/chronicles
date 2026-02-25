package core

import (
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

const (
	// PartitionCount is fixed per Step 0 global decisions.
	PartitionCount = 25
)

// Event models the append-only unit written by Chronicles.
//
// Dual-time model:
//   - EventTimeUTC: producer-facing timeline ordering
//   - ReceivedAtUTC/CommittedAtUTC: Chronicles operational truth
//
// Generic stream identity:
//   - SubjectType + StreamKey (not order-specific)
type Event struct {
	SubjectType    string
	StreamKey      string
	EventType      string
	Payload        []byte
	EventTimeUTC   time.Time
	ReceivedAtUTC  time.Time
	CommittedAtUTC time.Time
}

// StreamRoute pins stream routing at first accepted event.
// Once created, CreationDayUTC + PartitionID are immutable for a stream.
type StreamRoute struct {
	SubjectType    string
	StreamKey      string
	CreationDayUTC string // YYYY-MM-DD in UTC
	PartitionID    int
	CreatedAtUTC   time.Time
}

func (sr StreamRoute) Key() string {
	return fmt.Sprintf("%s::%s", sr.SubjectType, sr.StreamKey)
}

// Router creates and resolves immutable stream routes.
// First accepted event creates the route entry (generic to all stream kinds).
type Router struct {
	mu     sync.RWMutex
	routes map[string]StreamRoute
}

func NewRouter() *Router {
	return &Router{routes: make(map[string]StreamRoute)}
}

// EnsureRoute returns existing route or creates one if absent.
// Creation day is pinned to current UTC date at first acceptance.
func (r *Router) EnsureRoute(subjectType, streamKey string, now time.Time) StreamRoute {
	k := routeKey(subjectType, streamKey)
	r.mu.RLock()
	existing, ok := r.routes[k]
	r.mu.RUnlock()
	if ok {
		return existing
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.routes[k]; ok {
		return existing
	}

	nowUTC := now.UTC()
	created := StreamRoute{
		SubjectType:    subjectType,
		StreamKey:      streamKey,
		CreationDayUTC: nowUTC.Format(time.DateOnly),
		PartitionID:    PartitionForStreamKey(streamKey),
		CreatedAtUTC:   nowUTC,
	}
	r.routes[k] = created
	return created
}

func (r *Router) Route(subjectType, streamKey string) (StreamRoute, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	route, ok := r.routes[routeKey(subjectType, streamKey)]
	return route, ok
}

func routeKey(subjectType, streamKey string) string {
	return subjectType + "::" + streamKey
}

// PartitionForStreamKey computes deterministic partition assignment.
// partition_id = hash(stream_key) % 25
func PartitionForStreamKey(streamKey string) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(streamKey))
	return int(h.Sum64() % PartitionCount)
}

// QuorumSize returns simple majority for a replica group size.
// Event ACK is only valid after quorum durable commit.
func QuorumSize(replicaCount int) int {
	if replicaCount <= 0 {
		return 0
	}
	return (replicaCount / 2) + 1
}
