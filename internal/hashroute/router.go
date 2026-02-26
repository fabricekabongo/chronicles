package hashroute

import (
	"sync"
	"time"

	"chronicles/internal/domain"
)

type Router struct {
	mu     sync.RWMutex
	routes map[string]domain.ChronicleRoute
}

func NewRouter() *Router {
	return &Router{routes: make(map[string]domain.ChronicleRoute)}
}

func (r *Router) EnsureRoute(stream domain.StreamRef, receivedAt time.Time) domain.ChronicleRoute {
	k := key(stream)

	r.mu.RLock()
	route, ok := r.routes[k]
	r.mu.RUnlock()
	if ok {
		return route
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if route, ok := r.routes[k]; ok {
		return route
	}

	rt := receivedAt.UTC()
	created := domain.ChronicleRoute{
		TenantID:             stream.TenantID,
		SubjectType:          stream.SubjectType,
		StreamKey:            CanonicalizeStreamKey(stream.StreamKey),
		PartitionID:          domain.PartitionID(PartitionForStreamKey(stream.StreamKey)),
		CreationDayUTC:       rt.Format(time.DateOnly),
		FirstReceivedAtUTCNs: rt.UnixNano(),
		LastReceivedAtUTCNs:  rt.UnixNano(),
	}

	r.routes[k] = created
	return created
}

func (r *Router) GetRoute(stream domain.StreamRef) (domain.ChronicleRoute, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	route, ok := r.routes[key(stream)]
	return route, ok
}

func key(stream domain.StreamRef) string {
	return stream.TenantID + "::" + stream.SubjectType + "::" + CanonicalizeStreamKey(stream.StreamKey)
}
