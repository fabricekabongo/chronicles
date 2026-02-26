package storage

import (
	"context"
	"time"

	"chronicles/internal/domain"
)

// AppendEntry is the storage representation for one append-only chronicle event.
type AppendEntry struct {
	LSN             uint64
	Term            uint64
	TenantID        string
	SubjectType     string
	StreamKey       string
	EventNo         *uint64
	EventID         string
	EventType       string
	EventTimeUTCNs  int64
	ReceivedAtUTCNs int64
	PayloadJSON     string
	PayloadEncoding string
	Source          string
	SourceRef       string
	RecordHash      []byte
	PrevStreamHash  []byte
}

// QuerySort controls chronicle query ordering.
type QuerySort int

const (
	SortCommitOrder QuerySort = iota
	SortVisualOrder
)

// Engine is the storage contract for local durable persistence.
type Engine interface {
	EnsureRoute(ctx context.Context, stream domain.StreamRef, partitionID domain.PartitionID, receivedAt time.Time) (domain.ChronicleRoute, error)
	GetRoute(ctx context.Context, stream domain.StreamRef) (domain.ChronicleRoute, bool, error)
	AppendCommittedBatch(ctx context.Context, route domain.ChronicleRoute, term uint64, entries []AppendEntry, committedAt time.Time) error
	MarkCommitted(ctx context.Context, partitionID domain.PartitionID, creationDayUTC string, lsnFrom, lsnTo uint64, committedAt time.Time) error
	GetChronicleByStream(ctx context.Context, stream domain.StreamRef) ([]AppendEntry, error)
	GetChronicleByStreamVisualOrder(ctx context.Context, stream domain.StreamRef) ([]AppendEntry, error)
}
