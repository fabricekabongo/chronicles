package domain

import "time"

type PartitionID uint8

type StreamRef struct {
	TenantID    string
	SubjectType string
	StreamKey   string
}

type EventEnvelope struct {
	TenantID       string
	SubjectType    string
	StreamKey      string
	EventID        string
	EventType      string
	EventTimeUTCNs int64
	Payload        []byte
	Source         string
	SourceRef      string
	ReceivedAtUTC  time.Time
	Metadata       map[string]string
}

type ChronicleRoute struct {
	TenantID             string
	SubjectType          string
	StreamKey            string
	PartitionID          PartitionID
	CreationDayUTC       string
	FirstReceivedAtUTCNs int64
	LastReceivedAtUTCNs  int64
}

type CommitMetadata struct {
	PartitionID     PartitionID
	LSN             uint64
	CommittedAtUTC  time.Time
	LeaderNodeID    string
	QuorumReplicas  int
	AckedToUpstream bool
}
