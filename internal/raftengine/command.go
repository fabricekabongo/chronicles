package raftengine

import "time"

type AppendEntry struct {
	TenantID       string            `json:"tenant_id"`
	SubjectType    string            `json:"subject_type"`
	StreamKey      string            `json:"stream_key"`
	EventID        string            `json:"event_id"`
	EventType      string            `json:"event_type"`
	EventTimeUTCNs int64             `json:"event_time_utc_ns"`
	PayloadJSON    string            `json:"payload_json"`
	Source         string            `json:"source"`
	SourceRef      string            `json:"source_ref"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	AckToken       string            `json:"ack_token,omitempty"`
	ClientRequest  string            `json:"client_request_id,omitempty"`
}

type AppendBatchCommand struct {
	PartitionID    uint8         `json:"partition_id"`
	ExpectedRoutes []string      `json:"expected_routes,omitempty"`
	Entries        []AppendEntry `json:"entries"`
	TimestampUTCNs int64         `json:"timestamp_utc_ns"`
	RouteClose     bool          `json:"route_close,omitempty"`
}

func (c *AppendBatchCommand) FillTimestamp() {
	if c.TimestampUTCNs == 0 {
		c.TimestampUTCNs = time.Now().UTC().UnixNano()
	}
}
