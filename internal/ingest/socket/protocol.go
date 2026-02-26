package socket

import (
	"fmt"

	"github.com/golang/protobuf/proto"
)

type Operation int32

const (
	OperationUnknown                 Operation = 0
	OperationAppend                  Operation = 1
	OperationAppendBatch             Operation = 2
	OperationPing                    Operation = 3
	OperationGetChronicle            Operation = 4
	OperationGetChronicleVisualOrder Operation = 5
	OperationGetRoute                Operation = 6
	OperationHealth                  Operation = 7
)

type ErrorCode int32

const (
	ErrorCodeOK              ErrorCode = 0
	ErrorCodeBadRequest      ErrorCode = 1
	ErrorCodeUnauthenticated ErrorCode = 2
	ErrorCodeNotFound        ErrorCode = 3
	ErrorCodeOverloaded      ErrorCode = 4
	ErrorCodeInternal        ErrorCode = 5
)

type SocketRequest struct {
	RequestId    string              `protobuf:"bytes,1,opt,name=request_id,json=requestId,proto3"`
	AuthToken    string              `protobuf:"bytes,2,opt,name=auth_token,json=authToken,proto3"`
	Operation    int32               `protobuf:"varint,3,opt,name=operation,proto3"`
	Append       *AppendRequest      `protobuf:"bytes,4,opt,name=append,proto3"`
	AppendBatch  *AppendBatchRequest `protobuf:"bytes,5,opt,name=append_batch,json=appendBatch,proto3"`
	GetRoute     *RouteQuery         `protobuf:"bytes,6,opt,name=get_route,json=getRoute,proto3"`
	GetChronicle *ChronicleQuery     `protobuf:"bytes,7,opt,name=get_chronicle,json=getChronicle,proto3"`
	Ping         *PingRequest        `protobuf:"bytes,8,opt,name=ping,proto3"`
}

func (*SocketRequest) Reset()         {}
func (*SocketRequest) String() string { return "SocketRequest" }
func (*SocketRequest) ProtoMessage()  {}

type SocketResponse struct {
	RequestId    string             `protobuf:"bytes,1,opt,name=request_id,json=requestId,proto3"`
	ErrorCode    int32              `protobuf:"varint,2,opt,name=error_code,json=errorCode,proto3"`
	ErrorMessage string             `protobuf:"bytes,3,opt,name=error_message,json=errorMessage,proto3"`
	Append       *AppendResponse    `protobuf:"bytes,4,opt,name=append,proto3"`
	Pong         *PongResponse      `protobuf:"bytes,5,opt,name=pong,proto3"`
	Route        *RouteResponse     `protobuf:"bytes,6,opt,name=route,proto3"`
	Chronicle    *ChronicleResponse `protobuf:"bytes,7,opt,name=chronicle,proto3"`
	Health       *HealthResponse    `protobuf:"bytes,8,opt,name=health,proto3"`
}

func (*SocketResponse) Reset()         {}
func (*SocketResponse) String() string { return "SocketResponse" }
func (*SocketResponse) ProtoMessage()  {}

type Event struct {
	TenantId       string `protobuf:"bytes,1,opt,name=tenant_id,json=tenantId,proto3"`
	SubjectType    string `protobuf:"bytes,2,opt,name=subject_type,json=subjectType,proto3"`
	StreamKey      string `protobuf:"bytes,3,opt,name=stream_key,json=streamKey,proto3"`
	EventId        string `protobuf:"bytes,4,opt,name=event_id,json=eventId,proto3"`
	EventType      string `protobuf:"bytes,5,opt,name=event_type,json=eventType,proto3"`
	EventTimeUtcNs int64  `protobuf:"varint,6,opt,name=event_time_utc_ns,json=eventTimeUtcNs,proto3"`
	Payload        []byte `protobuf:"bytes,7,opt,name=payload,proto3"`
	Source         string `protobuf:"bytes,8,opt,name=source,proto3"`
	SourceRef      string `protobuf:"bytes,9,opt,name=source_ref,json=sourceRef,proto3"`
}

func (*Event) Reset()         {}
func (*Event) String() string { return "Event" }
func (*Event) ProtoMessage()  {}

type AppendRequest struct {
	Event *Event `protobuf:"bytes,1,opt,name=event,proto3"`
}

func (*AppendRequest) Reset()         {}
func (*AppendRequest) String() string { return "AppendRequest" }
func (*AppendRequest) ProtoMessage()  {}

type AppendBatchRequest struct {
	Events []*Event `protobuf:"bytes,1,rep,name=events,proto3"`
}

func (*AppendBatchRequest) Reset()         {}
func (*AppendBatchRequest) String() string { return "AppendBatchRequest" }
func (*AppendBatchRequest) ProtoMessage()  {}

type AppendResponse struct {
	Accepted         bool   `protobuf:"varint,1,opt,name=accepted,proto3"`
	PartitionId      uint32 `protobuf:"varint,2,opt,name=partition_id,json=partitionId,proto3"`
	CreationDayUtc   string `protobuf:"bytes,3,opt,name=creation_day_utc,json=creationDayUtc,proto3"`
	Lsn              uint64 `protobuf:"varint,4,opt,name=lsn,proto3"`
	CommittedAtUtcNs int64  `protobuf:"varint,5,opt,name=committed_at_utc_ns,json=committedAtUtcNs,proto3"`
	LeaderNodeId     string `protobuf:"bytes,6,opt,name=leader_node_id,json=leaderNodeId,proto3"`
}

func (*AppendResponse) Reset()         {}
func (*AppendResponse) String() string { return "AppendResponse" }
func (*AppendResponse) ProtoMessage()  {}

type PingRequest struct{}

func (*PingRequest) Reset()         {}
func (*PingRequest) String() string { return "PingRequest" }
func (*PingRequest) ProtoMessage()  {}

type PongResponse struct {
	UnixTimeNs int64 `protobuf:"varint,1,opt,name=unix_time_ns,json=unixTimeNs,proto3"`
}

func (*PongResponse) Reset()         {}
func (*PongResponse) String() string { return "PongResponse" }
func (*PongResponse) ProtoMessage()  {}

type RouteQuery struct{ TenantId, SubjectType, StreamKey string }

func (*RouteQuery) Reset()         {}
func (*RouteQuery) String() string { return "RouteQuery" }
func (*RouteQuery) ProtoMessage()  {}

type ChronicleQuery struct{ TenantId, SubjectType, StreamKey string }

func (*ChronicleQuery) Reset()         {}
func (*ChronicleQuery) String() string { return "ChronicleQuery" }
func (*ChronicleQuery) ProtoMessage()  {}

type RouteResponse struct {
	Found          bool   `protobuf:"varint,1,opt,name=found,proto3"`
	PartitionId    uint32 `protobuf:"varint,2,opt,name=partition_id,json=partitionId,proto3"`
	CreationDayUtc string `protobuf:"bytes,3,opt,name=creation_day_utc,json=creationDayUtc,proto3"`
}

func (*RouteResponse) Reset()         {}
func (*RouteResponse) String() string { return "RouteResponse" }
func (*RouteResponse) ProtoMessage()  {}

type ChronicleEvent struct {
	EventId string `protobuf:"bytes,1,opt,name=event_id,json=eventId,proto3"`
	Lsn     uint64 `protobuf:"varint,2,opt,name=lsn,proto3"`
}

func (*ChronicleEvent) Reset()         {}
func (*ChronicleEvent) String() string { return "ChronicleEvent" }
func (*ChronicleEvent) ProtoMessage()  {}

type ChronicleResponse struct {
	Found          bool              `protobuf:"varint,1,opt,name=found,proto3"`
	PartitionId    uint32            `protobuf:"varint,2,opt,name=partition_id,json=partitionId,proto3"`
	CreationDayUtc string            `protobuf:"bytes,3,opt,name=creation_day_utc,json=creationDayUtc,proto3"`
	Events         []*ChronicleEvent `protobuf:"bytes,4,rep,name=events,proto3"`
}

func (*ChronicleResponse) Reset()         {}
func (*ChronicleResponse) String() string { return "ChronicleResponse" }
func (*ChronicleResponse) ProtoMessage()  {}

type HealthResponse struct {
	Ok      bool   `protobuf:"varint,1,opt,name=ok,proto3"`
	Message string `protobuf:"bytes,2,opt,name=message,proto3"`
}

func (*HealthResponse) Reset()         {}
func (*HealthResponse) String() string { return "HealthResponse" }
func (*HealthResponse) ProtoMessage()  {}

func MarshalMessage(msg proto.Message) ([]byte, error) { return proto.Marshal(msg) }

func UnmarshalRequest(payload []byte) (*SocketRequest, error) {
	var req SocketRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func UnmarshalResponse(payload []byte) (*SocketResponse, error) {
	var res SocketResponse
	if err := proto.Unmarshal(payload, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func ValidateRequest(req *SocketRequest) error {
	if req == nil {
		return fmt.Errorf("nil request")
	}
	if req.Operation == int32(OperationUnknown) {
		return fmt.Errorf("operation is required")
	}
	return nil
}
