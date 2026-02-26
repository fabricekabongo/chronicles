package socket

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func startTestServer(t *testing.T, quorumDelay time.Duration) (*Server, string, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s := NewServer(Config{Network: "tcp", Address: "127.0.0.1:0", MaxInflight: 64, GlobalQueueLimit: 2048, AuthToken: "secret"}, NewInMemoryEngine("node-a", quorumDelay))
	go func() { _ = s.Start(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if addr := s.Addr(); addr != "" {
			return s, addr, cancel
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server not started")
	return nil, "", cancel
}

func TestAppendBatchQuorumAck(t *testing.T) {
	srv, addr, cancel := startTestServer(t, 30*time.Millisecond)
	defer cancel()
	defer srv.Close()
	start := time.Now()
	resp, err := DialAndRequest(context.Background(), "tcp", addr, &SocketRequest{RequestId: "a1", AuthToken: "secret", Operation: int32(OperationAppendBatch), AppendBatch: &AppendBatchRequest{Events: []*Event{{TenantId: "t", SubjectType: "order", StreamKey: "o1", EventId: "e1"}, {TenantId: "t", SubjectType: "order", StreamKey: "o1", EventId: "e2"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != int32(ErrorCodeOK) || resp.Append == nil || !resp.Append.Accepted {
		t.Fatalf("bad response: %+v", resp)
	}
	if time.Since(start) < 25*time.Millisecond {
		t.Fatalf("expected quorum wait, got %v", time.Since(start))
	}
}

func TestConcurrentLoad(t *testing.T) {
	srv, addr, cancel := startTestServer(t, 0)
	defer cancel()
	defer srv.Close()

	const clients = 20
	const perClient = 40
	var wg sync.WaitGroup
	errCh := make(chan error, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for j := 0; j < perClient; j++ {
				id := fmt.Sprintf("%d-%d", c, j)
				resp, err := DialAndRequest(context.Background(), "tcp", addr, &SocketRequest{RequestId: id, AuthToken: "secret", Operation: int32(OperationAppend), Append: &AppendRequest{Event: &Event{TenantId: "t", SubjectType: "order", StreamKey: fmt.Sprintf("k-%d", c%4), EventId: id}}})
				if err != nil {
					errCh <- err
					return
				}
				if resp.ErrorCode != int32(ErrorCodeOK) {
					errCh <- fmt.Errorf("code=%d", resp.ErrorCode)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestIdempotentAppend(t *testing.T) {
	srv, addr, cancel := startTestServer(t, 0)
	defer cancel()
	defer srv.Close()
	req := &SocketRequest{RequestId: "r1", AuthToken: "secret", Operation: int32(OperationAppend), Append: &AppendRequest{Event: &Event{TenantId: "t", SubjectType: "order", StreamKey: "s", EventId: "evt-1"}}}
	first, err := DialAndRequest(context.Background(), "tcp", addr, req)
	if err != nil {
		t.Fatal(err)
	}
	req.RequestId = "r2"
	second, err := DialAndRequest(context.Background(), "tcp", addr, req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Append.Lsn != second.Append.Lsn {
		t.Fatalf("expected idempotent lsn, got %d vs %d", first.Append.Lsn, second.Append.Lsn)
	}
}
