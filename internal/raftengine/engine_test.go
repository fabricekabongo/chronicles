package raftengine

import (
	"context"
	"errors"
	"fmt"
	"go.etcd.io/raft/v3"
	"net"
	"sync"
	"testing"
	"time"
)

type nopLogger struct{}

func (nopLogger) Debug(...any)            {}
func (nopLogger) Debugf(string, ...any)   {}
func (nopLogger) Info(...any)             {}
func (nopLogger) Infof(string, ...any)    {}
func (nopLogger) Warning(...any)          {}
func (nopLogger) Warningf(string, ...any) {}
func (nopLogger) Error(...any)            {}
func (nopLogger) Errorf(string, ...any)   {}
func (nopLogger) Fatal(...any)            {}
func (nopLogger) Fatalf(string, ...any)   {}
func (nopLogger) Panic(...any)            {}
func (nopLogger) Panicf(string, ...any)   {}

func init() {
	raft.SetLogger(nopLogger{})
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

type applyRecorder struct {
	mu      sync.Mutex
	applied map[string]int
	acks    map[string]int
}

func newApplyRecorder() *applyRecorder {
	return &applyRecorder{applied: map[string]int{}, acks: map[string]int{}}
}

func (r *applyRecorder) apply(_ uint8, cmd AppendBatchCommand) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range cmd.Entries {
		r.applied[e.EventID]++
	}
}

func (r *applyRecorder) ack(token string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.acks[token]++
}

func waitForLeader(t *testing.T, nodes map[uint64]*Engine, partition uint8) uint64 {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		leaders := map[uint64]int{}
		var leader uint64
		for _, n := range nodes {
			if n.IsLeader(partition) {
				leader = n.cfg.NodeID
				leaders[leader]++
			}
		}
		if len(leaders) == 1 {
			return leader
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no single leader elected for partition=%d", partition)
	return 0
}

func TestThreeNodeQuorumAndRecovery(t *testing.T) {
	addrs := map[uint64]string{1: freePort(t), 2: freePort(t), 3: freePort(t)}
	persist := map[uint64]*Persistence{1: NewPersistence(), 2: NewPersistence(), 3: NewPersistence()}
	rec := map[uint64]*applyRecorder{1: newApplyRecorder(), 2: newApplyRecorder(), 3: newApplyRecorder()}

	newNode := func(id uint64, bootstrap bool) *Engine {
		n, err := NewEngine(Config{NodeID: id, Address: addrs[id], PeerAddresses: addrs, Persistence: persist[id], BootstrapNewCluster: bootstrap, Apply: rec[id].apply, Ack: rec[id].ack})
		if err != nil {
			t.Fatal(err)
		}
		n.Start()
		return n
	}

	nodes := map[uint64]*Engine{1: newNode(1, true), 2: newNode(2, true), 3: newNode(3, true)}
	defer func() {
		for _, n := range nodes {
			_ = n.Stop()
		}
	}()

	leaderID := waitForLeader(t, nodes, 7)
	leader := nodes[leaderID]

	if err := leader.Propose(context.Background(), AppendBatchCommand{PartitionID: 7, Entries: []AppendEntry{{EventID: "e1", AckToken: "a1"}}}); err != nil {
		t.Fatalf("propose: %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	for id, r := range rec {
		if r.applied["e1"] != 1 {
			t.Fatalf("node %d did not apply committed entry", id)
		}
		if r.acks["a1"] != 1 {
			t.Fatalf("node %d missing ack callback", id)
		}
	}

	// Stop one follower; 2/3 should still commit.
	for id, n := range nodes {
		if id != leaderID {
			_ = n.Stop()
			delete(nodes, id)
			break
		}
	}
	if err := leader.Propose(context.Background(), AppendBatchCommand{PartitionID: 7, Entries: []AppendEntry{{EventID: "e2", AckToken: "a2"}}}); err != nil {
		t.Fatalf("propose with one down: %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	if rec[leaderID].applied["e2"] != 1 {
		t.Fatalf("leader did not apply e2")
	}

	// stop second node, no quorum left.
	for id, n := range nodes {
		if id != leaderID {
			_ = n.Stop()
			delete(nodes, id)
			break
		}
	}
	time.Sleep(500 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := leader.Propose(ctx, AppendBatchCommand{PartitionID: 7, Entries: []AppendEntry{{EventID: "e3", AckToken: "a3"}}})
	if err == nil {
		t.Fatalf("expected proposal failure without quorum")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrNotLeader) {
		// etcd/raft may return proposal dropped/ctx errors depending on timing.
		t.Logf("proposal failure without quorum: %v", err)
	}
	if rec[leaderID].acks["a3"] != 0 {
		t.Fatalf("ack should not happen without quorum")
	}
}

func TestLeaderCrashRestartRecovery(t *testing.T) {
	addrs := map[uint64]string{1: freePort(t), 2: freePort(t), 3: freePort(t)}
	persist := map[uint64]*Persistence{1: NewPersistence(), 2: NewPersistence(), 3: NewPersistence()}
	rec := map[uint64]*applyRecorder{1: newApplyRecorder(), 2: newApplyRecorder(), 3: newApplyRecorder()}

	newNode := func(id uint64, bootstrap bool) *Engine {
		n, err := NewEngine(Config{NodeID: id, Address: addrs[id], PeerAddresses: addrs, Persistence: persist[id], BootstrapNewCluster: bootstrap, Apply: rec[id].apply, Ack: rec[id].ack})
		if err != nil {
			t.Fatal(err)
		}
		n.Start()
		return n
	}

	nodes := map[uint64]*Engine{1: newNode(1, true), 2: newNode(2, true), 3: newNode(3, true)}
	defer func() {
		for _, n := range nodes {
			_ = n.Stop()
		}
	}()

	partition := uint8(11)
	leaderID := waitForLeader(t, nodes, partition)
	crashed := nodes[leaderID]
	_ = crashed.Stop()
	delete(nodes, leaderID)

	newLeader := waitForLeader(t, nodes, partition)
	if err := nodes[newLeader].Propose(context.Background(), AppendBatchCommand{PartitionID: partition, Entries: []AppendEntry{{EventID: "recovery-1", AckToken: "recovery-ack"}}}); err != nil {
		t.Fatalf("propose while crashed: %v", err)
	}
	time.Sleep(400 * time.Millisecond)

	restarted := newNode(leaderID, false)
	nodes[leaderID] = restarted
	time.Sleep(900 * time.Millisecond)

	if err := nodes[newLeader].Propose(context.Background(), AppendBatchCommand{PartitionID: partition, Entries: []AppendEntry{{EventID: "recovery-2", AckToken: "recovery-ack-2"}}}); err != nil {
		t.Fatalf("propose after restart: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if rec[leaderID].applied["recovery-2"] == 0 {
		t.Fatalf("restarted node did not catch up")
	}
}

func TestPartitionLeadershipAndNoSplitBrain(t *testing.T) {
	addrs := map[uint64]string{1: freePort(t), 2: freePort(t), 3: freePort(t)}
	persist := map[uint64]*Persistence{1: NewPersistence(), 2: NewPersistence(), 3: NewPersistence()}
	nodes := map[uint64]*Engine{}
	for _, id := range []uint64{1, 2, 3} {
		n, err := NewEngine(Config{NodeID: id, Address: addrs[id], PeerAddresses: addrs, Persistence: persist[id], BootstrapNewCluster: true})
		if err != nil {
			t.Fatal(err)
		}
		n.Start()
		nodes[id] = n
	}
	defer func() {
		for _, n := range nodes {
			_ = n.Stop()
		}
	}()

	for p := 0; p < 25; p++ {
		leader := waitForLeader(t, nodes, uint8(p))
		if leader == 0 {
			t.Fatalf("partition %d missing leader", p)
		}
		leaders := 0
		for _, n := range nodes {
			if n.IsLeader(uint8(p)) {
				leaders++
			}
		}
		if leaders != 1 {
			t.Fatalf("split brain detected partition=%d leaders=%d", p, leaders)
		}
	}

	// follower rejects external writes.
	partition := uint8(3)
	leader := waitForLeader(t, nodes, partition)
	for id, n := range nodes {
		if id == leader {
			continue
		}
		err := n.Propose(context.Background(), AppendBatchCommand{PartitionID: partition, Entries: []AppendEntry{{EventID: fmt.Sprintf("follower-%d", id)}}})
		if !errors.Is(err, ErrNotLeader) {
			t.Fatalf("expected follower reject, got %v", err)
		}
	}
}
