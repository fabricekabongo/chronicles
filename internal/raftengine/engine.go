package raftengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var ErrNotLeader = errors.New("partition leader required")

type ApplyFunc func(partition uint8, cmd AppendBatchCommand)
type AckFunc func(token string)

type Config struct {
	NodeID              uint64
	Address             string
	PeerAddresses       map[uint64]string
	TickInterval        time.Duration
	ElectionTicks       int
	HeartbeatTicks      int
	MaxInflightMsgs     int
	MaxMessageSize      uint64
	Persistence         *Persistence
	Apply               ApplyFunc
	Ack                 AckFunc
	BootstrapNewCluster bool
}

type Persistence struct {
	mu      sync.Mutex
	storage map[uint8]*raft.MemoryStorage
}

func NewPersistence() *Persistence { return &Persistence{storage: map[uint8]*raft.MemoryStorage{}} }

func (p *Persistence) forPartition(partition uint8) *raft.MemoryStorage {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s, ok := p.storage[partition]; ok {
		return s
	}
	s := raft.NewMemoryStorage()
	p.storage[partition] = s
	return s
}

type Engine struct {
	cfg       Config
	transport *tcpTransport
	workers   [25]*partitionWorker
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

type partitionWorker struct {
	partition uint8
	node      raft.Node
	storage   *raft.MemoryStorage
	proposeCh chan []byte
}

func NewEngine(cfg Config) (*Engine, error) {
	if cfg.Persistence == nil {
		cfg.Persistence = NewPersistence()
	}
	if cfg.TickInterval == 0 {
		cfg.TickInterval = 20 * time.Millisecond
	}
	if cfg.ElectionTicks == 0 {
		cfg.ElectionTicks = 10
	}
	if cfg.HeartbeatTicks == 0 {
		cfg.HeartbeatTicks = 1
	}
	if cfg.MaxInflightMsgs == 0 {
		cfg.MaxInflightMsgs = 256
	}
	if cfg.MaxMessageSize == 0 {
		cfg.MaxMessageSize = 1024 * 1024
	}

	e := &Engine{cfg: cfg, stopCh: make(chan struct{})}
	t, err := newTCPTransport(cfg.NodeID, cfg.Address, cfg.PeerAddresses, func(partition uint8, msg raftpb.Message) {
		if partition >= 25 || e.workers[partition] == nil {
			return
		}
		e.workers[partition].node.Step(context.Background(), msg)
	})
	if err != nil {
		return nil, err
	}
	e.transport = t

	peers := make([]raft.Peer, 0, len(cfg.PeerAddresses))
	for id := range cfg.PeerAddresses {
		peers = append(peers, raft.Peer{ID: id})
	}

	for p := 0; p < 25; p++ {
		ms := cfg.Persistence.forPartition(uint8(p))
		rc := &raft.Config{ID: cfg.NodeID, ElectionTick: cfg.ElectionTicks, HeartbeatTick: cfg.HeartbeatTicks, Storage: ms, MaxSizePerMsg: cfg.MaxMessageSize, MaxInflightMsgs: cfg.MaxInflightMsgs, CheckQuorum: true, PreVote: true}
		var n raft.Node
		if cfg.BootstrapNewCluster {
			n = raft.StartNode(rc, peers)
		} else {
			n = raft.RestartNode(rc)
		}
		e.workers[p] = &partitionWorker{partition: uint8(p), node: n, storage: ms, proposeCh: make(chan []byte, 256)}
	}
	return e, nil
}

func (e *Engine) Start() {
	for _, w := range e.workers {
		e.wg.Add(1)
		go e.runPartition(w)
	}
}

func (e *Engine) Stop() error {
	close(e.stopCh)
	for _, w := range e.workers {
		w.node.Stop()
	}
	e.wg.Wait()
	return e.transport.close()
}

func (e *Engine) Leader(partition uint8) uint64 { return e.workers[partition].node.Status().Lead }

func (e *Engine) IsLeader(partition uint8) bool {
	return e.workers[partition].node.Status().RaftState == raft.StateLeader
}

func (e *Engine) Propose(ctx context.Context, cmd AppendBatchCommand) error {
	if cmd.PartitionID >= 25 {
		return fmt.Errorf("invalid partition")
	}
	cmd.FillTimestamp()
	w := e.workers[cmd.PartitionID]
	if w.node.Status().RaftState != raft.StateLeader {
		return fmt.Errorf("%w: leader=%d", ErrNotLeader, w.node.Status().Lead)
	}
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	return w.node.Propose(ctx, b)
}

func (e *Engine) runPartition(w *partitionWorker) {
	defer e.wg.Done()
	ticker := time.NewTicker(e.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			w.node.Tick()
		case rd := <-w.node.Ready():
			if !raft.IsEmptySnap(rd.Snapshot) {
				_ = w.storage.ApplySnapshot(rd.Snapshot)
			}
			if !raft.IsEmptyHardState(rd.HardState) {
				_ = w.storage.SetHardState(rd.HardState)
			}
			_ = w.storage.Append(rd.Entries)
			for _, m := range rd.Messages {
				_ = e.transport.send(m.To, w.partition, m)
			}
			for _, ent := range rd.CommittedEntries {
				if ent.Type != raftpb.EntryNormal || len(ent.Data) == 0 {
					continue
				}
				var cmd AppendBatchCommand
				if err := json.Unmarshal(ent.Data, &cmd); err != nil {
					continue
				}
				if e.cfg.Apply != nil {
					e.cfg.Apply(w.partition, cmd)
				}
				if e.cfg.Ack != nil {
					for _, entry := range cmd.Entries {
						if entry.AckToken != "" {
							e.cfg.Ack(entry.AckToken)
						}
					}
				}
			}
			w.node.Advance()
		}
	}
}
