package raftengine

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

type messageHandler func(partition uint8, msg raftpb.Message)

type tcpTransport struct {
	nodeID   uint64
	addr     string
	handler  messageHandler
	listener net.Listener

	mu       sync.Mutex
	peers    map[uint64]string
	outbound map[uint64]map[uint8]chan raftpb.Message
	closed   chan struct{}
}

func newTCPTransport(nodeID uint64, addr string, peers map[uint64]string, handler messageHandler) (*tcpTransport, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	t := &tcpTransport{nodeID: nodeID, addr: addr, peers: peers, handler: handler, listener: ln, outbound: make(map[uint64]map[uint8]chan raftpb.Message), closed: make(chan struct{})}
	for peer := range peers {
		if peer == nodeID {
			continue
		}
		t.outbound[peer] = make(map[uint8]chan raftpb.Message)
		for p := 0; p < 25; p++ {
			ch := make(chan raftpb.Message, 128)
			t.outbound[peer][uint8(p)] = ch
			go t.sender(peer, uint8(p), ch)
		}
	}
	go t.acceptLoop()
	return t, nil
}

func (t *tcpTransport) send(to uint64, partition uint8, msg raftpb.Message) error {
	t.mu.Lock()
	parts, ok := t.outbound[to]
	t.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown peer %d", to)
	}
	ch := parts[partition]
	select {
	case ch <- msg:
		return nil
	default:
		return fmt.Errorf("peer %d partition %d queue full", to, partition)
	}
}

func (t *tcpTransport) sender(peer uint64, partition uint8, ch <-chan raftpb.Message) {
	for {
		select {
		case <-t.closed:
			return
		case msg := <-ch:
			addr := t.peers[peer]
			conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
			if err != nil {
				continue
			}
			_ = conn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
			if err := writeEnvelope(conn, partition, msg); err != nil {
				_ = conn.Close()
				continue
			}
			_ = conn.Close()
		}
	}
}

func (t *tcpTransport) acceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.closed:
				return
			default:
			}
			continue
		}
		go func(c net.Conn) {
			defer c.Close()
			_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
			partition, msg, err := readEnvelope(c)
			if err != nil {
				return
			}
			t.handler(partition, msg)
		}(conn)
	}
}

func (t *tcpTransport) close() error {
	close(t.closed)
	return t.listener.Close()
}

func writeEnvelope(w io.Writer, partition uint8, msg raftpb.Message) error {
	b, err := msg.Marshal()
	if err != nil {
		return err
	}
	payload := make([]byte, 1+len(b))
	payload[0] = partition
	copy(payload[1:], b)
	if err := binary.Write(w, binary.BigEndian, uint32(len(payload))); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}

func readEnvelope(r io.Reader) (uint8, raftpb.Message, error) {
	var sz uint32
	if err := binary.Read(r, binary.BigEndian, &sz); err != nil {
		return 0, raftpb.Message{}, err
	}
	br := bufio.NewReader(r)
	buf := make([]byte, sz)
	if _, err := io.ReadFull(br, buf); err != nil {
		return 0, raftpb.Message{}, err
	}
	if len(buf) < 1 {
		return 0, raftpb.Message{}, io.ErrUnexpectedEOF
	}
	var msg raftpb.Message
	if err := msg.Unmarshal(buf[1:]); err != nil {
		return 0, raftpb.Message{}, err
	}
	return buf[0], msg, nil
}
