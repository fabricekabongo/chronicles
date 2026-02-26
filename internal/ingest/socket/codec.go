package socket

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

const MaxFrameSize = 8 << 20

func WriteFrame(w io.Writer, payload []byte) error {
	if len(payload) > MaxFrameSize {
		return fmt.Errorf("frame too large: %d", len(payload))
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func ReadFrame(r *bufio.Reader) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	sz := binary.BigEndian.Uint32(header)
	if sz == 0 {
		return nil, fmt.Errorf("empty frame")
	}
	if sz > MaxFrameSize {
		return nil, fmt.Errorf("frame too large: %d", sz)
	}
	payload := make([]byte, int(sz))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}
