package socket

import (
	"bufio"
	"bytes"
	"testing"
)

func FuzzReadFrame(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 0x2a})
	f.Add([]byte{0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ReadFrame(bufio.NewReader(bytes.NewReader(data)))
	})
}

func FuzzUnmarshalRequest(f *testing.F) {
	f.Add([]byte{0x08, 0x01})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = UnmarshalRequest(data)
	})
}
