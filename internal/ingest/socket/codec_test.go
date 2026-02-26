package socket

import (
	"bufio"
	"bytes"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	in := []byte("hello")
	var b bytes.Buffer
	if err := WriteFrame(&b, in); err != nil {
		t.Fatal(err)
	}
	out, err := ReadFrame(bufio.NewReader(&b))
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != string(in) {
		t.Fatalf("got %q", out)
	}
}

func TestFrameRejectsOversized(t *testing.T) {
	tooBig := make([]byte, MaxFrameSize+1)
	var b bytes.Buffer
	if err := WriteFrame(&b, tooBig); err == nil {
		t.Fatal("expected error")
	}
}

func TestProtoRoundTrip(t *testing.T) {
	req := &SocketRequest{RequestId: "1", Operation: int32(OperationPing), Ping: &PingRequest{}}
	payload, err := MarshalMessage(req)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalRequest(payload)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.RequestId != "1" || Operation(decoded.Operation) != OperationPing {
		t.Fatalf("bad decode: %+v", decoded)
	}
}
