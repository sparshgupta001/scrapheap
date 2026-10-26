package proto

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
)

func TestJSONRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  Message
	}{
		{"hello", Message{Type: MessageHello, DeviceID: "phone-1", UserAgent: "test browser", Cores: 4, Threads: 2, Mock: true}},
		{"hello mock default", Message{Type: MessageHello, DeviceID: "phone-1", UserAgent: "test browser", Cores: 4, Threads: 2}},
		{"bench result", Message{Type: MessageBenchResult, TilesPerSec: 12.5, PerThread: 6.25, Threads: 2, Hash: strings.Repeat("ab", 32)}},
		{"request task", Message{Type: MessageRequestTask}},
		{"heartbeat", Message{Type: MessageHeartbeat}},
		{"ack", Message{Type: MessageAck, TaskID: 7, Attempt: 3, JobID: 19}},
		{"job done", Message{Type: MessageJobDone, JobID: 19}},
		{"welcome", Message{Type: MessageWelcome, HeartbeatIntervalMs: 1000, HeartbeatTimeoutMs: 5000}},
		{"job started", Message{Type: MessageJobStarted, JobID: 19}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeJSON(tt.msg)
			if err != nil {
				t.Fatalf("EncodeJSON: %v", err)
			}
			got, err := DecodeJSON(encoded)
			if err != nil {
				t.Fatalf("DecodeJSON: %v", err)
			}
			if got != tt.msg {
				t.Fatalf("round trip = %#v, want %#v", got, tt.msg)
			}
		})
	}
}

func TestJSONUnknownMessageTypeIsTyped(t *testing.T) {
	_, err := DecodeJSON([]byte(`{"type":"UNKNOWN","future":true}`))
	var unknown *UnknownMessageTypeError
	if !errors.As(err, &unknown) || unknown.Type != "UNKNOWN" {
		t.Fatalf("DecodeJSON error = %#v, want UnknownMessageTypeError", err)
	}
}

func TestJSONUnknownExtraFieldsAreIgnored(t *testing.T) {
	got, err := DecodeJSON([]byte(`{"type":"HELLO","deviceId":"d","userAgent":"ua","cores":4,"threads":2,"future":{"x":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got.Mock {
		t.Fatal("omitted mock should default to false")
	}
}

func TestJSONMalformed(t *testing.T) {
	tests := []struct{ name, input string }{
		{"invalid json", `{`},
		{"missing hello field", `{"type":"HELLO","deviceId":"d","cores":1,"threads":1}`},
		{"zero cores", `{"type":"HELLO","deviceId":"d","userAgent":"ua","cores":0,"threads":1}`},
		{"zero hello threads", `{"type":"HELLO","deviceId":"d","userAgent":"ua","cores":4,"threads":0}`},
		{"too many hello threads", `{"type":"HELLO","deviceId":"d","userAgent":"ua","cores":4,"threads":17}`},
		{"attempt does not fit uint16", `{"type":"ACK","jobId":1,"taskId":1,"attempt":65536}`},
		{"bench missing hash", `{"type":"BENCH_RESULT","tilesPerSec":12.5,"perThread":6.25,"threads":2}`},
		{"bench malformed hash", `{"type":"BENCH_RESULT","tilesPerSec":12.5,"perThread":6.25,"threads":2,"hash":"xyz"}`},
		{"bench non-hex hash", `{"type":"BENCH_RESULT","tilesPerSec":12.5,"perThread":6.25,"threads":2,"hash":"gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg"}`},
		{"bench zero perThread", `{"type":"BENCH_RESULT","tilesPerSec":12.5,"perThread":0,"threads":2,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`},
		{"bench too many threads", `{"type":"BENCH_RESULT","tilesPerSec":12.5,"perThread":6.25,"threads":17,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`},
		{"ack missing job id", `{"type":"ACK","taskId":7,"attempt":1}`},
		{"job done missing job id", `{"type":"JOB_DONE"}`},
		{"welcome zero interval", `{"type":"WELCOME","heartbeatIntervalMs":0,"heartbeatTimeoutMs":5000}`},
		{"trailing json", `{"type":"JOB_DONE","jobId":1}{}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DecodeJSON([]byte(tt.input)); err == nil {
				t.Fatal("DecodeJSON unexpectedly succeeded")
			}
		})
	}
}

func TestBinaryExactBytesAndRoundTrip(t *testing.T) {
	t.Run("TASK", func(t *testing.T) {
		want := Task{JobID: 42, TaskID: 0x01020304, Attempt: 2, X: 10, Y: 20, W: 2, H: 1, X0: -2, Y0: 0.5, Scale: 1, MaxIter: 256}
		encoded, err := EncodeTask(want)
		if err != nil {
			t.Fatal(err)
		}
		expected := []byte{
			0x01, 0x00, 0x02, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x04, 0x03, 0x02, 0x01,
			0x0a, 0x00, 0x14, 0x00, 0x02, 0x00, 0x01, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe0, 0x3f,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
			0x00, 0x01, 0x00, 0x00,
		}
		if len(encoded) != 48 || !bytes.Equal(encoded, expected) {
			t.Fatalf("TASK bytes (%d) = % x, want % x", len(encoded), encoded, expected)
		}
		encoded[1] = 0x7f // Reserved is ignored on read.
		got, err := DecodeTask(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	})
	t.Run("RESULT", func(t *testing.T) {
		want := Result{JobID: 42, TaskID: 0x01020304, Attempt: 2, W: 2, H: 1, ComputeMs: 300, RGBA: []byte{0, 1, 2, 255, 16, 17, 18, 255}}
		encoded, err := EncodeResult(want)
		if err != nil {
			t.Fatal(err)
		}
		expected := []byte{0x02, 0x00, 0x02, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x04, 0x03, 0x02, 0x01, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00, 0x96, 0x43, 0x00, 0x01, 0x02, 0xff, 0x10, 0x11, 0x12, 0xff}
		if len(encoded) != 28 || !bytes.Equal(encoded, expected) {
			t.Fatalf("RESULT bytes (%d) = % x, want % x", len(encoded), encoded, expected)
		}
		encoded[1] = 0xfe // Reserved is ignored on read.
		got, err := DecodeResult(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if got.JobID != want.JobID || got.TaskID != want.TaskID || got.Attempt != want.Attempt || got.W != want.W || got.H != want.H || got.ComputeMs != want.ComputeMs || !bytes.Equal(got.RGBA, want.RGBA) {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	})
}

func TestStaleJobIDIsPreservedForServerCheck(t *testing.T) {
	frame, err := EncodeResult(Result{JobID: 11, TaskID: 3, Attempt: 1, W: 1, H: 1, RGBA: []byte{1, 2, 3, 4}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := DecodeResult(frame)
	if err != nil {
		t.Fatal(err)
	}
	if result.JobID == 12 {
		t.Fatal("stale jobId unexpectedly matched current job")
	}
}

func TestMalformedBinaryFrames(t *testing.T) {
	validTask, _ := EncodeTask(Task{JobID: 1, TaskID: 1, Attempt: 1, W: 1, H: 1, X0: -2, Scale: 1, MaxIter: 10})
	validResult, _ := EncodeResult(Result{JobID: 1, TaskID: 1, Attempt: 1, W: 1, H: 1, ComputeMs: 0, RGBA: []byte{1, 2, 3, 4}})
	mutate := func(src []byte, fn func([]byte)) []byte { out := append([]byte(nil), src...); fn(out); return out }
	tests := []struct {
		name   string
		frame  []byte
		result bool
	}{
		{"truncated task", validTask[:47], false},
		{"oversized task width", mutate(validTask, func(b []byte) { binary.LittleEndian.PutUint16(b[16:18], MaxTileDim+1) }), false},
		{"zero task width", mutate(validTask, func(b []byte) { binary.LittleEndian.PutUint16(b[16:18], 0) }), false},
		{"zero task height", mutate(validTask, func(b []byte) { binary.LittleEndian.PutUint16(b[18:20], 0) }), false},
		{"truncated result header", validResult[:19], true},
		{"truncated result payload", validResult[:len(validResult)-1], true},
		{"extra result payload", append(append([]byte(nil), validResult...), 0), true},
		{"oversized result width", mutate(validResult, func(b []byte) { binary.LittleEndian.PutUint16(b[12:14], MaxTileDim+1) }), true},
		{"oversized result height", mutate(validResult, func(b []byte) { binary.LittleEndian.PutUint16(b[14:16], MaxTileDim+1) }), true},
		{"zero result width", mutate(validResult, func(b []byte) { binary.LittleEndian.PutUint16(b[12:14], 0) }), true},
		{"zero result height", mutate(validResult, func(b []byte) { binary.LittleEndian.PutUint16(b[14:16], 0) }), true},
		{"nonfinite computeMs", mutate(validResult, func(b []byte) { binary.LittleEndian.PutUint32(b[16:20], math.Float32bits(float32(math.NaN()))) }), true},
		{"negative computeMs", mutate(validResult, func(b []byte) { binary.LittleEndian.PutUint32(b[16:20], math.Float32bits(-1)) }), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			if tt.result {
				_, err = DecodeResult(tt.frame)
			} else {
				_, err = DecodeTask(tt.frame)
			}
			if err == nil {
				t.Fatal("decoder accepted malformed frame")
			}
		})
	}
}

func TestEncodeRejectsInvalidValues(t *testing.T) {
	if _, err := EncodeTask(Task{Attempt: 1, W: 1, H: 1, Scale: math.Inf(1), MaxIter: 1}); err == nil {
		t.Fatal("EncodeTask accepted infinity")
	}
	if _, err := EncodeResult(Result{Attempt: 1, W: 1, H: 1, ComputeMs: float32(math.Inf(1)), RGBA: []byte{1, 2, 3, 4}}); err == nil {
		t.Fatal("EncodeResult accepted infinite computeMs")
	}
	if _, err := EncodeResult(Result{Attempt: 1, W: 1, H: 1, ComputeMs: -0.1, RGBA: []byte{1, 2, 3, 4}}); err == nil {
		t.Fatal("EncodeResult accepted negative computeMs")
	}
	if _, err := EncodeResult(Result{Attempt: 1, W: 1, H: 1, RGBA: []byte{1, 2, 3}}); err == nil {
		t.Fatal("EncodeResult accepted wrong pixel length")
	}
	if _, err := EncodeJSON(Message{Type: MessageBenchResult, TilesPerSec: 1, PerThread: 1, Threads: 1, Hash: strings.Repeat("a", 64)}); err != nil {
		t.Fatalf("valid bench message: %v", err)
	}
}

func TestJSONFormat(t *testing.T) {
	data, err := EncodeJSON(Message{Type: MessageHello, DeviceID: "d", UserAgent: "ua", Cores: 4, Threads: 1})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if mock, ok := got["mock"]; !ok || mock != false {
		t.Fatalf("HELLO mock must be explicitly false by default: %s", data)
	}
}

func TestCanvasAndBenchmarkConstants(t *testing.T) {
	if CanvasSize != 2048 || TileSize != 64 {
		t.Fatalf("canvas/tile constants = %d/%d", CanvasSize, TileSize)
	}
	tile := BenchTile()
	if tile.X0 != -0.75 || tile.Y0 != 0.1 || tile.Scale != 0.005 || tile.W != 64 || tile.H != 64 || tile.MaxIter != 256 {
		t.Fatalf("BenchTile() = %#v, want fixed documented benchmark parameters", tile)
	}
}
