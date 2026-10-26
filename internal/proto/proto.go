package proto

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
)

// MessageType identifies a JSON control message.
type MessageType string

const (
	MessageHello       MessageType = "HELLO"
	MessageBenchResult MessageType = "BENCH_RESULT"
	MessageRequestTask MessageType = "REQUEST_TASK"
	MessageHeartbeat   MessageType = "HEARTBEAT"
	MessageAck         MessageType = "ACK"
	MessageJobDone     MessageType = "JOB_DONE"
	MessageWelcome     MessageType = "WELCOME"
	MessageJobStarted  MessageType = "JOB_STARTED"
)

// Message is the JSON control-message envelope. Fields not used by Type are omitted.
type Message struct {
	Type                MessageType `json:"type"`
	DeviceID            string      `json:"deviceId,omitempty"`
	UserAgent           string      `json:"userAgent,omitempty"`
	Cores               uint32      `json:"cores,omitempty"`
	Threads             int         `json:"threads,omitempty"`
	Mock                bool        `json:"mock,omitempty"`
	TilesPerSec         float64     `json:"tilesPerSec,omitempty"`
	PerThread           float64     `json:"perThread,omitempty"`
	Hash                string      `json:"hash,omitempty"`
	TaskID              uint32      `json:"taskId,omitempty"`
	Attempt             uint16      `json:"attempt,omitempty"`
	JobID               uint32      `json:"jobId,omitempty"`
	HeartbeatIntervalMs uint32      `json:"heartbeatIntervalMs,omitempty"`
	HeartbeatTimeoutMs  uint32      `json:"heartbeatTimeoutMs,omitempty"`
}

// Task contains render parameters transported in a TASK binary frame.
type Task struct {
	JobID   uint32
	TaskID  uint32
	Attempt uint16
	X       uint16
	Y       uint16
	W       uint16
	H       uint16
	X0      float64
	Y0      float64
	Scale   float64
	MaxIter uint32
}

// Result contains a RESULT header and tightly packed row-major RGBA bytes.
type Result struct {
	JobID     uint32
	TaskID    uint32
	Attempt   uint16
	W         uint16
	H         uint16
	ComputeMs float32
	RGBA      []byte
}

const (
	FrameTask   byte = 0x01
	FrameResult byte = 0x02

	CanvasSize       = 2048
	TileSize         = 64
	MaxTileDim       = 256
	TaskHeaderSize   = 48
	ResultHeaderSize = 20

	BenchTileW       uint16 = 64
	BenchTileH       uint16 = 64
	BenchTileMaxIter uint32 = 256
	BenchTileX0             = -0.75
	BenchTileY0             = 0.1
	BenchTileScale          = 0.005
)

var (
	ErrMalformedFrame = errors.New("malformed binary frame")
	ErrInvalidMessage = errors.New("invalid control message")
)

// UnknownMessageTypeError identifies an unsupported JSON control-message type.
type UnknownMessageTypeError struct{ Type string }

func (e *UnknownMessageTypeError) Error() string {
	return fmt.Sprintf("unknown control message type %q", e.Type)
}

// UnknownFrameTypeError identifies an unsupported binary frame discriminator.
type UnknownFrameTypeError struct{ Type byte }

func (e *UnknownFrameTypeError) Error() string {
	return fmt.Sprintf("unknown binary frame type 0x%02x", e.Type)
}

// BenchTile returns the fixed render parameters used to benchmark kernel speed and output.
func BenchTile() Task {
	return Task{X0: BenchTileX0, Y0: BenchTileY0, Scale: BenchTileScale, W: BenchTileW, H: BenchTileH, MaxIter: BenchTileMaxIter}
}

// EncodeJSON validates and encodes one JSON control message.
func EncodeJSON(msg Message) ([]byte, error) {
	fields := messageValues(msg)
	if err := validateMessageFields(msg, rawFields(fields)); err != nil {
		return nil, err
	}
	data, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("encode control message: %w", err)
	}
	return data, nil
}

// DecodeJSON decodes exactly one JSON control message and rejects unknown,
// missing, or type-inappropriate fields.
func DecodeJSON(data []byte) (Message, error) {
	var msg Message
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&msg); err != nil {
		return Message{}, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return Message{}, fmt.Errorf("%w: trailing JSON data", ErrInvalidMessage)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return Message{}, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	if err := validateMessageFields(msg, fields); err != nil {
		return Message{}, err
	}
	return msg, nil
}

func messageValues(msg Message) map[string]any {
	fields := map[string]any{"type": msg.Type}
	switch msg.Type {
	case MessageHello:
		fields["deviceId"], fields["userAgent"], fields["cores"], fields["threads"], fields["mock"] = msg.DeviceID, msg.UserAgent, msg.Cores, msg.Threads, msg.Mock
	case MessageBenchResult:
		fields["tilesPerSec"], fields["perThread"], fields["threads"], fields["hash"] = msg.TilesPerSec, msg.PerThread, msg.Threads, msg.Hash
	case MessageAck:
		fields["taskId"], fields["attempt"], fields["jobId"] = msg.TaskID, msg.Attempt, msg.JobID
	case MessageJobDone:
		fields["jobId"] = msg.JobID
	case MessageWelcome:
		fields["heartbeatIntervalMs"], fields["heartbeatTimeoutMs"] = msg.HeartbeatIntervalMs, msg.HeartbeatTimeoutMs
	case MessageJobStarted:
		fields["jobId"] = msg.JobID
	}
	return fields
}

func rawFields(values map[string]any) map[string]json.RawMessage {
	encoded, _ := json.Marshal(values) // Values are limited to JSON scalar types.
	fields := make(map[string]json.RawMessage)
	_ = json.Unmarshal(encoded, &fields)
	return fields
}

func validateMessageFields(msg Message, fields map[string]json.RawMessage) error {
	require := func(name string) error {
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("%w: missing %q", ErrInvalidMessage, name)
		}
		return nil
	}
	switch msg.Type {
	case MessageHello:
		for _, name := range []string{"deviceId", "userAgent", "cores", "threads"} {
			if err := require(name); err != nil {
				return err
			}
		}
		if msg.DeviceID == "" || msg.Cores == 0 || msg.Threads < 1 || msg.Threads > 16 {
			return fmt.Errorf("%w: HELLO requires deviceId, cores > 0, and threads in 1..16", ErrInvalidMessage)
		}
	case MessageBenchResult:
		for _, name := range []string{"tilesPerSec", "perThread", "threads", "hash"} {
			if err := require(name); err != nil {
				return err
			}
		}
		if msg.TilesPerSec <= 0 || !finite(msg.TilesPerSec) || msg.PerThread <= 0 || !finite(msg.PerThread) {
			return fmt.Errorf("%w: tilesPerSec and perThread must be finite and > 0", ErrInvalidMessage)
		}
		if msg.Threads < 1 || msg.Threads > 16 {
			return fmt.Errorf("%w: threads must be in 1..16", ErrInvalidMessage)
		}
		if len(msg.Hash) != 64 {
			return fmt.Errorf("%w: hash must be a 64-character SHA-256 hex digest", ErrInvalidMessage)
		}
		if _, err := hex.DecodeString(msg.Hash); err != nil {
			return fmt.Errorf("%w: hash must be hexadecimal: %v", ErrInvalidMessage, err)
		}
	case MessageRequestTask, MessageHeartbeat, MessageJobDone:
		if msg.Type == MessageJobDone {
			if err := require("jobId"); err != nil {
				return err
			}
		}
	case MessageAck:
		for _, name := range []string{"taskId", "attempt", "jobId"} {
			if err := require(name); err != nil {
				return err
			}
		}
		if msg.Attempt == 0 {
			return fmt.Errorf("%w: ACK attempt must be > 0", ErrInvalidMessage)
		}
	case MessageWelcome:
		for _, name := range []string{"heartbeatIntervalMs", "heartbeatTimeoutMs"} {
			if err := require(name); err != nil {
				return err
			}
		}
		if msg.HeartbeatIntervalMs == 0 || msg.HeartbeatTimeoutMs == 0 {
			return fmt.Errorf("%w: WELCOME heartbeat intervals must be > 0", ErrInvalidMessage)
		}
	case MessageJobStarted:
		if err := require("jobId"); err != nil {
			return err
		}
	default:
		return &UnknownMessageTypeError{Type: string(msg.Type)}
	}
	return nil
}

// EncodeTask encodes a TASK frame using the fixed 48-byte little-endian header.
func EncodeTask(task Task) ([]byte, error) {
	if !validTask(task) {
		return nil, fmt.Errorf("%w: invalid TASK values", ErrMalformedFrame)
	}
	out := make([]byte, TaskHeaderSize)
	out[0] = FrameTask
	out[1] = 0
	binary.LittleEndian.PutUint16(out[2:4], task.Attempt)
	binary.LittleEndian.PutUint32(out[4:8], task.JobID)
	binary.LittleEndian.PutUint32(out[8:12], task.TaskID)
	binary.LittleEndian.PutUint16(out[12:14], task.X)
	binary.LittleEndian.PutUint16(out[14:16], task.Y)
	binary.LittleEndian.PutUint16(out[16:18], task.W)
	binary.LittleEndian.PutUint16(out[18:20], task.H)
	binary.LittleEndian.PutUint64(out[20:28], math.Float64bits(task.X0))
	binary.LittleEndian.PutUint64(out[28:36], math.Float64bits(task.Y0))
	binary.LittleEndian.PutUint64(out[36:44], math.Float64bits(task.Scale))
	binary.LittleEndian.PutUint32(out[44:48], task.MaxIter)
	return out, nil
}

// DecodeTask validates and decodes one complete TASK frame.
func DecodeTask(data []byte) (Task, error) {
	if len(data) == 0 {
		return Task{}, fmt.Errorf("%w: TASK frame is empty", ErrMalformedFrame)
	}
	if data[0] != FrameTask && data[0] != FrameResult {
		return Task{}, &UnknownFrameTypeError{Type: data[0]}
	}
	if len(data) != TaskHeaderSize || data[0] != FrameTask {
		return Task{}, fmt.Errorf("%w: TASK must be exactly %d bytes", ErrMalformedFrame, TaskHeaderSize)
	}
	task := Task{
		Attempt: binary.LittleEndian.Uint16(data[2:4]),
		JobID:   binary.LittleEndian.Uint32(data[4:8]),
		TaskID:  binary.LittleEndian.Uint32(data[8:12]),
		X:       binary.LittleEndian.Uint16(data[12:14]), Y: binary.LittleEndian.Uint16(data[14:16]),
		W: binary.LittleEndian.Uint16(data[16:18]), H: binary.LittleEndian.Uint16(data[18:20]),
		X0:      math.Float64frombits(binary.LittleEndian.Uint64(data[20:28])),
		Y0:      math.Float64frombits(binary.LittleEndian.Uint64(data[28:36])),
		Scale:   math.Float64frombits(binary.LittleEndian.Uint64(data[36:44])),
		MaxIter: binary.LittleEndian.Uint32(data[44:48]),
	}
	if !validTask(task) {
		return Task{}, fmt.Errorf("%w: invalid TASK values", ErrMalformedFrame)
	}
	return task, nil
}

func validTask(task Task) bool {
	return validTileDims(task.W, task.H) && task.Attempt != 0 && task.MaxIter != 0 &&
		finite(task.X0) && finite(task.Y0) && finite(task.Scale) && task.Scale > 0
}

// EncodeResult encodes a RESULT frame and requires exactly W*H*4 pixel bytes.
func EncodeResult(result Result) ([]byte, error) {
	payloadSize, ok := rgbaSize(result.W, result.H)
	if !ok || result.Attempt == 0 || !finite32(result.ComputeMs) || result.ComputeMs < 0 || uint64(len(result.RGBA)) != payloadSize {
		return nil, fmt.Errorf("%w: invalid RESULT dimensions, attempt, computeMs, or RGBA length", ErrMalformedFrame)
	}
	out := make([]byte, ResultHeaderSize+len(result.RGBA))
	out[0] = FrameResult
	out[1] = 0
	binary.LittleEndian.PutUint16(out[2:4], result.Attempt)
	binary.LittleEndian.PutUint32(out[4:8], result.JobID)
	binary.LittleEndian.PutUint32(out[8:12], result.TaskID)
	binary.LittleEndian.PutUint16(out[12:14], result.W)
	binary.LittleEndian.PutUint16(out[14:16], result.H)
	binary.LittleEndian.PutUint32(out[16:20], math.Float32bits(result.ComputeMs))
	copy(out[ResultHeaderSize:], result.RGBA)
	return out, nil
}

// DecodeResult validates and decodes one complete RESULT frame.
func DecodeResult(data []byte) (Result, error) {
	if len(data) == 0 {
		return Result{}, fmt.Errorf("%w: RESULT frame is empty", ErrMalformedFrame)
	}
	if data[0] != FrameTask && data[0] != FrameResult {
		return Result{}, &UnknownFrameTypeError{Type: data[0]}
	}
	if len(data) < ResultHeaderSize || data[0] != FrameResult {
		return Result{}, fmt.Errorf("%w: RESULT header is incomplete or has wrong type", ErrMalformedFrame)
	}
	result := Result{
		Attempt: binary.LittleEndian.Uint16(data[2:4]),
		JobID:   binary.LittleEndian.Uint32(data[4:8]),
		TaskID:  binary.LittleEndian.Uint32(data[8:12]),
		W:       binary.LittleEndian.Uint16(data[12:14]), H: binary.LittleEndian.Uint16(data[14:16]),
		ComputeMs: math.Float32frombits(binary.LittleEndian.Uint32(data[16:20])),
	}
	payloadSize, ok := rgbaSize(result.W, result.H)
	if !ok || result.Attempt == 0 || !finite32(result.ComputeMs) || result.ComputeMs < 0 || payloadSize != uint64(len(data)-ResultHeaderSize) {
		return Result{}, fmt.Errorf("%w: invalid RESULT values or payload length", ErrMalformedFrame)
	}
	result.RGBA = append([]byte(nil), data[ResultHeaderSize:]...)
	return result, nil
}

func rgbaSize(w, h uint16) (uint64, bool) {
	if !validTileDims(w, h) {
		return 0, false
	}
	width, height := uint64(w), uint64(h)
	if width > math.MaxUint64/height/4 {
		return 0, false
	}
	return width * height * 4, true
}

func validTileDims(w, h uint16) bool { return w > 0 && h > 0 && w <= MaxTileDim && h <= MaxTileDim }

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func finite32(v float32) bool { return !math.IsNaN(float64(v)) && !math.IsInf(float64(v), 0) }
