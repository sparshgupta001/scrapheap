package metrics

// DeviceSnapshot represents the state of a single compute device.
type DeviceSnapshot struct {
	ID          string  `json:"id"`
	Label       string  `json:"label"`
	Threads     int     `json:"threads"`
	TilesDone   int     `json:"tilesDone"`
	TilesPerSec float64 `json:"tilesPerSec"`
	State       string  `json:"state"` // "active" | "idle" | "gone"
}

// Snapshot represents the complete system snapshot pushed over WebSocket every 500 ms.
type Snapshot struct {
	Type         string           `json:"type"` // "snapshot"
	T            float64          `json:"t"`    // seconds since job start
	Job          string           `json:"job"`  // "idle" | "running" | "done"
	JobID        string           `json:"jobId"`
	Done         int              `json:"done"`
	Total        int              `json:"total"`
	TilesPerSec  float64          `json:"tilesPerSec"`
	Devices      []DeviceSnapshot `json:"devices"`
	Retries      int              `json:"retries"`
	DupesDropped int              `json:"dupesDropped"`
	VerifiedOK   int              `json:"verifiedOK"`
	VerifiedFail int              `json:"verifiedFail"`
	TileStates   string           `json:"tileStates"` // 1 char per tile: Q, L, D, R
}

// DeviceJoinedEvent is emitted when a worker device joins.
type DeviceJoinedEvent struct {
	Type  string  `json:"type"` // "device_joined"
	ID    string  `json:"id"`
	Label string  `json:"label"`
	T     float64 `json:"t"`
}

// DeviceLeftEvent is emitted when a worker device leaves.
type DeviceLeftEvent struct {
	Type string  `json:"type"` // "device_left"
	ID   string  `json:"id"`
	T    float64 `json:"t"`
}

// TileRequeuedEvent is emitted when a tile is requeued.
type TileRequeuedEvent struct {
	Type   string  `json:"type"` // "tile_requeued"
	Task   int     `json:"task"`
	Reason string  `json:"reason"` // "disconnect" | "lease"
	T      float64 `json:"t"`
}

// JobDoneEvent is emitted when a job finishes.
type JobDoneEvent struct {
	Type          string  `json:"type"` // "job_done"
	T             float64 `json:"t"`
	FinalChecksum string  `json:"finalChecksum"`
}
