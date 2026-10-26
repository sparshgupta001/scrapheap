package metrics

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

type deviceInternal struct {
	id          string
	label       string
	threads     int
	tilesDone   int
	state       string // "active" | "idle" | "gone"
	completions []time.Time
}

type tileInternal struct {
	deviceID  string
	computeMs uint32
	attempt   uint32
	attempts  int
	rgba      []byte
}

// Collector collects cluster metrics and device states in a thread-safe manner.
type Collector struct {
	mu sync.RWMutex

	jobID             string
	jobState          string // "idle" | "running" | "done"
	startTime         time.Time
	endTime           time.Time
	totalTiles        int
	doneCount         int
	retries           int
	dupesDropped      int
	verifiedOK        int
	verifiedFail      int
	tileStates        []byte
	tiles             []tileInternal
	assignedDevices   map[int]string
	devices           map[string]*deviceInternal
	deviceOrder       []string
	runDevices        map[string]struct{}
	globalCompletions []time.Time
	window            time.Duration

	// Optional run tracking for CSV
	baseline      bool
	schedulerMode string
	bytesIn       int64
	bytesOut      int64
	runs          []RunRecord

	listeners []func(any)
	nowFunc   func() time.Time
}

// NewCollector creates an initialized Collector.
func NewCollector() *Collector {
	return &Collector{
		jobState:        "idle",
		assignedDevices: make(map[int]string),
		devices:         make(map[string]*deviceInternal),
		runDevices:      make(map[string]struct{}),
		schedulerMode:   "pull",
		window:          3 * time.Second,
		nowFunc:         time.Now,
	}
}

// SetNowFunc overrides the time source (useful for testing).
func (c *Collector) SetNowFunc(f func() time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nowFunc = f
}

// SetWindow sets the rolling window duration for rate calculation.
func (c *Collector) SetWindow(w time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.window = w
}

// SetConfig marks whether the next run is the laptop-only baseline.
func (c *Collector) SetConfig(cfg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.baseline = cfg == "B"
}

// SetScheduler selects the scheduler mode used in subsequent CSV run records.
func (c *Collector) SetScheduler(mode string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if mode == "aware" {
		c.schedulerMode = "aware"
		return
	}
	c.schedulerMode = "pull"
}

// RecordBytes tracks network I/O for the current run.
func (c *Collector) RecordBytes(bytesIn, bytesOut int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bytesIn += bytesIn
	c.bytesOut += bytesOut
}

// OnEvent registers a listener callback for broadcast events.
func (c *Collector) OnEvent(listener func(any)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listeners = append(c.listeners, listener)
}

func (c *Collector) emit(event any) {
	c.mu.RLock()
	listeners := make([]func(any), len(c.listeners))
	copy(listeners, c.listeners)
	c.mu.RUnlock()

	for _, l := range listeners {
		l(event)
	}
}

func (c *Collector) now() time.Time {
	if c.nowFunc != nil {
		return c.nowFunc()
	}
	return time.Now()
}

func (c *Collector) elapsedLocked() float64 {
	if c.startTime.IsZero() {
		return 0.0
	}
	var end time.Time
	if !c.endTime.IsZero() && c.jobState == "done" {
		end = c.endTime
	} else {
		end = c.now()
	}
	sec := end.Sub(c.startTime).Seconds()
	if sec < 0 {
		return 0.0
	}
	return math.Round(sec*10) / 10
}

func (c *Collector) calcRateLocked(completions []time.Time, now time.Time) float64 {
	if len(completions) == 0 {
		return 0.0
	}
	cutoff := now.Add(-c.window)
	count := 0
	for _, t := range completions {
		if !t.Before(cutoff) && !t.After(now) {
			count++
		}
	}
	if count == 0 {
		return 0.0
	}
	span := c.window.Seconds()
	if !c.startTime.IsZero() {
		elapsed := now.Sub(c.startTime).Seconds()
		if elapsed < span && elapsed > 0.1 {
			span = elapsed
		}
	}
	if span <= 0 {
		return 0.0
	}
	rate := float64(count) / span
	return math.Round(rate*10) / 10
}

// ShortLabel returns label if non-empty, otherwise parses userAgent into a concise device name.
func ShortLabel(label, userAgent string) string {
	trimmed := strings.TrimSpace(label)
	if trimmed != "" {
		return trimmed
	}
	ua := userAgent
	lower := strings.ToLower(ua)
	if strings.Contains(lower, "iphone") {
		if strings.Contains(lower, "crios") {
			return "iPhone Chrome"
		}
		if strings.Contains(lower, "safari") {
			return "iPhone Safari"
		}
		return "iPhone"
	}
	if strings.Contains(lower, "ipad") {
		if strings.Contains(lower, "safari") {
			return "iPad Safari"
		}
		return "iPad"
	}
	if strings.Contains(lower, "android") {
		if strings.Contains(lower, "firefox") {
			return "Android Firefox"
		}
		if strings.Contains(lower, "chrome") {
			return "Android Chrome"
		}
		return "Android Device"
	}
	if strings.Contains(lower, "windows") {
		if strings.Contains(lower, "edg") {
			return "Windows Edge"
		}
		if strings.Contains(lower, "chrome") {
			return "Windows Chrome"
		}
		if strings.Contains(lower, "firefox") {
			return "Windows Firefox"
		}
		return "Windows PC"
	}
	if strings.Contains(lower, "macintosh") || strings.Contains(lower, "mac os") {
		if strings.Contains(lower, "chrome") {
			return "Mac Chrome"
		}
		if strings.Contains(lower, "safari") {
			return "Mac Safari"
		}
		if strings.Contains(lower, "firefox") {
			return "Mac Firefox"
		}
		return "Mac"
	}
	if strings.Contains(lower, "linux") {
		if strings.Contains(lower, "chrome") {
			return "Linux Chrome"
		}
		if strings.Contains(lower, "firefox") {
			return "Linux Firefox"
		}
		return "Linux PC"
	}
	return "Device"
}

// DeviceJoined registers a new device or marks a reconnecting device as active.
func (c *Collector) DeviceJoined(id, label string, threads int) {
	c.mu.Lock()
	if threads <= 0 {
		threads = 1
	}
	dev, exists := c.devices[id]
	if !exists {
		dev = &deviceInternal{
			id:      id,
			label:   label,
			threads: threads,
			state:   "active",
		}
		c.devices[id] = dev
		c.deviceOrder = append(c.deviceOrder, id)
	} else {
		// Reconnect case: reuse device, preserve tilesDone, set state active
		dev.state = "active"
		if threads > 0 {
			dev.threads = threads
		}
		if label != "" {
			dev.label = label
		}
	}
	lbl := dev.label
	if c.jobState == "running" {
		c.runDevices[id] = struct{}{}
	}
	t := c.elapsedLocked()
	c.mu.Unlock()

	c.emit(DeviceJoinedEvent{
		Type:  "device_joined",
		ID:    id,
		Label: lbl,
		T:     t,
	})
}

// DeviceLeft marks a device as "gone".
func (c *Collector) DeviceLeft(id string) {
	c.mu.Lock()
	if dev, exists := c.devices[id]; exists {
		dev.state = "gone"
	}
	t := c.elapsedLocked()
	c.mu.Unlock()

	c.emit(DeviceLeftEvent{
		Type: "device_left",
		ID:   id,
		T:    t,
	})
}

// TaskAssigned marks a task as leased ("L") and tracks its device assignment.
func (c *Collector) TaskAssigned(taskID int, deviceID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if taskID >= 0 && taskID < len(c.tileStates) {
		if c.tileStates[taskID] != 'D' {
			c.tileStates[taskID] = 'L'
			c.tiles[taskID].attempts++
		}
	}
	c.assignedDevices[taskID] = deviceID
}

// TaskAccepted records a result, retaining pixels only for correctly sized verified data.
func (c *Collector) TaskAccepted(taskID int, deviceID string, verifiedOK bool, computeMs uint32, attempt uint32, rgba []byte) {
	c.mu.Lock()
	if taskID < 0 || taskID >= len(c.tileStates) {
		c.mu.Unlock()
		return
	}
	if c.tileStates[taskID] == 'D' {
		c.dupesDropped++
		c.mu.Unlock()
		return
	}
	now := c.now()
	c.tileStates[taskID] = 'D'
	c.doneCount++
	tile := &c.tiles[taskID]
	tile.deviceID = deviceID
	tile.computeMs = computeMs
	tile.attempt = attempt
	if tile.attempts < int(attempt) {
		tile.attempts = int(attempt)
	}
	tile.rgba = nil
	if verifiedOK && len(rgba) == tileRGBABytes {
		tile.rgba = append([]byte(nil), rgba...)
	}

	if verifiedOK {
		c.verifiedOK++
	} else {
		c.verifiedFail++
	}

	c.globalCompletions = append(c.globalCompletions, now)

	if dev, exists := c.devices[deviceID]; exists {
		dev.tilesDone++
		dev.completions = append(dev.completions, now)
	}
	t := c.elapsedLocked()
	c.mu.Unlock()

	c.emit(TileDoneEvent{
		Type:      "tile_done",
		Task:      taskID,
		Device:    deviceID,
		ComputeMs: computeMs,
		Attempt:   attempt,
		T:         t,
	})
}

// TaskRequeued marks a task as requeued ("R") and emits a tile_requeued event.
func (c *Collector) TaskRequeued(taskID int, reason string) {
	c.mu.Lock()
	if taskID >= 0 && taskID < len(c.tileStates) {
		if c.tileStates[taskID] != 'D' {
			c.tileStates[taskID] = 'R'
		}
	}
	c.retries++
	t := c.elapsedLocked()
	c.mu.Unlock()

	c.emit(TileRequeuedEvent{
		Type:   "tile_requeued",
		Task:   taskID,
		Reason: reason,
		T:      t,
	})
}

// DupDropped increments the dropped duplicates counter.
func (c *Collector) DupDropped(taskID int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dupesDropped++
}

// JobStart begins a new compute job and resets per-job tile counters.
func (c *Collector) JobStart(jobID string, totalTiles int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.jobID = jobID
	c.jobState = "running"
	c.startTime = c.now()
	c.endTime = time.Time{}
	c.totalTiles = totalTiles
	c.doneCount = 0
	c.retries = 0
	c.dupesDropped = 0
	c.verifiedOK = 0
	c.verifiedFail = 0
	c.globalCompletions = nil

	c.tileStates = make([]byte, totalTiles)
	c.tiles = make([]tileInternal, totalTiles)
	for i := range c.tileStates {
		c.tileStates[i] = 'Q'
	}
	c.assignedDevices = make(map[int]string)
	c.runDevices = make(map[string]struct{})

	for _, dev := range c.devices {
		dev.tilesDone = 0
		dev.completions = nil
		if dev.state != "gone" {
			dev.state = "active"
		}
	}
}

// JobDone marks the job as completed, archives a RunRecord for CSV, and emits a job_done event.
func (c *Collector) JobDone(finalChecksum string) {
	c.mu.Lock()
	c.jobState = "done"
	c.endTime = c.now()
	t := c.elapsedLocked()

	wallS := c.endTime.Sub(c.startTime).Seconds()
	if wallS <= 0 {
		wallS = t
	}
	totalDevices := len(c.runDevices)
	totalThreads := 0
	for id := range c.runDevices {
		if device := c.devices[id]; device != nil {
			totalThreads += device.threads
		}
	}
	tiles := c.verifiedOK + c.verifiedFail
	tilesPerSec := 0.0
	if wallS > 0 {
		tilesPerSec = float64(tiles) / wallS
	}

	config := fmt.Sprintf("%s+%d", c.schedulerMode, max(len(c.runDevices)-1, 0))
	if c.baseline {
		config = "B"
	}
	record := RunRecord{
		RunID:        c.jobID,
		Config:       config,
		Devices:      totalDevices,
		Threads:      totalThreads,
		WallS:        wallS,
		ComputeS:     wallS * float64(totalThreads),
		Tiles:        tiles,
		TilesPerSec:  tilesPerSec,
		Retries:      c.retries,
		DupesDropped: c.dupesDropped,
		VerifiedOK:   c.verifiedOK,
		VerifiedFail: c.verifiedFail,
		BytesIn:      c.bytesIn,
		BytesOut:     c.bytesOut,
	}

	c.runs = append(c.runs, record)
	c.mu.Unlock()

	c.emit(JobDoneEvent{
		Type:          "job_done",
		T:             t,
		FinalChecksum: finalChecksum,
	})
}

const tileRGBABytes = 64 * 64 * 4

// TileRGBA returns a copy of a completed tile's verified RGBA pixels.
func (c *Collector) TileRGBA(taskID int) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if taskID < 0 || taskID >= len(c.tileStates) || c.tileStates[taskID] != 'D' ||
		len(c.tiles[taskID].rgba) != tileRGBABytes {
		return nil, false
	}
	return append([]byte(nil), c.tiles[taskID].rgba...), true
}

// TilesInfo returns metadata for completed tiles in task order.
func (c *Collector) TilesInfo() []TileInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]TileInfo, 0, c.doneCount)
	for task, state := range c.tileStates {
		if state != 'D' {
			continue
		}
		tile := c.tiles[task]
		result = append(result, TileInfo{
			Task:      task,
			Device:    tile.deviceID,
			ComputeMs: tile.computeMs,
			Attempts:  tile.attempts,
		})
	}
	return result
}

// Snapshot generates the current system snapshot.
func (c *Collector) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	t := c.elapsedLocked()
	tilesPerSec := c.calcRateLocked(c.globalCompletions, now)

	devSnapshots := make([]DeviceSnapshot, 0, len(c.devices))
	for _, id := range c.deviceOrder {
		dev := c.devices[id]
		devRate := c.calcRateLocked(dev.completions, now)
		devSnapshots = append(devSnapshots, DeviceSnapshot{
			ID:          dev.id,
			Label:       dev.label,
			Threads:     dev.threads,
			TilesDone:   dev.tilesDone,
			TilesPerSec: devRate,
			State:       dev.state,
		})
	}

	// Sort devices descending by tilesDone (stable)
	sort.SliceStable(devSnapshots, func(i, j int) bool {
		return devSnapshots[i].TilesDone > devSnapshots[j].TilesDone
	})

	tileStatesStr := ""
	if c.tileStates != nil {
		tileStatesStr = string(c.tileStates)
	}

	return Snapshot{
		Type:         "snapshot",
		T:            t,
		Job:          c.jobState,
		JobID:        c.jobID,
		Done:         c.doneCount,
		Total:        c.totalTiles,
		TilesPerSec:  tilesPerSec,
		Devices:      devSnapshots,
		Retries:      c.retries,
		DupesDropped: c.dupesDropped,
		VerifiedOK:   c.verifiedOK,
		VerifiedFail: c.verifiedFail,
		TileStates:   tileStatesStr,
	}
}

// AddRun manually adds a RunRecord (useful for tests or external injectors).
func (c *Collector) AddRun(r RunRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.runs = append(c.runs, r)
}

// GetRuns returns a copy of all completed runs.
func (c *Collector) GetRuns() []RunRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res := make([]RunRecord, len(c.runs))
	copy(res, c.runs)
	return res
}
