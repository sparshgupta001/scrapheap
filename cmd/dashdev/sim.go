package main

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scrap-heap/backend/internal/metrics"
)

const (
	simulationTileCount = 1024

	laptopRate = 8.0
	pixelRate  = 6.0
	iphoneRate = 6.0
	ipadRate   = 4.0

	pixelJoinAt  = 10.0
	iphoneJoinAt = 18.0
	ipadJoinAt   = 24.0
	pixelLeaveAt = 35.0

	tileLatency = 0.75
	jitterMin   = 0.85
	jitterMax   = 1.15
)

// Rates: 0-10s = 80 tiles; 10-18s = 112; 18-24s = 120; 24-35s = 264,
// totaling 576. After Pixel leaves, 448 remain at 18 tiles/s (~24.9s),
// so expected completion is about t=60s, plus a few requeued Pixel tiles.

type devicePlan struct {
	id       string
	label    string
	threads  int
	rate     float64
	joinAt   float64
	leaveAt  float64
	hasLeave bool
}

var scriptedDevices = []devicePlan{
	{id: "laptop", label: "Laptop", threads: 6, rate: laptopRate},
	{id: "pixel-7", label: "Pixel 7", threads: 8, rate: pixelRate, joinAt: pixelJoinAt, leaveAt: pixelLeaveAt, hasLeave: true},
	{id: "iphone-14", label: "iPhone 14", threads: 6, rate: iphoneRate, joinAt: iphoneJoinAt},
	{id: "ipad", label: "iPad", threads: 4, rate: ipadRate, joinAt: ipadJoinAt},
}

type scheduledTile struct {
	deviceID string
	finishAt float64
}

type simulator struct {
	collector *metrics.Collector
	speed     float64
	nextRun   atomic.Uint64

	mu     sync.Mutex
	cancel context.CancelFunc
}

func (s *simulator) start(baseline bool) {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.mu.Unlock()

	runNumber := s.nextRun.Add(1)
	config := "A"
	if baseline {
		config = "B"
	}
	go s.run(ctx, runNumber, config, baseline)
}

func (s *simulator) close() {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()
}

func (s *simulator) run(ctx context.Context, runNumber uint64, config string, baseline bool) {
	jobID := "run-" + itoa64(runNumber)
	s.collector.SetConfig(config)
	s.collector.JobStart(jobID, simulationTileCount)

	plans := scriptedDevices
	if baseline {
		plans = scriptedDevices[:1]
	}

	states := make([]byte, simulationTileCount)
	for i := range states {
		states[i] = 'Q'
	}
	assigned := make(map[int]string)
	pending := make(map[int]scheduledTile)
	joined := make(map[string]bool)
	left := make(map[string]bool)
	nextAssign := make(map[string]float64)
	random := rand.New(rand.NewSource(int64(runNumber)))
	started := time.Now()

	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		now := time.Since(started).Seconds() * s.speed
		if now < 0 {
			now = 0
		}

		for _, device := range plans {
			if !joined[device.id] && now >= device.joinAt {
				joined[device.id] = true
				s.collector.DeviceJoined(device.id, device.label, device.threads)
				nextAssign[device.id] = now
			}
			if device.hasLeave && !left[device.id] && now >= device.leaveAt {
				left[device.id] = true
				s.collector.DeviceLeft(device.id)
				for _, task := range requeueDeviceTiles(states, assigned, device.id) {
					delete(pending, task)
					s.collector.TaskRequeued(task, "disconnect")
				}
			}
		}

		for task, item := range pending {
			if now < item.finishAt {
				continue
			}
			delete(pending, task)
			delete(assigned, task)
			states[task] = 'D'
			s.collector.TaskAccepted(task, item.deviceID, true, 240, 1, nil)
		}

		for _, device := range plans {
			if !joined[device.id] || left[device.id] || now < nextAssign[device.id] {
				continue
			}
			for nextAssign[device.id] <= now {
				assignedAt := nextAssign[device.id]
				task, ok := assignNextTile(states, assigned, device.id)
				if !ok {
					break
				}
				s.collector.TaskAssigned(task, device.id)
				jitter := jitterMin + random.Float64()*(jitterMax-jitterMin)
				pending[task] = scheduledTile{
					deviceID: device.id,
					finishAt: assignedAt + tileLatency*jitter,
				}
				nextAssign[device.id] = assignedAt + 1/(device.rate*jitter)
			}
		}

		if countDoneTiles(states) == simulationTileCount {
			s.collector.JobDone("dashdev-simulated")
			return
		}
	}
}

func totalRateAt(elapsed float64, baseline bool) float64 {
	if baseline {
		return laptopRate
	}
	rate := 0.0
	for _, device := range scriptedDevices {
		if elapsed < device.joinAt || device.hasLeave && elapsed >= device.leaveAt {
			continue
		}
		rate += device.rate
	}
	return rate
}

func assignNextTile(states []byte, assigned map[int]string, deviceID string) (int, bool) {
	for task, state := range states {
		if state == 'Q' || state == 'R' {
			states[task] = 'L'
			assigned[task] = deviceID
			return task, true
		}
	}
	return 0, false
}

func requeueDeviceTiles(states []byte, assigned map[int]string, deviceID string) []int {
	var requeued []int
	for task, assignedDevice := range assigned {
		if assignedDevice != deviceID {
			continue
		}
		if task >= 0 && task < len(states) && states[task] != 'D' {
			states[task] = 'R'
			requeued = append(requeued, task)
		}
		delete(assigned, task)
	}
	return requeued
}

func countDoneTiles(states []byte) int {
	done := 0
	for _, state := range states {
		if state == 'D' {
			done++
		}
	}
	return done
}

func itoa64(value uint64) string {
	const digits = "0123456789"
	if value == 0 {
		return "0"
	}
	var buf [20]byte
	position := len(buf)
	for value > 0 {
		position--
		buf[position] = digits[value%10]
		value /= 10
	}
	return string(buf[position:])
}
