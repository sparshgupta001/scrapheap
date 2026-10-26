package main

import (
	"context"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scrap-heap/backend/internal/metrics"
)

const (
	simulationTileCount = 1024
	tilePixels          = 64
	canvasPixels        = 2048
	maxIterations       = 420

	laptopRate = 8.0
	pixelRate  = 6.0
	iphoneRate = 6.0
	ipadRate   = 4.0

	pixelJoinAt  = 10.0
	iphoneJoinAt = 18.0
	ipadJoinAt   = 24.0
	pixelLeaveAt = 35.0

	jitterMin = 0.85
	jitterMax = 1.15
)

// Rate arithmetic: 0-10s = 80 tiles; 10-18s = 112; 18-24s = 120;
// 24-35s = 264 (576 total); then 448 at 18 tiles/s takes about 24.9s.
// A few Pixel requeues put completion near t=60-62s before per-tile jitter.

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
	deviceID  string
	finishAt  float64
	computeMs uint32
	attempt   uint32
}

type simulator struct {
	collector *metrics.Collector
	speed     float64
	mode      string
	nextRun   atomic.Uint64

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

var heavyTilesOnce sync.Once
var heavyTiles [simulationTileCount]bool

func (s *simulator) start(baseline bool) {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
		<-s.done
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	s.cancel = cancel
	s.done = done
	runNumber := s.nextRun.Add(1)
	s.mu.Unlock()

	go func() {
		defer close(done)
		s.run(ctx, runNumber, baseline)
	}()
}

func (s *simulator) close() {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
		<-s.done
		s.cancel = nil
	}
	s.mu.Unlock()
}

func (s *simulator) run(ctx context.Context, runNumber uint64, baseline bool) {
	mode := s.mode
	if mode != "pull" && mode != "aware" {
		mode = "aware"
	}
	s.collector.SetConfig("")
	s.collector.SetScheduler(mode)
	if baseline {
		s.collector.SetConfig("B")
	}
	jobID := "run-" + itoa64(runNumber)
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
	attempts := make([]uint32, simulationTileCount)
	joined := make(map[string]bool)
	left := make(map[string]bool)
	nextAssign := make(map[string]float64)
	random := rand.New(rand.NewSource(int64(runNumber)))
	orders := map[string][]int{
		"laptop": tileOrder(mode, "laptop", random),
	}
	if mode == "aware" {
		for _, device := range plans {
			if device.id != "laptop" {
				orders[device.id] = tileOrder(mode, device.id, nil)
			}
		}
	} else {
		for _, device := range plans {
			orders[device.id] = orders["laptop"]
		}
	}
	started := time.Now()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		now := time.Since(started).Seconds() * s.speed
		for _, device := range plans {
			if !joined[device.id] && now >= device.joinAt {
				joined[device.id] = true
				s.collector.DeviceJoined(device.id, device.label, device.threads)
				nextAssign[device.id] = device.joinAt
			}
		}

		for _, task := range sortedPendingTasks(pending) {
			item := pending[task]
			if now < item.finishAt {
				continue
			}
			delete(pending, task)
			delete(assigned, task)
			pixels := renderTile(task)
			states[task] = 'D'
			s.collector.TaskAccepted(task, item.deviceID, true, item.computeMs, item.attempt, pixels)
		}

		for _, device := range plans {
			if device.hasLeave && !left[device.id] && now >= device.leaveAt {
				left[device.id] = true
				s.collector.DeviceLeft(device.id)
				for _, task := range requeueDeviceTiles(states, assigned, device.id) {
					delete(pending, task)
					s.collector.TaskRequeued(task, "disconnect")
				}
			}
		}

		for _, device := range plans {
			if !joined[device.id] || left[device.id] || now < nextAssign[device.id] {
				continue
			}
			deviceOrder := orders[device.id]
			for nextAssign[device.id] <= now {
				assignedAt := nextAssign[device.id]
				task, ok := assignNextTile(states, assigned, deviceOrder, device.id)
				if !ok {
					break
				}
				attempts[task]++
				s.collector.TaskAssigned(task, device.id)
				jitter := jitterMin + random.Float64()*(jitterMax-jitterMin)
				pending[task] = scheduledTile{
					deviceID:  device.id,
					finishAt:  assignedAt + 0.75*jitter,
					computeMs: simulatedComputeMs(task, random),
					attempt:   attempts[task],
				}
				nextAssign[device.id] = assignedAt + 1/(device.rate*jitter)
			}
		}

		if countDoneTiles(states) == simulationTileCount {
			s.collector.JobDone("dashdev-mandelbrot")
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

func tileOrder(mode, deviceID string, random *rand.Rand) []int {
	order := make([]int, simulationTileCount)
	for task := range order {
		order[task] = task
	}
	if mode == "pull" {
		if random != nil {
			random.Shuffle(len(order), func(i, j int) {
				order[i], order[j] = order[j], order[i]
			})
		}
		return order
	}
	sort.SliceStable(order, func(i, j int) bool {
		iHeavy := isHeavyTile(order[i])
		jHeavy := isHeavyTile(order[j])
		if iHeavy == jHeavy {
			return order[i] < order[j]
		}
		if deviceID == "laptop" {
			return iHeavy
		}
		return !iHeavy
	})
	return order
}

func assignNextTile(states []byte, assigned map[int]string, order []int, deviceID string) (int, bool) {
	for _, task := range order {
		if task >= 0 && task < len(states) && (states[task] == 'Q' || states[task] == 'R') {
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
	sort.Ints(requeued)
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

func sortedPendingTasks(pending map[int]scheduledTile) []int {
	tasks := make([]int, 0, len(pending))
	for task := range pending {
		tasks = append(tasks, task)
	}
	sort.Ints(tasks)
	return tasks
}

func simulatedComputeMs(task int, random *rand.Rand) uint32 {
	minimum, maximum := 120, 450
	if isHeavyTile(task) {
		minimum, maximum = 500, 900
	}
	return uint32(minimum + random.Intn(maximum-minimum+1))
}

func isHeavyTile(task int) bool {
	if task < 0 || task >= simulationTileCount {
		return false
	}
	heavyTilesOnce.Do(func() {
		for taskID := 0; taskID < simulationTileCount; taskID++ {
			heavyTiles[taskID] = classifyHeavyTile(taskID)
		}
	})
	return heavyTiles[task]
}

func classifyHeavyTile(task int) bool {
	row, col := task/32, task%32
	escaped := 0
	totalIterations := 0
	const samplePositions = 5
	for sample := 0; sample < samplePositions; sample++ {
		offset := float64(sample) / float64(samplePositions-1)
		x := (float64(col)+offset)*tilePixels + tilePixels/2
		y := (float64(row)+1-offset)*tilePixels - tilePixels/2
		cx, cy := fractalCoordinate(x, y)
		iterations := mandelbrotIterations(cx, cy, 160)
		totalIterations += iterations
		if iterations < 160 {
			escaped++
		}
	}
	return totalIterations/samplePositions > 70 || escaped > 0 && escaped < samplePositions
}

func renderTile(task int) []byte {
	pixels := make([]byte, tilePixels*tilePixels*4)
	row, col := task/32, task%32
	for y := 0; y < tilePixels; y++ {
		for x := 0; x < tilePixels; x++ {
			cx, cy := fractalCoordinate(float64(col*tilePixels+x), float64(row*tilePixels+y))
			colorMandelbrot(pixels[(y*tilePixels+x)*4:], cx, cy)
		}
	}
	return pixels
}

func fractalCoordinate(x, y float64) (float64, float64) {
	const centerX = -0.7435669
	const centerY = 0.1314023
	const scale = 0.00012
	return centerX + (x-float64(canvasPixels)/2)*scale,
		centerY + (y-float64(canvasPixels)/2)*scale
}

func mandelbrotIterations(cx, cy float64, maximum int) int {
	zx, zy := 0.0, 0.0
	for iteration := 0; iteration < maximum; iteration++ {
		zx2, zy2 := zx*zx, zy*zy
		if zx2+zy2 > 256 {
			return iteration
		}
		zy = 2*zx*zy + cy
		zx = zx2 - zy2 + cx
	}
	return maximum
}

func colorMandelbrot(pixel []byte, cx, cy float64) {
	xMinusQuarter := cx - 0.25
	q := xMinusQuarter*xMinusQuarter + cy*cy
	if q*(q+xMinusQuarter) <= 0.25*cy*cy || (cx+1)*(cx+1)+cy*cy <= 0.0625 {
		pixel[0], pixel[1], pixel[2], pixel[3] = 7, 9, 18, 255
		return
	}
	zx, zy := 0.0, 0.0
	for iteration := 0; iteration < maxIterations; iteration++ {
		zx2, zy2 := zx*zx, zy*zy
		magnitude := zx2 + zy2
		if magnitude > 256 {
			smooth := float64(iteration+1) - math.Log2(math.Log2(math.Sqrt(magnitude)))
			hue := math.Mod(smooth*0.035, 1)
			r, g, b := hueColor(hue)
			pixel[0], pixel[1], pixel[2], pixel[3] = r, g, b, 255
			return
		}
		zy = 2*zx*zy + cy
		zx = zx2 - zy2 + cx
	}
	pixel[0], pixel[1], pixel[2], pixel[3] = 7, 9, 18, 255
}

func hueColor(hue float64) (byte, byte, byte) {
	segment := hue * 6
	x := 1 - math.Abs(math.Mod(segment, 2)-1)
	var r, g, b float64
	switch int(segment) % 6 {
	case 0:
		r, g = 1, x
	case 1:
		r, g = x, 1
	case 2:
		g, b = 1, x
	case 3:
		g, b = x, 1
	case 4:
		r, b = x, 1
	default:
		r, b = 1, x
	}
	return byte(30 + r*205), byte(30 + g*205), byte(40 + b*195)
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
