package main

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/scrap-heap/backend/internal/metrics"
)

func TestRateSchedule(t *testing.T) {
	tests := []struct {
		elapsed  float64
		baseline bool
		want     float64
	}{
		{elapsed: 0, want: 8},
		{elapsed: 9.99, want: 8},
		{elapsed: 10, want: 14},
		{elapsed: 18, want: 20},
		{elapsed: 24, want: 24},
		{elapsed: 35, want: 18},
		{elapsed: 100, baseline: true, want: 8},
	}
	for _, test := range tests {
		if got := totalRateAt(test.elapsed, test.baseline); got != test.want {
			t.Errorf("totalRateAt(%v, %v) = %v, want %v", test.elapsed, test.baseline, got, test.want)
		}
	}
}

func TestAwareTileOrderPrefersHeavyForLaptop(t *testing.T) {
	laptopOrder := tileOrder("aware", "laptop", nil)
	phoneOrder := tileOrder("aware", "pixel-7", nil)
	firstHeavy := -1
	firstLight := -1
	for _, task := range laptopOrder {
		if isHeavyTile(task) {
			firstHeavy = task
			break
		}
	}
	for _, task := range phoneOrder {
		if !isHeavyTile(task) {
			firstLight = task
			break
		}
	}
	if firstHeavy < 0 || firstLight < 0 {
		t.Fatal("tile classifier did not produce both heavy and light task classes")
	}
	if !isHeavyTile(laptopOrder[0]) {
		t.Fatalf("laptop's first tile %d is not heavy", laptopOrder[0])
	}
	if isHeavyTile(phoneOrder[0]) {
		t.Fatalf("phone's first tile %d is heavy", phoneOrder[0])
	}
}

func TestTileAssignmentAndPixelRequeue(t *testing.T) {
	states := []byte{'Q', 'Q', 'Q', 'Q', 'Q'}
	assigned := make(map[int]string)
	order := []int{0, 1, 2, 3, 4}
	for expectedTask := 0; expectedTask < 4; expectedTask++ {
		task, ok := assignNextTile(states, assigned, order, "pixel-7")
		if !ok || task != expectedTask {
			t.Fatalf("assignNextTile() = (%d, %v), want (%d, true)", task, ok, expectedTask)
		}
	}
	states[1] = 'D'
	requeued := requeueDeviceTiles(states, assigned, "pixel-7")
	if len(requeued) != 3 {
		t.Fatalf("requeued %v, want the 3 unfinished leases", requeued)
	}
	if states[1] != 'D' {
		t.Fatalf("completed tile changed to %q during requeue", states[1])
	}
	for _, task := range []int{0, 2, 3} {
		if states[task] != 'R' {
			t.Errorf("tile %d state = %q, want R", task, states[task])
		}
		if _, stillAssigned := assigned[task]; stillAssigned {
			t.Errorf("requeued tile %d still has an assignment", task)
		}
	}
	task, ok := assignNextTile(states, assigned, order, "laptop")
	if !ok || task != 0 || states[task] != 'L' || assigned[task] != "laptop" {
		t.Fatalf("requeued tile was not reassigned: got (%d, %v), state=%q", task, ok, states[task])
	}
}

func TestRequeueOnlyLeavesSelectedDevice(t *testing.T) {
	states := []byte{'L', 'L', 'L'}
	assigned := map[int]string{0: "pixel-7", 1: "laptop", 2: "pixel-7"}
	requeued := requeueDeviceTiles(states, assigned, "pixel-7")
	if len(requeued) != 2 || states[1] != 'L' || len(assigned) != 1 {
		t.Fatalf("requeue result states=%q assignments=%v requeued=%v", states, assigned, requeued)
	}
}

func TestScriptedRunRendersAndCompletesAllTiles(t *testing.T) {
	collector := metrics.NewCollector()
	var eventMu sync.Mutex
	seen := make(map[string]int)
	collector.OnEvent(func(event any) {
		typeGetter, ok := event.(interface{ GetType() string })
		if ok {
			eventMu.Lock()
			seen[typeGetter.GetType()]++
			eventMu.Unlock()
			return
		}
		switch event.(type) {
		case metrics.DeviceJoinedEvent:
			eventMu.Lock()
			seen["device_joined"]++
			eventMu.Unlock()
		case metrics.DeviceLeftEvent:
			eventMu.Lock()
			seen["device_left"]++
			eventMu.Unlock()
		case metrics.TileRequeuedEvent:
			eventMu.Lock()
			seen["tile_requeued"]++
			eventMu.Unlock()
		case metrics.TileDoneEvent:
			eventMu.Lock()
			seen["tile_done"]++
			eventMu.Unlock()
		case metrics.JobDoneEvent:
			eventMu.Lock()
			seen["job_done"]++
			eventMu.Unlock()
		}
	})

	sim := &simulator{collector: collector, speed: 50, mode: "aware"}
	defer sim.close()
	sim.start(false)
	sim.mu.Lock()
	done := sim.done
	sim.mu.Unlock()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("scripted run did not complete within 20 seconds")
	}

	snapshot := collector.Snapshot()
	if snapshot.Job != "done" || snapshot.Done != simulationTileCount ||
		snapshot.VerifiedOK != simulationTileCount || snapshot.VerifiedFail != 0 {
		t.Fatalf("final snapshot = %+v, want 1024 verified tiles and no failures", snapshot)
	}
	for task, state := range snapshot.TileStates {
		if state != 'D' {
			t.Fatalf("tile %d ended in state %q, want D", task, state)
		}
		pixels, ok := collector.TileRGBA(task)
		if !ok || len(pixels) != 16384 {
			t.Fatalf("tile %d pixels = %d bytes, %v; want 16384 bytes", task, len(pixels), ok)
		}
	}
	if info := collector.TilesInfo(); len(info) != simulationTileCount {
		t.Fatalf("TilesInfo() = %d entries, want %d", len(info), simulationTileCount)
	}
	secondAttempts := 0
	laptopHeavyTiles := 0
	for _, tile := range collector.TilesInfo() {
		if tile.Attempts > 1 {
			secondAttempts++
		}
		if isHeavyTile(tile.Task) && tile.Device == "laptop" {
			laptopHeavyTiles++
		}
		if isHeavyTile(tile.Task) && (tile.ComputeMs < 500 || tile.ComputeMs > 900) {
			t.Errorf("heavy tile %d computeMs = %d, outside 500-900", tile.Task, tile.ComputeMs)
		}
		if !isHeavyTile(tile.Task) && (tile.ComputeMs < 120 || tile.ComputeMs > 450) {
			t.Errorf("light tile %d computeMs = %d, outside 120-450", tile.Task, tile.ComputeMs)
		}
	}
	if secondAttempts == 0 {
		t.Fatal("no requeued tile completed with attempt 2")
	}
	if laptopHeavyTiles == 0 {
		t.Fatal("aware schedule did not assign any heavy tiles to the laptop")
	}
	if runs := collector.GetRuns(); len(runs) != 1 || runs[0].Config != "aware+3" || runs[0].Devices != 4 {
		t.Fatalf("script run CSV metrics = %+v, want one aware+3 run with 4 devices", runs)
	}
	eventMu.Lock()
	defer eventMu.Unlock()
	for _, event := range []string{"device_joined", "device_left", "tile_requeued", "tile_done", "job_done"} {
		if seen[event] == 0 {
			t.Errorf("scripted run did not publish %s", event)
		}
	}
}

func TestBaselineRunUsesOnlyLaptopAndIsCSVFirst(t *testing.T) {
	collector := metrics.NewCollector()
	sim := &simulator{collector: collector, speed: 100, mode: "aware"}
	defer sim.close()
	sim.start(true)
	sim.mu.Lock()
	done := sim.done
	sim.mu.Unlock()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("baseline simulation did not complete within 20 seconds")
	}

	runs := collector.GetRuns()
	if len(runs) != 1 || runs[0].Config != "B" || runs[0].Devices != 1 || runs[0].Tiles != simulationTileCount {
		t.Fatalf("baseline run = %+v, want config B, one device, 1024 tiles", runs)
	}
	if got := totalRateAt(100, true); got != laptopRate {
		t.Fatalf("baseline rate = %v, want %v", got, laptopRate)
	}
}

func TestPullTileOrderIsSeededRandom(t *testing.T) {
	first := tileOrder("pull", "laptop", rand.New(rand.NewSource(4)))
	second := tileOrder("pull", "laptop", rand.New(rand.NewSource(4)))
	for index := range first {
		if first[index] != second[index] {
			t.Fatalf("seeded pull order differs at index %d", index)
		}
	}
}
