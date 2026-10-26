package metrics

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSnapshotJSONContract(t *testing.T) {
	c := NewCollector()
	c.JobStart("run-3", 10)
	c.DeviceJoined("d1", "Laptop", 6)
	c.TaskAssigned(0, "d1")
	c.TaskAccepted(0, "d1", true)
	c.TaskRequeued(1, "lease")
	c.DupDropped(0)

	snap := c.Snapshot()
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("failed to marshal snapshot: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	requiredKeys := []string{
		"type", "t", "job", "jobId", "done", "total", "tilesPerSec",
		"devices", "retries", "dupesDropped", "verifiedOK", "verifiedFail", "tileStates",
	}
	for _, key := range requiredKeys {
		if _, ok := m[key]; !ok {
			t.Errorf("missing required key in snapshot JSON: %s", key)
		}
	}

	if m["type"] != "snapshot" {
		t.Errorf("expected type 'snapshot', got %v", m["type"])
	}
	if m["job"] != "running" {
		t.Errorf("expected job 'running', got %v", m["job"])
	}
	if m["jobId"] != "run-3" {
		t.Errorf("expected jobId 'run-3', got %v", m["jobId"])
	}

	devices, ok := m["devices"].([]any)
	if !ok || len(devices) == 0 {
		t.Fatalf("expected non-empty devices array")
	}
	devMap, ok := devices[0].(map[string]any)
	if !ok {
		t.Fatalf("expected device to be map")
	}
	devKeys := []string{"id", "label", "threads", "tilesDone", "tilesPerSec", "state"}
	for _, key := range devKeys {
		if _, ok := devMap[key]; !ok {
			t.Errorf("missing required key in device JSON: %s", key)
		}
	}
}

func TestRollingTilesPerSec(t *testing.T) {
	c := NewCollector()
	baseTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	currentTime := baseTime

	c.SetNowFunc(func() time.Time {
		return currentTime
	})
	c.SetWindow(3 * time.Second)

	c.JobStart("test-run", 100)
	c.DeviceJoined("d1", "Worker", 2)

	// Complete 6 tiles over 2 seconds (3 tiles/sec)
	for i := 0; i < 6; i++ {
		currentTime = currentTime.Add(333 * time.Millisecond)
		c.TaskAccepted(i, "d1", true)
	}

	snap := c.Snapshot()
	if snap.TilesPerSec <= 0 {
		t.Errorf("expected positive tilesPerSec, got %.2f", snap.TilesPerSec)
	}

	// Advance time past the 3s window with no more tasks
	currentTime = currentTime.Add(4 * time.Second)
	snapAfter := c.Snapshot()
	if snapAfter.TilesPerSec != 0.0 {
		t.Errorf("expected tilesPerSec to drop to 0 after window expired, got %.2f", snapAfter.TilesPerSec)
	}
}

func TestRequeueMarksTileR(t *testing.T) {
	c := NewCollector()
	c.JobStart("job-requeue", 5)
	c.DeviceJoined("d1", "Phone", 1)

	// Initially all 'Q'
	snap := c.Snapshot()
	if snap.TileStates != "QQQQQ" {
		t.Fatalf("expected 'QQQQQ', got %s", snap.TileStates)
	}

	// Assign task 2 -> 'L'
	c.TaskAssigned(2, "d1")
	snap = c.Snapshot()
	if snap.TileStates != "QQLQQ" {
		t.Fatalf("expected 'QQLQQ', got %s", snap.TileStates)
	}

	// Requeue task 2 -> 'R'
	c.TaskRequeued(2, "disconnect")
	snap = c.Snapshot()
	if snap.TileStates != "QQRQQ" {
		t.Fatalf("expected 'QQRQQ', got %s", snap.TileStates)
	}
	if snap.Retries != 1 {
		t.Errorf("expected retries == 1, got %d", snap.Retries)
	}

	// Reassign task 2 -> 'L'
	c.TaskAssigned(2, "d1")
	snap = c.Snapshot()
	if snap.TileStates != "QQLQQ" {
		t.Fatalf("expected 'QQLQQ' after reassignment, got %s", snap.TileStates)
	}

	// Complete task 2 -> 'D'
	c.TaskAccepted(2, "d1", true)
	snap = c.Snapshot()
	if snap.TileStates != "QQDQQ" {
		t.Fatalf("expected 'QQDQQ', got %s", snap.TileStates)
	}
}

func TestDeviceLeftSetsStateGone(t *testing.T) {
	c := NewCollector()
	c.DeviceJoined("d1", "Pixel 7", 4)

	snap := c.Snapshot()
	if len(snap.Devices) != 1 {
		t.Fatalf("expected 1 device, got %d", len(snap.Devices))
	}
	if snap.Devices[0].State != "active" {
		t.Errorf("expected active state, got %s", snap.Devices[0].State)
	}

	c.DeviceLeft("d1")
	snapAfter := c.Snapshot()
	if len(snapAfter.Devices) != 1 {
		t.Fatalf("expected device to remain in list, got %d", len(snapAfter.Devices))
	}
	if snapAfter.Devices[0].State != "gone" {
		t.Errorf("expected state 'gone', got %s", snapAfter.Devices[0].State)
	}
}

func TestDeviceReconnectPreservesTilesDone(t *testing.T) {
	c := NewCollector()
	c.JobStart("job-reconnect", 20)

	// Device joins
	c.DeviceJoined("phone-1", "Pixel 7", 4)
	c.TaskAssigned(0, "phone-1")
	c.TaskAccepted(0, "phone-1", true)
	c.TaskAssigned(1, "phone-1")
	c.TaskAccepted(1, "phone-1", true)

	snap := c.Snapshot()
	if len(snap.Devices) != 1 {
		t.Fatalf("expected 1 device, got %d", len(snap.Devices))
	}
	if snap.Devices[0].TilesDone != 2 {
		t.Errorf("expected 2 tilesDone, got %d", snap.Devices[0].TilesDone)
	}

	// Device disconnects
	c.DeviceLeft("phone-1")
	snap = c.Snapshot()
	if snap.Devices[0].State != "gone" {
		t.Errorf("expected state 'gone', got %s", snap.Devices[0].State)
	}

	// Phone reconnects after lock
	c.DeviceJoined("phone-1", "Pixel 7", 4)
	snap = c.Snapshot()
	if len(snap.Devices) != 1 {
		t.Fatalf("expected still 1 device bar after reconnect, got %d", len(snap.Devices))
	}
	if snap.Devices[0].State != "active" {
		t.Errorf("expected state 'active' on reconnect, got %s", snap.Devices[0].State)
	}
	if snap.Devices[0].TilesDone != 2 {
		t.Errorf("expected tilesDone preserved at 2, got %d", snap.Devices[0].TilesDone)
	}
}

func TestShortLabel(t *testing.T) {
	// Custom label preserved
	if got := ShortLabel("My Phone", "Mozilla/5.0..."); got != "My Phone" {
		t.Errorf("expected 'My Phone', got %s", got)
	}

	// iOS tests
	if got := ShortLabel("", "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"); got != "iPhone Safari" {
		t.Errorf("expected 'iPhone Safari', got %s", got)
	}
	if got := ShortLabel("", "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/105.0.5195.100 Mobile/15E148 Safari/604.1"); got != "iPhone Chrome" {
		t.Errorf("expected 'iPhone Chrome', got %s", got)
	}
	if got := ShortLabel("", "Mozilla/5.0 (iPad; CPU OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"); got != "iPad Safari" {
		t.Errorf("expected 'iPad Safari', got %s", got)
	}

	// Android tests
	if got := ShortLabel("", "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Mobile Safari/537.36"); got != "Android Chrome" {
		t.Errorf("expected 'Android Chrome', got %s", got)
	}
	if got := ShortLabel("", "Mozilla/5.0 (Android 13; Mobile; rv:109.0) Gecko/116.0 Firefox/116.0"); got != "Android Firefox" {
		t.Errorf("expected 'Android Firefox', got %s", got)
	}

	// Desktop tests
	if got := ShortLabel("", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.69"); got != "Windows Edge" {
		t.Errorf("expected 'Windows Edge', got %s", got)
	}
	if got := ShortLabel("", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"); got != "Windows Chrome" {
		t.Errorf("expected 'Windows Chrome', got %s", got)
	}
	if got := ShortLabel("", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/116.0.0.0 Safari/537.36"); got != "Mac Chrome" {
		t.Errorf("expected 'Mac Chrome', got %s", got)
	}

	// Unknown
	if got := ShortLabel("", "UnknownBot/1.0"); got != "Device" {
		t.Errorf("expected 'Device', got %s", got)
	}
}

func TestCSVHeaderAndSpeedupEfficiencyMath(t *testing.T) {
	// Case 1: No baseline run
	runsNoBaseline := []RunRecord{
		{
			RunID:        "run-1",
			Config:       "standard",
			Devices:      2,
			Threads:      4,
			WallS:        40.0,
			ComputeS:     160.0,
			Tiles:        100,
			TilesPerSec:  2.5,
			Retries:      1,
			DupesDropped: 0,
			VerifiedOK:   100,
			VerifiedFail: 0,
			BytesIn:      1024,
			BytesOut:     2048,
		},
	}
	csvOut := FormatCSV(runsNoBaseline)
	lines := strings.Split(strings.TrimSpace(csvOut), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines (header + 1 row), got %d", len(lines))
	}
	if lines[0] != CSVHeader {
		t.Errorf("header mismatch:\ngot:  %s\nwant: %s", lines[0], CSVHeader)
	}
	// Row should have empty speedup and efficiency
	cols := strings.Split(lines[1], ",")
	if cols[8] != "" || cols[9] != "" {
		t.Errorf("expected empty speedup and efficiency when no baseline, got speedup=%q eff=%q", cols[8], cols[9])
	}

	// Case 2: With baseline run "B"
	runsWithBaseline := []RunRecord{
		{
			RunID:        "run-b",
			Config:       "B",
			Devices:      1,
			Threads:      2,
			WallS:        60.0,
			ComputeS:     120.0,
			Tiles:        100,
			TilesPerSec:  1.67,
			Retries:      0,
			DupesDropped: 0,
			VerifiedOK:   100,
			VerifiedFail: 0,
			BytesIn:      1000,
			BytesOut:     2000,
		},
		{
			RunID:        "run-cluster",
			Config:       "cluster-4dev",
			Devices:      4,
			Threads:      16,
			WallS:        20.0,
			ComputeS:     320.0,
			Tiles:        100,
			TilesPerSec:  5.0,
			Retries:      2,
			DupesDropped: 1,
			VerifiedOK:   100,
			VerifiedFail: 0,
			BytesIn:      4000,
			BytesOut:     8000,
		},
	}

	csvOut2 := FormatCSV(runsWithBaseline)
	lines2 := strings.Split(strings.TrimSpace(csvOut2), "\n")
	if len(lines2) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(lines2))
	}

	// Baseline run row: speedup = 60/60 = 1.00, eff = 1.00 / 1 = 1.00
	rowB := strings.Split(lines2[1], ",")
	if rowB[8] != "1.00" {
		t.Errorf("expected baseline speedup '1.00', got %s", rowB[8])
	}
	if rowB[9] != "1.00" {
		t.Errorf("expected baseline efficiency '1.00', got %s", rowB[9])
	}

	// Cluster run row: speedup = 60/20 = 3.00, eff = 3.00 / 4 = 0.75
	rowCluster := strings.Split(lines2[2], ",")
	if rowCluster[8] != "3.00" {
		t.Errorf("expected cluster speedup '3.00', got %s", rowCluster[8])
	}
	if rowCluster[9] != "0.75" {
		t.Errorf("expected cluster efficiency '0.75', got %s", rowCluster[9])
	}
}

func TestCollectorConcurrency50Goroutines(t *testing.T) {
	c := NewCollector()
	c.JobStart("race-check-50", 1000)

	var wg sync.WaitGroup
	const goroutines = 50

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		devID := "dev-" + strings.Repeat("x", i%5) + string(rune('a'+i))
		go func(id string, idx int) {
			defer wg.Done()
			c.DeviceJoined(id, "Device-"+id, 2)
			for task := idx * 10; task < (idx+1)*10; task++ {
				c.TaskAssigned(task, id)
				if task%4 == 0 {
					c.TaskRequeued(task, "lease")
					c.TaskAssigned(task, id)
				}
				c.TaskAccepted(task, id, true)
			}
			_ = c.Snapshot()
			if idx%3 == 0 {
				c.DeviceLeft(id)
			}
			if idx%6 == 0 {
				c.DeviceJoined(id, "Device-"+id, 2) // Reconnect
			}
		}(devID, i)
	}

	wg.Wait()
	snap := c.Snapshot()
	if snap.Done <= 0 {
		t.Errorf("expected completed tasks, got %d", snap.Done)
	}
}
