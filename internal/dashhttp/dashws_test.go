package dashhttp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scrap-heap/backend/internal/metrics"
)

func TestRegisterDashEndpoints(t *testing.T) {
	c := metrics.NewCollector()
	hub := metrics.NewHub(c)
	defer hub.Close()

	// Add dummy run to collector
	c.AddRun(metrics.RunRecord{
		RunID:       "run-dev-1",
		Config:      "B",
		Devices:     1,
		Threads:     6,
		WallS:       55.2,
		ComputeS:    331.2,
		Tiles:       1024,
		TilesPerSec: 18.5,
		Retries:     2,
		VerifiedOK:  1024,
	})

	mux := http.NewServeMux()
	RegisterDash(mux, hub)

	// Test GET /api/export.csv
	recCSV := httptest.NewRecorder()
	reqCSV := httptest.NewRequest(http.MethodGet, "/api/export.csv", nil)
	mux.ServeHTTP(recCSV, reqCSV)

	if recCSV.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /api/export.csv, got %d", recCSV.Code)
	}
	cd := recCSV.Header().Get("Content-Disposition")
	if !strings.Contains(cd, "scrapheap-results.csv") {
		t.Errorf("expected Content-Disposition with scrapheap-results.csv, got %s", cd)
	}
	lines := strings.Split(strings.TrimSpace(recCSV.Body.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected at least 2 lines (header + row), got %d", len(lines))
	}
	if lines[0] != metrics.CSVHeader {
		t.Errorf("header mismatch: %s", lines[0])
	}

	// Test POST /api/run-again without hook (501 Not Implemented)
	recRA1 := httptest.NewRecorder()
	reqRA1 := httptest.NewRequest(http.MethodPost, "/api/run-again", nil)
	mux.ServeHTTP(recRA1, reqRA1)
	if recRA1.Code != http.StatusNotImplemented {
		t.Errorf("expected 501 for unhandled run-again, got %d", recRA1.Code)
	}

	// Test POST /api/run-again with hook (200 OK)
	var runAgainCalled int32
	hub.OnRunAgain = func() {
		atomic.StoreInt32(&runAgainCalled, 1)
	}

	recRA2 := httptest.NewRecorder()
	reqRA2 := httptest.NewRequest(http.MethodPost, "/api/run-again", nil)
	mux.ServeHTTP(recRA2, reqRA2)

	if recRA2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from run-again hook, got %d", recRA2.Code)
	}
	var res map[string]bool
	if err := json.Unmarshal(recRA2.Body.Bytes(), &res); err != nil || !res["ok"] {
		t.Errorf("expected ok: true, got %v (err %v)", res, err)
	}

	// Wait briefly for goroutine hook
	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&runAgainCalled) != 1 {
		t.Errorf("expected OnRunAgain hook to be called")
	}
}
