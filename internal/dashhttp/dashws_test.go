package dashhttp

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/scrap-heap/backend/internal/metrics"
)

func TestDashWebSocketFirstMessageIsCurrentSnapshot(t *testing.T) {
	hub := metrics.NewHub(metrics.NewCollector())
	defer hub.Close()
	server := newDashTestServer(t, hub)
	defer server.Close()

	conn := dialDash(t, server)
	defer conn.Close(websocket.StatusNormalClosure, "")

	message := readDashMessage(t, conn)
	var snapshot metrics.Snapshot
	if err := json.Unmarshal(message, &snapshot); err != nil {
		t.Fatalf("decode first websocket message: %v", err)
	}
	if snapshot.Type != "snapshot" || snapshot.Job != "idle" {
		t.Fatalf("first message = %+v, want current idle snapshot", snapshot)
	}
}

func TestDashWebSocketPublishesToTwoClients(t *testing.T) {
	hub := metrics.NewHub(metrics.NewCollector())
	defer hub.Close()
	server := newDashTestServer(t, hub)
	defer server.Close()

	first := dialDash(t, server)
	defer first.Close(websocket.StatusNormalClosure, "")
	second := dialDash(t, server)
	defer second.Close(websocket.StatusNormalClosure, "")
	_ = readDashMessage(t, first)
	_ = readDashMessage(t, second)

	const message = `{"type":"snapshot","jobId":"published"}`
	hub.Publish([]byte(message))
	for index, conn := range []*websocket.Conn{first, second} {
		got := string(readDashMessage(t, conn))
		if got != message {
			t.Errorf("client %d message = %q, want %q", index+1, got, message)
		}
	}
}

func TestSlowSubscriberDoesNotBlockAnother(t *testing.T) {
	hub := metrics.NewHub(metrics.NewCollector())
	defer hub.Close()

	slow, cancelSlow := hub.Subscribe()
	defer cancelSlow()
	active, cancelActive := hub.Subscribe()
	defer cancelActive()
	<-slow
	<-active

	const count = 65
	for i := 0; i < count; i++ {
		message := `{"type":"snapshot","n":` + strconv.Itoa(i) + `}`
		hub.Publish([]byte(message))
		select {
		case received := <-active:
			if got := string(received); got != message {
				t.Fatalf("active subscriber got %q, want %q", got, message)
			}
		default:
			t.Fatal("publishing did not synchronously queue a message for the active subscriber")
		}
	}

	drained := 0
	for range slow {
		drained++
	}
	if drained != 64 {
		t.Fatalf("stalled subscriber buffered %d messages before disconnect, want 64", drained)
	}
	if got := hub.SubscriberCount(); got != 1 {
		t.Fatalf("SubscriberCount() = %d, want 1 active subscriber", got)
	}
}

func TestDashWebSocketDisconnectUnsubscribes(t *testing.T) {
	hub := metrics.NewHub(metrics.NewCollector())
	defer hub.Close()
	server := newDashTestServer(t, hub)

	conn := dialDash(t, server)
	_ = readDashMessage(t, conn)
	waitForSubscriberCount(t, hub, 1)
	if err := conn.Close(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatalf("close dashboard websocket: %v", err)
	}
	waitForSubscriberCount(t, hub, 0)
	server.Close()
}

func TestDashboardHTTPEndpoints(t *testing.T) {
	collector := metrics.NewCollector()
	hub := metrics.NewHub(collector)
	defer hub.Close()
	mux := http.NewServeMux()
	RegisterDash(mux, hub)

	collector.AddRun(metrics.RunRecord{
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
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/export.csv", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/export.csv status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Disposition"); got != `attachment; filename="scrapheap-results.csv"` {
		t.Errorf("Content-Disposition = %q", got)
	}
	rows, err := csv.NewReader(strings.NewReader(rec.Body.String())).ReadAll()
	if err != nil {
		t.Fatalf("parse CSV export: %v", err)
	}
	if len(rows) < 2 {
		t.Fatalf("CSV has %d rows, want header and run row", len(rows))
	}
	if got := strings.Join(rows[0], ","); got != metrics.CSVHeader {
		t.Fatalf("CSV header = %q, want %q", got, metrics.CSVHeader)
	}

	unwired := httptest.NewRecorder()
	mux.ServeHTTP(unwired, httptest.NewRequest(http.MethodPost, "/api/run-again", nil))
	if unwired.Code != http.StatusNotImplemented || strings.TrimSpace(unwired.Body.String()) != `{"error":"run-again not wired"}` {
		t.Fatalf("unwired run-again = %d %s", unwired.Code, unwired.Body)
	}

	var called int32
	hub.OnRunAgain = func() {
		atomic.StoreInt32(&called, 1)
	}
	wired := httptest.NewRecorder()
	mux.ServeHTTP(wired, httptest.NewRequest(http.MethodPost, "/api/run-again", nil))
	if wired.Code != http.StatusAccepted || strings.TrimSpace(wired.Body.String()) != `{"ok":true}` {
		t.Fatalf("wired run-again = %d %s, want 202 {\"ok\":true}", wired.Code, wired.Body)
	}
	if atomic.LoadInt32(&called) != 1 {
		t.Fatal("OnRunAgain was not called before returning")
	}

	var requestedConfig string
	hub.OnRunAgainWithConfig = func(config string) {
		requestedConfig = config
	}
	configured := httptest.NewRecorder()
	mux.ServeHTTP(configured, httptest.NewRequest(http.MethodPost, "/api/run-again?config=B", nil))
	if configured.Code != http.StatusAccepted || requestedConfig != "B" {
		t.Fatalf("configured run-again = %d, config %q; want 202 and B", configured.Code, requestedConfig)
	}

	methodNotAllowed := httptest.NewRecorder()
	mux.ServeHTTP(methodNotAllowed, httptest.NewRequest(http.MethodGet, "/api/run-again", nil))
	if methodNotAllowed.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /api/run-again status = %d, want 405", methodNotAllowed.Code)
	}
}

func TestTileImageEndpoints(t *testing.T) {
	collector := metrics.NewCollector()
	collector.JobStart("tiles-api", 1024)
	collector.DeviceJoined("laptop", "Laptop", 6)
	pixels := bytes.Repeat([]byte{1, 2, 3, 255}, 64*64)
	collector.TaskAccepted(0, "laptop", true, 300, 1, pixels)
	hub := metrics.NewHub(collector)
	defer hub.Close()
	mux := http.NewServeMux()
	RegisterDash(mux, hub)

	for _, test := range []struct {
		path string
		code int
	}{
		{path: "/api/tile/not-number", code: http.StatusBadRequest},
		{path: "/api/tile/1024", code: http.StatusBadRequest},
		{path: "/api/tile/-1", code: http.StatusBadRequest},
		{path: "/api/tile/1", code: http.StatusNotFound},
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, test.path, nil))
		if rec.Code != test.code {
			t.Errorf("GET %s status = %d, want %d", test.path, rec.Code, test.code)
		}
	}

	image := httptest.NewRecorder()
	mux.ServeHTTP(image, httptest.NewRequest(http.MethodGet, "/api/tile/0", nil))
	if image.Code != http.StatusOK {
		t.Fatalf("GET /api/tile/0 status = %d, want 200: %s", image.Code, image.Body)
	}
	if got := image.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Errorf("tile Content-Type = %q", got)
	}
	if got := image.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("tile Cache-Control = %q", got)
	}
	if image.Body.Len() != 16384 || !bytes.Equal(image.Body.Bytes(), pixels) {
		t.Fatalf("tile response = %d bytes, want exact 16384-byte RGBA payload", image.Body.Len())
	}

	tiles := httptest.NewRecorder()
	mux.ServeHTTP(tiles, httptest.NewRequest(http.MethodGet, "/api/tiles", nil))
	if tiles.Code != http.StatusOK {
		t.Fatalf("GET /api/tiles status = %d", tiles.Code)
	}
	var info []metrics.TileInfo
	if err := json.Unmarshal(tiles.Body.Bytes(), &info); err != nil {
		t.Fatalf("decode /api/tiles response: %v", err)
	}
	if len(info) != 1 || info[0] != (metrics.TileInfo{Task: 0, Device: "laptop", ComputeMs: 300, Attempts: 1}) {
		t.Fatalf("/api/tiles = %+v, want metadata for completed tile 0 only", info)
	}
}

func newDashTestServer(t *testing.T, hub *metrics.Hub) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	RegisterDash(mux, hub)
	return httptest.NewServer(mux)
}

func dialDash(t *testing.T, server *httptest.Server) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+`/ws/dash`, nil)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial dashboard websocket: %v", err)
	}
	return conn
}

func readDashMessage(t *testing.T, conn *websocket.Conn) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, message, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read dashboard websocket message: %v", err)
	}
	return message
}

func waitForSubscriberCount(t *testing.T, hub *metrics.Hub, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := hub.SubscriberCount(); got == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("SubscriberCount() = %d, want %d", hub.SubscriberCount(), want)
}
