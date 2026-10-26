package dashhttp

import (
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
	received := make(chan []byte, count)
	go func() {
		for {
			message, ok := <-active
			if !ok {
				return
			}
			received <- message
			if string(message) == `{"type":"snapshot","n":64}` {
				return
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		for i := 0; i < count; i++ {
			hub.Publish([]byte(`{"type":"snapshot","n":` + strconv.Itoa(i) + `}`))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("publishing blocked on a stalled subscriber")
	}

	var last string
	for i := 0; i < count; i++ {
		select {
		case message := <-received:
			last = string(message)
			if last == `{"type":"snapshot","n":64}` {
				i = count
			}
		case <-time.After(time.Second):
			t.Fatal("active subscriber did not receive published messages")
		}
	}
	if last != `{"type":"snapshot","n":64}` {
		t.Fatalf("last active subscriber message = %q", last)
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

	methodNotAllowed := httptest.NewRecorder()
	mux.ServeHTTP(methodNotAllowed, httptest.NewRequest(http.MethodGet, "/api/run-again", nil))
	if methodNotAllowed.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /api/run-again status = %d, want 405", methodNotAllowed.Code)
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
