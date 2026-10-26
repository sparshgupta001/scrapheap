package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type dashClient struct {
	conn      *websocket.Conn
	send      chan []byte
	closeOnce sync.Once
}

func (c *dashClient) close() {
	c.closeOnce.Do(func() {
		close(c.send)
	})
}

// Hub manages WebSocket clients and broadcasts snapshots and events.
type Hub struct {
	collector *Collector

	mu      sync.RWMutex
	clients map[*dashClient]struct{}

	ctx    context.Context
	cancel context.CancelFunc

	OnRunAgain func()
}

// NewHub creates a new Hub and starts its periodic 500ms snapshot broadcast loop.
func NewHub(collector *Collector) *Hub {
	ctx, cancel := context.WithCancel(context.Background())
	h := &Hub{
		collector: collector,
		clients:   make(map[*dashClient]struct{}),
		ctx:       ctx,
		cancel:    cancel,
	}

	// Subscribe to events from collector
	if collector != nil {
		collector.OnEvent(func(event any) {
			h.Broadcast(event)
		})
	}

	// Start 500ms ticker for periodic snapshots
	go h.runSnapshotLoop()

	return h
}

// Collector returns the associated Collector.
func (h *Hub) Collector() *Collector {
	return h.collector
}

// Close shuts down the hub and disconnects all clients.
func (h *Hub) Close() {
	h.cancel()
	h.mu.Lock()
	defer h.mu.Unlock()
	for client := range h.clients {
		client.close()
		_ = client.conn.Close(websocket.StatusGoingAway, "server shutting down")
		delete(h.clients, client)
	}
}

// ExportCSV returns the CSV export from the underlying collector.
func (h *Hub) ExportCSV() string {
	if h.collector == nil {
		return CSVHeader + "\n"
	}
	return h.collector.ExportCSV()
}

func (h *Hub) runSnapshotLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			if h.collector != nil {
				snap := h.collector.Snapshot()
				h.Broadcast(snap)
			}
		}
	}
}

// Broadcast serializes any message (snapshot or event) and delivers to all connected clients.
func (h *Hub) Broadcast(msg any) {
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for client := range h.clients {
		select {
		case client.send <- data:
		default:
			// Slow client buffer full; drop message to prevent blocking
		}
	}
}

func (h *Hub) register(c *dashClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = struct{}{}
}

func (h *Hub) unregister(c *dashClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		c.close()
	}
}

// ServeWS handles WebSocket upgrade and manages client connection lifetime.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		return
	}
	defer conn.CloseNow()

	client := &dashClient{
		conn: conn,
		send: make(chan []byte, 128),
	}
	h.register(client)
	defer h.unregister(client)

	// Immediately send the current snapshot so new client gets immediate state
	if h.collector != nil {
		snap := h.collector.Snapshot()
		if initData, err := json.Marshal(snap); err == nil {
			select {
			case client.send <- initData:
			default:
			}
		}
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Writer goroutine
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-client.send:
				if !ok {
					return
				}
				writeCtx, writeCancel := context.WithTimeout(ctx, 3*time.Second)
				err := conn.Write(writeCtx, websocket.MessageText, msg)
				writeCancel()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()

	// Reader loop (maintains connection and detects closure)
	for {
		_, _, err := conn.Read(ctx)
		if err != nil {
			break
		}
	}
}

// HandleWS is an alias for ServeWS.
func (h *Hub) HandleWS(w http.ResponseWriter, r *http.Request) {
	h.ServeWS(w, r)
}
