package metrics

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const subscriberBufferSize = 64

type subscriber struct {
	messages chan []byte
}

// Hub broadcasts snapshots and events to dashboard subscribers.
type Hub struct {
	collector *Collector

	mu          sync.Mutex
	subscribers map[*subscriber]struct{}
	closed      bool

	ctx    context.Context
	cancel context.CancelFunc

	OnRunAgain func()
}

// NewHub creates a Hub and broadcasts collector snapshots every 500ms.
func NewHub(collector *Collector) *Hub {
	ctx, cancel := context.WithCancel(context.Background())
	h := &Hub{
		collector:   collector,
		subscribers: make(map[*subscriber]struct{}),
		ctx:         ctx,
		cancel:      cancel,
	}

	if collector != nil {
		collector.OnEvent(func(event any) {
			h.Broadcast(event)
		})
	}
	go h.runSnapshotLoop()
	return h
}

// Collector returns the associated Collector.
func (h *Hub) Collector() *Collector {
	return h.collector
}

// Close stops the snapshot loop and closes all subscriber channels.
func (h *Hub) Close() {
	h.cancel()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for sub := range h.subscribers {
		delete(h.subscribers, sub)
		close(sub.messages)
	}
}

// ExportCSV returns the CSV export from the underlying collector.
func (h *Hub) ExportCSV() string {
	if h.collector == nil {
		return CSVHeader + "\n"
	}
	return h.collector.ExportCSV()
}

// CurrentSnapshotJSON returns the current snapshot encoded as JSON.
func (h *Hub) CurrentSnapshotJSON() []byte {
	snapshot := Snapshot{
		Type:    "snapshot",
		Job:     "idle",
		Devices: []DeviceSnapshot{},
	}
	if h.collector != nil {
		snapshot = h.collector.Snapshot()
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		log.Printf("marshal dashboard snapshot: %v", err)
		return nil
	}
	return data
}

// Subscribe registers a buffered subscriber and queues the current snapshot first.
func (h *Hub) Subscribe() (<-chan []byte, func()) {
	sub := &subscriber{messages: make(chan []byte, subscriberBufferSize)}
	h.mu.Lock()
	if h.closed {
		close(sub.messages)
		h.mu.Unlock()
		return sub.messages, func() {}
	}
	h.subscribers[sub] = struct{}{}
	current := h.CurrentSnapshotJSON()
	if current != nil {
		sub.messages <- current
	}
	h.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			h.mu.Lock()
			if _, ok := h.subscribers[sub]; ok {
				delete(h.subscribers, sub)
				close(sub.messages)
			}
			h.mu.Unlock()
		})
	}
	return sub.messages, cancel
}

// Publish delivers a raw JSON message without waiting for any subscriber.
func (h *Hub) Publish(message []byte) {
	if len(message) == 0 {
		return
	}
	message = append([]byte(nil), message...)

	h.mu.Lock()
	defer h.mu.Unlock()
	for sub := range h.subscribers {
		select {
		case sub.messages <- message:
		default:
			delete(h.subscribers, sub)
			close(sub.messages)
		}
	}
}

// SubscriberCount returns the number of active subscribers.
func (h *Hub) SubscriberCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subscribers)
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
				h.Broadcast(h.collector.Snapshot())
			}
		}
	}
}

// Broadcast marshals a snapshot or event before publishing it to subscribers.
func (h *Hub) Broadcast(message any) {
	data, err := json.Marshal(message)
	if err != nil {
		log.Printf("marshal dashboard message: %v", err)
		return
	}
	h.Publish(data)
}

// ServeWS upgrades a dashboard request and streams its subscription.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// The dashboard is served to devices on the coordinator's LAN; origin checks
		// would reject legitimate phone-to-laptop connections with different origins.
		InsecureSkipVerify: true,
	})
	if err != nil {
		log.Printf("accept dashboard websocket: %v", err)
		return
	}
	defer conn.CloseNow()

	messages, unsubscribe := h.Subscribe()
	defer unsubscribe()

	ctx, cancel := context.WithCancel(r.Context())
	stopHubCancel := context.AfterFunc(h.ctx, cancel)
	defer func() {
		stopHubCancel()
		cancel()
	}()

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		pingTicker := time.NewTicker(15 * time.Second)
		defer pingTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case message, ok := <-messages:
				if !ok {
					cancel()
					return
				}
				writeCtx, writeCancel := context.WithTimeout(ctx, 3*time.Second)
				err := conn.Write(writeCtx, websocket.MessageText, message)
				writeCancel()
				if err != nil {
					cancel()
					return
				}
			case <-pingTicker.C:
				pingCtx, pingCancel := context.WithTimeout(ctx, 3*time.Second)
				err := conn.Ping(pingCtx)
				pingCancel()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()

	for {
		if _, _, err := conn.Read(ctx); err != nil {
			break
		}
	}
	cancel()
	<-writerDone
}

// HandleWS is an alias for ServeWS.
func (h *Hub) HandleWS(w http.ResponseWriter, r *http.Request) {
	h.ServeWS(w, r)
}
