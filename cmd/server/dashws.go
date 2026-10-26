package main

import (
	"net/http"

	"github.com/sparshgupta001/scrapheap/internal/dashhttp"
	"github.com/sparshgupta001/scrapheap/internal/metrics"
)

// RegisterDash registers the dashboard WebSocket, CSV export, and run-again endpoints.
func RegisterDash(mux *http.ServeMux, hub *metrics.Hub) {
	dashhttp.RegisterDash(mux, hub)
}
