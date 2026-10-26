package main

import (
	"net/http"

	"github.com/scrap-heap/backend/internal/dashhttp"
	"github.com/scrap-heap/backend/internal/metrics"
)

// RegisterDash registers the dashboard WebSocket, CSV export, and run-again endpoints.
func RegisterDash(mux *http.ServeMux, hub *metrics.Hub) {
	dashhttp.RegisterDash(mux, hub)
}
