package dashhttp

import (
	"encoding/json"
	"net/http"

	"github.com/scrap-heap/backend/internal/metrics"
)

// RegisterDash registers the dashboard WebSocket, CSV export, and run-again endpoints.
func RegisterDash(mux *http.ServeMux, hub *metrics.Hub) {
	mux.HandleFunc("/ws/dash", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if hub == nil {
			writeJSONError(w, http.StatusInternalServerError, "metrics hub not initialized")
			return
		}
		hub.ServeWS(w, r)
	})

	mux.HandleFunc("/api/export.csv", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		csvData := metrics.CSVHeader + "\n"
		if hub != nil {
			csvData = hub.ExportCSV()
		}
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", `attachment; filename="scrapheap-results.csv"`)
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(csvData)); err != nil {
			return
		}
	})

	mux.HandleFunc("/api/run-again", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if hub == nil || hub.OnRunAgain == nil {
			writeJSONError(w, http.StatusNotImplemented, "run-again not wired")
			return
		}

		hub.OnRunAgain()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	})
}
