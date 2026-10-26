package dashhttp

import (
	"encoding/json"
	"net/http"

	"github.com/scrap-heap/backend/internal/metrics"
)

// RegisterDash registers the WebSocket, CSV export, and run-again endpoints.
func RegisterDash(mux *http.ServeMux, hub *metrics.Hub) {
	mux.HandleFunc("/ws/dash", func(w http.ResponseWriter, r *http.Request) {
		if hub != nil {
			hub.ServeWS(w, r)
		} else {
			http.Error(w, "metrics hub not initialized", http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("/api/export.csv", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var csvData string
		if hub != nil {
			csvData = hub.ExportCSV()
		} else {
			csvData = metrics.CSVHeader + "\n"
		}

		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition", `attachment; filename="scrapheap-results.csv"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(csvData))
	})

	mux.HandleFunc("/api/run-again", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if hub == nil || hub.OnRunAgain == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotImplemented)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "run-again not implemented",
			})
			return
		}

		go hub.OnRunAgain()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]bool{
			"ok": true,
		})
	})
}
