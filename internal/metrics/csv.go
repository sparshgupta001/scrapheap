package metrics

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"sort"
	"strconv"
)

// RunRecord represents all recorded metrics for a completed compute run.
type RunRecord struct {
	RunID        string  `json:"run_id"`
	Config       string  `json:"config"`
	Devices      int     `json:"devices"`
	Threads      int     `json:"threads"`
	WallS        float64 `json:"wall_s"`
	ComputeS     float64 `json:"compute_s"`
	Tiles        int     `json:"tiles"`
	TilesPerSec  float64 `json:"tiles_per_sec"`
	Retries      int     `json:"retries"`
	DupesDropped int     `json:"dupes_dropped"`
	VerifiedOK   int     `json:"verified_ok"`
	VerifiedFail int     `json:"verified_fail"`
	BytesIn      int64   `json:"bytes_in"`
	BytesOut     int64   `json:"bytes_out"`
}

// CSVHeader defines the exact comma-separated header row.
const CSVHeader = "run_id,config,devices,threads,wall_s,compute_s,tiles,tiles_per_sec,speedup_vs_B,efficiency,retries,dupes_dropped,verified_ok,verified_fail,bytes_in,bytes_out"

// FormatCSV formats a slice of RunRecords into CSV according to the contract.
func FormatCSV(runs []RunRecord) string {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	// Write exact header
	header := []string{
		"run_id", "config", "devices", "threads", "wall_s", "compute_s",
		"tiles", "tiles_per_sec", "speedup_vs_B", "efficiency",
		"retries", "dupes_dropped", "verified_ok", "verified_fail",
		"bytes_in", "bytes_out",
	}
	_ = writer.Write(header)

	// Check if any run is marked as baseline "B"
	var baselineWallS float64
	hasBaseline := false
	for _, r := range runs {
		if r.Config == "B" && r.WallS > 0 {
			baselineWallS = r.WallS
			hasBaseline = true
			break
		}
	}

	orderedRuns := append([]RunRecord(nil), runs...)
	sort.SliceStable(orderedRuns, func(i, j int) bool {
		return orderedRuns[i].Config == "B" && orderedRuns[j].Config != "B"
	})

	for _, r := range orderedRuns {
		speedupStr := ""
		efficiencyStr := ""

		if hasBaseline && r.WallS > 0 {
			speedup := baselineWallS / r.WallS
			speedupStr = fmt.Sprintf("%.2f", speedup)
			if r.Devices > 0 {
				efficiency := speedup / float64(r.Devices)
				efficiencyStr = fmt.Sprintf("%.2f", efficiency)
			}
		}

		row := []string{
			r.RunID,
			r.Config,
			strconv.Itoa(r.Devices),
			strconv.Itoa(r.Threads),
			fmt.Sprintf("%.2f", r.WallS),
			fmt.Sprintf("%.2f", r.ComputeS),
			strconv.Itoa(r.Tiles),
			fmt.Sprintf("%.2f", r.TilesPerSec),
			speedupStr,
			efficiencyStr,
			strconv.Itoa(r.Retries),
			strconv.Itoa(r.DupesDropped),
			strconv.Itoa(r.VerifiedOK),
			strconv.Itoa(r.VerifiedFail),
			strconv.FormatInt(r.BytesIn, 10),
			strconv.FormatInt(r.BytesOut, 10),
		}
		_ = writer.Write(row)
	}

	writer.Flush()
	return buf.String()
}

// ExportCSV exports all runs recorded in the collector as CSV text.
func (c *Collector) ExportCSV() string {
	return FormatCSV(c.GetRuns())
}
