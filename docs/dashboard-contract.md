# Scrap Heap Dashboard Data Contract

## WebSocket `/ws/dash`

Pushed over WebSocket `/ws/dash` every 500 ms:
```json
{
  "type": "snapshot",
  "t": 12.5,
  "job": "idle|running|done",
  "jobId": "run-3",
  "done": 140,
  "total": 1024,
  "tilesPerSec": 11.2,
  "devices": [
    {
      "id": "d1",
      "label": "Laptop",
      "threads": 6,
      "tilesDone": 88,
      "tilesPerSec": 7.1,
      "state": "active|idle|gone"
    }
  ],
  "retries": 3,
  "dupesDropped": 1,
  "verifiedOK": 140,
  "verifiedFail": 0,
  "tileStates": "DDDDLLQQQRQ..."
}
```

### Event Messages (pushed on the same WebSocket)

```json
{ "type": "device_joined", "id": "d2", "label": "Pixel 7", "t": 20.1 }
{ "type": "device_left",   "id": "d2", "t": 31.4 }
{ "type": "tile_requeued", "task": 57, "reason": "disconnect|lease", "t": 31.5 }
{ "type": "job_done",      "t": 62.0, "finalChecksum": "abc123" }
```

### Worker HELLO (from R3, informational: R1 maps it into DeviceJoined)
ONE WebSocket and ONE HELLO per DEVICE, not per thread.
Fields: `deviceId`, `label`, `cores`, `userAgent`, `threads`. There is NO `threadId`.
- One HELLO = one device bar. `threads` is the thread count shown on that bar.
- If `label` is empty, derive a short readable name from `userAgent` (e.g. "iPhone Safari", "Android Chrome", "Windows Chrome", else "Device").
- Reconnecting device (`deviceId` already seen) reuses that device: sets state back to "active", preserves `tilesDone`, does not create a second bar.

---

## CSV Export (`GET /api/export.csv`)

One row per run, exact header:
```csv
run_id,config,devices,threads,wall_s,compute_s,tiles,tiles_per_sec,speedup_vs_B,efficiency,retries,dupes_dropped,verified_ok,verified_fail,bytes_in,bytes_out
```

- `speedup_vs_B` = `baselineB_wall_s / run_wall_s`
- `efficiency` = `speedup / devices`
- Leave both empty if no run is marked as baseline "B".
