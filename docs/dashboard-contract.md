# Scrap Heap Dashboard Data Contract

The browser-based workers render a Mandelbrot fractal from scratch with WASM.
The canvas is 2048x2048 pixels, divided into 64x64 tiles for a 32x32 grid and
1,024 tasks. Tile IDs are row-major: `taskId = row * 32 + col`. Each result is
the 64x64 tile's 16,384 RGBA bytes plus its compute time. Tile cost varies
significantly between heavy interior regions and light exterior regions.

## WebSocket `/ws/dash`

The dashboard feed sends a JSON snapshot every 500 ms:

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

`tileStates` has one character per task: `Q` queued, `L` leased, `D` done, and
`R` requeued. The snapshot fields are fixed; tile image pixels and per-tile
metadata are fetched through the HTTP endpoints below.

### Event messages (pushed on the same WebSocket)

```json
{ "type": "device_joined", "id": "d2", "label": "Pixel 7", "t": 20.1 }
{ "type": "device_left", "id": "d2", "t": 31.4 }
{ "type": "tile_requeued", "task": 57, "reason": "disconnect|lease", "t": 31.5 }
{ "type": "tile_done", "task": 57, "device": "d2", "computeMs": 312, "attempt": 1, "t": 31.8 }
{ "type": "job_done", "t": 62.0, "finalChecksum": "abc123" }
```

The server's `TILE_DONE {taskId, deviceId, computeMs, attempt}` notification is
mapped to the dashboard event `tile_done` with `task`, `device`, `computeMs`,
`attempt`, and elapsed job time `t`.

### Worker HELLO

There is one worker WebSocket and one HELLO per **device**, not per thread.
HELLO fields are `deviceId`, `label`, `cores`, `userAgent`, and `threads`; there
is no `threadId`. The `label` and `threads` fields come from R3's worker HELLO;
R1 is adding them to the protocol.

- One HELLO creates one device bar; `threads` is the thread count shown there.
- If `label` is empty, derive a short readable name from `userAgent` (for
  example, "iPhone Safari", "Android Chrome", "Windows Chrome", or "Device").
- A reconnecting device reuses its existing entry, becomes active, retains
  `tilesDone`, and does not create a second bar.

## Tile HTTP APIs

### `GET /api/tile/{id}`

Returns the completed tile's raw RGBA bytes as `application/octet-stream`.
Valid IDs are integers from 0 through 1023. An invalid or out-of-range ID
returns 400; a tile without retrievable verified pixels returns 404. Successful
responses contain exactly 16,384 bytes and set `Cache-Control: no-store`.

### `GET /api/tiles`

Returns a JSON array of metadata for completed tiles. This is used to populate
tiles when the dashboard connects or reloads mid-run:

```json
[
  { "task": 57, "device": "d2", "computeMs": 312, "attempts": 1 }
]
```

Metadata is also updated live from `tile_done` WebSocket events. Failed or
wrong-sized pixel data is not available from `/api/tile/{id}`.

The collector acceptance interface is:

```go
TaskAccepted(taskID int, deviceID string, verifiedOK bool, computeMs uint32, attempt uint32, rgba []byte)
```

The first accepted result for a task wins. RGBA bytes are retained only when
verification succeeds and the byte length is exactly 16,384.

## CSV Export (`GET /api/export.csv`)

One row per run, with this exact header:

```csv
run_id,config,devices,threads,wall_s,compute_s,tiles,tiles_per_sec,speedup_vs_B,efficiency,retries,dupes_dropped,verified_ok,verified_fail,bytes_in,bytes_out
```

The `config` column is `B` for a laptop-only baseline, otherwise `pull+N` or
`aware+N`, where `N` is the number of phones in that run.

- `speedup_vs_B` = `baselineB_wall_s / run_wall_s`
- `efficiency` = `speedup / devices`
- Leave both values empty when no run is marked as baseline `B`.
