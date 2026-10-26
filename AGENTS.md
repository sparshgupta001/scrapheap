# Scrap Heap: backend context

Project: a browser-based distributed compute cluster for a 24-hour hackathon.
Devices (laptop + phones) join by scanning a QR code, open a web page, and render
tiles of a Mandelbrot image from scratch. The coordinator is a single Go server.
Goal: real computation, measurable speedup vs laptop-only, fault tolerance.

My scope: the Go backend only (server, protocol, scheduler, metrics, QR, dashboard feed).
Others own: the WASM kernel, the worker JS page, the dashboard UI.

Architecture
- Star topology: one Go binary on the coordinator laptop. HTTP + WebSocket (binary frames).
- Go 1.22+, stdlib plus github.com/coder/websocket and github.com/skip2/go-qrcode only.
  Ask before adding any other dependency.
- Canvas 2048x2048, 64x64 tiles = 1,024 tasks. Task = render parameters only
  (x0, y0, scale, w, h, maxIter). Result = tile RGBA pixels (w*h*4 bytes).
- Pull-based scheduling: worker threads send REQUEST_TASK; server answers with a tile.
- Task states: QUEUED -> LEASED -> DONE; LEASED -> QUEUED on lease expiry or disconnect.
- Lease timeout ~3x expected tile time (estimated cost / worker speed), floor ~2s.
- Results keyed by (taskId, attempt); first valid result wins, duplicates dropped.
- End-game: when the queue is empty but tasks are in flight, reissue stragglers to idle workers.
- Capability-aware assignment (later step): heavy and light queues sorted by estimated
  cost; strong devices prefer heavy tiles, weak devices prefer light, with fallback.
- Dashboard gets a TILE_DONE event {taskId, deviceId, computeMs, attempt} over its own WebSocket.

Code layout
  cmd/server/            main, HTTP, WS wiring, embed web/
  internal/proto/        binary frame + JSON message encode/decode (shared contract)
  internal/sched/        PURE state machine, no networking, no goroutines of its own
                         except one owner loop; fully unit-testable with a fake clock
  internal/kernel/       owned by another teammate; depend only on the interface in
                         internal/kernel/kernel.go
  internal/metrics/      counters, per-device stats, CSV export
  web/                   owned by others; do not edit

Rules
- Development is on Windows (repo at G:\Neverland). Do not depend on `make`; provide
  plain `go` commands and, if helpful, a scripts/*.ps1 helper.
- Do not change anything outside the files the current task names.
- No simulation shortcuts in production code. Fake workers are allowed ONLY in tests/tools.
- Keep it simple; no frameworks, no abstractions we don't need today.
- Run `go vet ./...` and `go test ./... -race` before declaring done.
  (If -race is unavailable on this machine, say so in the report and run without it.)
- After EVERY task, follow the Report Protocol below.

Report Protocol
After finishing a task, append a section to docs/PROGRESS.md using exactly this template:

## <task name> (<date/time>)
- What was built: 2-5 bullets
- Files created/changed: list with one-line purpose each
- Public interfaces: exported types, function signatures, message formats, endpoints
- How to run/test: exact commands, and the actual results you saw (pass/fail counts)
- Decisions & deviations from this spec, and why
- Known issues / TODOs / things you were unsure about
- What the next task depends on
Do not claim anything works unless you ran it. If something failed, say so plainly.
