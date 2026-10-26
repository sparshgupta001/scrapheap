# Scrap Heap backend

Go backend for the Scrap Heap distributed Mandelbrot renderer. The protocol
package is available; coordinator HTTP/WebSocket service and worker UI are
future tasks.

## Requirements

- Go 1.22 or newer
- Windows PowerShell

## Build and verify

```powershell
go fmt ./...
go vet ./...
go build ./...
go test ./...
```

## Worker contract

Workers connect to /ws/worker; the dashboard connects to /ws/dash. The shared
contract defines 48-byte TASK and 20-byte RESULT headers, row-major RGBA output,
job lifecycle behavior, and the `process_tile` kernel signature. See
[docs/protocol.md](docs/protocol.md) for field offsets, pixel mapping, and the
fixed benchmark parameters.

## Run

The HTTP/WebSocket coordinator is not implemented yet. When it is added, start it
from the repository root with:

```powershell
go run ./cmd/server
```
