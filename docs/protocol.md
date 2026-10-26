# Worker and dashboard protocol

This document is the shared source of truth for the Go backend and worker client.
All multi-byte integers, float32 values, and float64 values in binary frames are
little-endian. There is no implicit padding or alignment. Binary offsets are
zero-based from the start of a WebSocket message.

## Connections and framing

- Workers connect to `/ws/worker`.
- The dashboard connects to `/ws/dash`.
- WebSocket text frames contain UTF-8 JSON messages.
- WebSocket binary frames contain TASK or RESULT frames.

Unknown extra JSON fields are ignored. Unknown JSON `type` values are rejected
with `UnknownMessageTypeError`.

## JSON messages

Every message has a case-sensitive `type` string. Fields below are case-sensitive.

| Type | Direction | Fields | Example |
| --- | --- | --- | --- |
| `HELLO` | worker to server | `deviceId`: non-empty string; `userAgent`: string; `cores`: uint32 > 0; `threads`: int 1..16 (Web Worker count); `mock`: bool, defaults to false | `{"type":"HELLO","deviceId":"phone-1","userAgent":"Example Browser","cores":4,"threads":2,"mock":false}` |
| `BENCH_RESULT` | worker to server | `tilesPerSec`: finite number > 0; `perThread`: finite number > 0; `threads`: int 1..16; `hash`: 64 hexadecimal SHA-256 characters | `{"type":"BENCH_RESULT","tilesPerSec":12.5,"perThread":6.25,"threads":2,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}` |
| `REQUEST_TASK` | worker to server | none | `{"type":"REQUEST_TASK"}` |
| `HEARTBEAT` | worker to server | none | `{"type":"HEARTBEAT"}` |
| `ACK` | worker to server | `jobId`: uint32; `taskId`: uint32; `attempt`: uint16 > 0 | `{"type":"ACK","jobId":42,"taskId":7,"attempt":1}` |
| `JOB_DONE` | worker to server | `jobId`: uint32 | `{"type":"JOB_DONE","jobId":42}` |
| `WELCOME` | server to worker | `heartbeatIntervalMs`: uint32 > 0; `heartbeatTimeoutMs`: uint32 > 0 | `{"type":"WELCOME","heartbeatIntervalMs":1000,"heartbeatTimeoutMs":5000}` |
| `JOB_STARTED` | server to all connected workers | `jobId`: uint32 | `{"type":"JOB_STARTED","jobId":42}` |

HELLO's omitted `mock` value is false. Devices with `mock: true` are tagged as
mock devices and excluded from benchmark exports. A valid HELLO includes
`threads` from 1 through 16. BENCH_RESULT carries the measured throughput for
all threads and the per-thread throughput, plus the hash of the benchmark tile's
RGBA bytes.

The server sends WELCOME after accepting HELLO. Whenever a job starts or resets,
it increments `jobId` and broadcasts JOB_STARTED to connected workers. Results
whose `jobId` is not the current job are dropped.

On JOB_DONE or reset, the server drops all held REQUEST_TASKs. Workers send a new
REQUEST_TASK after receiving JOB_STARTED, or after WELCOME if a job is already
running. A device may have at most `threads` outstanding requests plus leased
tasks combined; extra REQUEST_TASKs are ignored. No prefetch is done by default.
A 1-second timer after JOB_DONE is an acceptable client fallback for requesting
again.

## Shared render contract

Canvas size is `CanvasSize = 2048` and standard tile size is `TileSize = 64`.
`MaxTileDim = 256` is the protocol's maximum width or height for a tile.

For pixel `(px, py)` in a tile, compute its point independently as:

```text
(x0 + float64(px)*scale, y0 + float64(py)*scale)
```

Do this calculation per pixel; never accumulate `scale` in a loop. The server
derives each tile's `x0` and `y0` from the canvas origin and the tile's `x` and
`y` pixel offsets using the same rule:

```text
tileX0 = canvasX0 + float64(x)*canvasScale
tileY0 = canvasY0 + float64(y)*canvasScale
```

The kernel has no input pixels, halo, or edge padding. It fills a caller-provided
RGBA buffer:

```text
process_tile(out Uint8Array, x0, y0, scale, w, h, maxIter)
```

`out` has exactly `w*h*4` bytes. Pixels are row-major, beginning at the tile's
top-left; each pixel contains R, G, B, A bytes.

The benchmark parameters are exported in `internal/proto` as constants and
returned by `proto.BenchTile()`:

| Parameter | Exact value |
| --- | ---: |
| x0 | -0.75 |
| y0 | 0.1 |
| scale | 0.005 |
| w | 64 |
| h | 64 |
| maxIter | 256 |

This tile covers a Mandelbrot boundary-detail region. BENCH_RESULT.hash is the
hex SHA-256 digest of the exact 16,384 RGBA bytes produced for this tile, with no
metadata included in the digest.

## Binary frame common rules

Byte 0 is the frame type (`0x01` TASK, `0x02` RESULT). Byte 1 is reserved: the
encoder always writes zero and the decoder ignores its value. Attempts are
uint16 and must be nonzero. Tile dimensions must be in 1..256. RESULT payload
length must equal exactly `w*h*4`; no missing or trailing bytes are accepted.

## TASK (48 bytes, server to worker, no pixels)

| Offset | Size (bytes) | Field | Type / encoding |
| ---: | ---: | --- | --- |
| 0 | 1 | type | uint8, value `1` |
| 1 | 1 | reserved | uint8, write zero; ignore on read |
| 2 | 2 | attempt | uint16, little-endian, > 0 |
| 4 | 4 | jobId | uint32, little-endian |
| 8 | 4 | taskId | uint32, little-endian |
| 12 | 2 | x | uint16, little-endian |
| 14 | 2 | y | uint16, little-endian |
| 16 | 2 | w | uint16, little-endian, 1..256 |
| 18 | 2 | h | uint16, little-endian, 1..256 |
| 20 | 8 | x0 | IEEE-754 float64, little-endian, finite |
| 28 | 8 | y0 | IEEE-754 float64, little-endian, finite |
| 36 | 8 | scale | IEEE-754 float64, little-endian, finite and > 0 |
| 44 | 4 | maxIter | uint32, little-endian, > 0 |

Example TASK (jobId 42, taskId `0x01020304`, attempt 2, x 10, y 20, w 2,
h 1, x0 -2, y0 0.5, scale 1, maxIter 256):

```text
01 00 02 00 2a 00 00 00 04 03 02 01 0a 00 14 00
02 00 01 00 00 00 00 00 00 00 00 c0
00 00 00 00 00 00 e0 3f 00 00 00 00 00 00 f0 3f
00 01 00 00
```

## RESULT (20-byte header plus RGBA pixels, worker to server)

| Offset | Size (bytes) | Field | Type / encoding |
| ---: | ---: | --- | --- |
| 0 | 1 | type | uint8, value `2` |
| 1 | 1 | reserved | uint8, write zero; ignore on read |
| 2 | 2 | attempt | uint16, little-endian, > 0 |
| 4 | 4 | jobId | uint32, little-endian |
| 8 | 4 | taskId | uint32, little-endian |
| 12 | 2 | w | uint16, little-endian, 1..256 |
| 14 | 2 | h | uint16, little-endian, 1..256 |
| 16 | 4 | computeMs | IEEE-754 float32, little-endian, finite and >= 0 |
| 20 | `w*h*4` | rgba | row-major RGBA bytes, from the tile's top-left |

A 64x64 RESULT is exactly 16,404 bytes: 20-byte header plus 16,384 pixel bytes.

Example RESULT (jobId 42, taskId `0x01020304`, attempt 2, w 2, h 1,
computeMs 300, pixels `00 01 02 ff` and `10 11 12 ff`):

```text
02 00 02 00 2a 00 00 00 04 03 02 01 02 00 01 00 00 00 96 43
00 01 02 ff 10 11 12 ff
```
