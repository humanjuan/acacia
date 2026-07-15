# Acacia

Asynchronous, low-allocation Go logger with typed structured JSON, explicit backpressure, safe filesystem handling, redaction, rotation, and documented Sync/Close semantics.

[![Go Reference](https://pkg.go.dev/badge/github.com/humanjuan/acacia/v2.svg)](https://pkg.go.dev/github.com/humanjuan/acacia/v2)

---

## Why Acacia

Acacia was designed to provide a small, highly controlled, and auditable logger for services in the HumanJuan/Golyn Go ecosystem. It exists to solve specific engineering requirements where the following properties matter:
* **Predictable Latency & Allocations:** Reusable buffer pools to control garbage collection pressure.
* **Congestion Behavior:** Explicit policies for handling full queues rather than silent memory expansion.
* **Sensitive Data Security:** Built-in and explicit redaction of credentials in structured formats.
* **Safe Lifecycles:** Clean and idempotent shutdown semantics for flushing accepted events during controlled shutdown.
* **Verifiable Documentation:** Every major change is backed by documented tests and reproducible benchmarks.

---

## Capabilities at a Glance

- **Async File Logging** with a single-writer pipeline to minimize writer thread contention.
- **Direct `string` and `[]byte` hot paths** to bypass serialization overhead.
- **`io.Writer` support** for libraries and frameworks that accept writers.
- **Standard library `log.Logger` compatibility** through `log.New`.
- **Typed structured JSON builder** for reflection-free event construction.
- **Nested `Object` and `Array` builders** using sequentially processed callbacks.
- **`Group`** for nested JSON domains and **`Namespace`** for flat prefixed keys.
- **`RawJSON`** for trusted, pre-serialized JSON payloads.
- **`Any`** as an explicit dynamic reflection-based slow fallback.
- **Legacy `map[string]interface{}`** compatibility path.
- **Configurable backpressure policies** (`DropNewest`, `BlockOnFull`, `DropOldest`, `DropDebugOnly`).
- **Rotation support** based on size (MB) and time (daily).
- **Secure filesystem defaults** (`0600` file modes and `0700` directory modes).
- **Sensitive data redaction** via key-pattern matching and explicit `.Secret()` / `.StrRedacted()`.
- **Documented `Sync()` and `Close()`** lifecycle barriers.

---

## Installation

```bash
go get github.com/humanjuan/acacia/v2
```

---

## Quick Start

```go
package main

import (
    acacia "github.com/humanjuan/acacia/v2"
)

func main() {
    // Create the logger (parent directory must exist)
    log, err := acacia.Start("app.log", "./logs", acacia.Level.INFO)
    if err != nil {
        panic(err)
    }
    // Drains the queue and flushes buffer to disk safely on exit
    defer log.Close()

    log.InfoEvent().
        Str("service", "billing").
        Int("attempt", 3).
        Bool("cached", false).
        Msg("payment request processed")
}
```

---

## Write Paths

### Plain string logs
For simple raw string outputs without structured formatting overhead:
```go
log.Info("service started")
log.Warn("cache miss")
log.Error("payment failed")
```

### Direct `[]byte` logs
Useful when the payload is already formatted and should avoid extra encoding work:
```go
payload := []byte(`{"event":"cache_hit","key":"users:42"}`)
log.InfoBytes(payload)
```

### `io.Writer` compatibility
Acacia implements `io.Writer`, allowing integration with external frameworks that accept standard writers:
```go
_, _ = acaciaLog.Write([]byte("framework emitted this line\n"))
```

### Standard library `log.Logger` compatibility
Wrap Acacia with the Go standard library logger to support legacy packages:
```go
import (
    "log"
    acacia "github.com/humanjuan/acacia/v2"
)

acaciaLog, _ := acacia.Start("app.log", "./logs", acacia.Level.INFO)
stdlibLog := log.New(acaciaLog, "http: ", log.LstdFlags)

stdlibLog.Println("server started")
```
*This is useful for frameworks and libraries that accept an `io.Writer` or a standard `*log.Logger`.*

### Typed structured JSON builder
The fluent event builder writes keys and values sequentially to the buffer:
```go
acaciaLog.InfoEvent().
    Str("service", "billing").
    Int("status", 200).
    Bool("cached", false).
    Msg("request completed")
```

### Nested objects and arrays
Construct multi-level JSON shapes reflection-free:
```go
acaciaLog.InfoEvent().
    Object("request", func(o *acacia.Object) {
        o.Str("method", "POST")
        o.Str("path", "/v1/orders")
        o.Int("status", 201)
    }).
    Array("items", func(a *acacia.Array) {
        a.Object(func(o *acacia.Object) {
            o.Str("sku", "book-001")
            o.Int("qty", 2)
        })
    }).
    Msg("order created")
```

### Group and Namespace
Prefix fields dynamically to avoid key collision:
```go
// Group emits nested JSON: {"http": {"method": "GET", "status": 200}}
acaciaLog.InfoEvent().
    Group("http", func(g *acacia.Object) {
        g.Str("method", "GET")
        g.Int("status", 200)
    }).
    Msg("request completed")

// Namespace emits flat prefixed keys: {"http.method": "GET", "http.status": 200}
acaciaLog.InfoEvent().
    Namespace("http", func(n *acacia.Namespace) {
        n.Str("method", "GET")
        n.Int("status", 200)
    }).
    Msg("request completed")
```

### RawJSON
Inject pre-serialized JSON byte slices directly:
```go
raw := []byte(`{"provider":"internal","score":98}`)
acaciaLog.InfoEvent().
    RawJSON("payload", raw).
    Msg("provider response")
```
> [!WARNING]
> `RawJSON` trusts the caller's payload to contain valid JSON and does not perform internal key redaction.

### Any fallback
Dynamic formatting path that inspects arbitrary values (structs, maps, slices) via reflection:
```go
acaciaLog.InfoEvent().
    Any("payload", someStruct).
    Msg("debug payload")
```
> [!WARNING]
> `Any` is useful for dynamic or low-frequency debugging payloads, but is not the recommended hot path.

### Legacy map compatibility
Support for map structures:
```go
acaciaLog.Info(map[string]interface{}{
    "event": "legacy_event",
    "status": 200,
})
```
*Kept for v2.x compatibility; use typed builders for performance-critical paths.*

---

## Configuration

### Buffering and flush interval
Customize buffer sizes and flush schedules to align latency with I/O patterns:
```go
acaciaLog, err := acacia.Start(
    "app.log",
    "./logs",
    acacia.Level.INFO,
    acacia.WithBufferSize(64<<10),               // 64KB write channel queue
    acacia.WithBufferCap(512<<10),               // 512KB internal bytes buffer
    acacia.WithFlushInterval(100*time.Millisecond), // Flush scheduled every 100ms
)
```

### Backpressure policies
Define how the logger behaves when the asynchronous write queue is saturated:
```go
acaciaLog, err := acacia.Start(
    "app.log",
    "./logs",
    acacia.Level.INFO,
    acacia.WithBackpressurePolicy(acacia.DropNewest),
)
```
Supported policies:
* `BlockOnFull` (default): Blocks caller goroutines until queue capacity is available, avoiding intentional drops under transient saturation.
* `DropNewest`: Discards incoming logs immediately when the queue is full.
* `DropOldest`: Discards the oldest log in the queue to write the new one.
* `DropDebugOnly`: Drops `DEBUG` and `INFO` logs when saturated, while blocking on critical/warn levels to prevent administrative record losses.

### Rotation by size
Configure file size limits and backup retention on the active log file:
```go
// Limit log file to 100 MB, retaining up to 5 historical backup files
acaciaLog.Rotation(100, 5)
```

### Daily rotation
Enable daily time-based rotation:
```go
acaciaLog.DailyRotation(true)
```

### File and directory permissions
Acacia defaults to secure owner-restricted filesystem access, but allows override:
```go
acaciaLog, err := acacia.Start(
    "app.log",
    "./logs",
    acacia.Level.INFO,
    acacia.WithFileMode(0600), // restrict log files to owner read/write
    acacia.WithDirMode(0700),  // restrict created directories to owner read/write/traverse
)
```

### Sync and Close
lifecycle methods enforce explicit synchronization barriers:
```go
defer acaciaLog.Close()

// Force a persistence barrier at critical checkpoints
acaciaLog.Sync()
```
* `Close()` flushes accepted/enqueued events according to the documented Close semantics before shutting down.
* `Sync()` waits for currently accepted events according to the documented Sync semantics and attempts to flush/sync them to disk.

---

## Security

### JSON and log injection safety
The append-only encoder automatically escapes control characters (`\x00` - `\x1f`), carriage returns, and line feeds, ensuring log forgery attempts cannot corrupt SIEM parses.

### Sensitive data redaction
Keys matching known sensitive patterns (e.g. `password`, `token`, `secret`, `authorization_header`) are redacted automatically. Explicit fields can be secured inline using `Secret()` or `StrRedacted()`:
```go
acaciaLog.InfoEvent().
    Str("user", "juan").
    Str("password", "plain-text-value"). // auto-redacted based on key name
    Secret("api_key", token).            // explicitly redacted regardless of key name
    Msg("login attempt")
```
> [!NOTE]
> Redaction is robust under documented test suites; it does not represent a mathematical guarantee of perfect redaction over arbitrary custom struct pointer graphs.

### Filesystem safety
Log filenames are validated to block directory traversal patterns (`../`) and ensure log writes reside strictly within the base directory.

### Symlink protection
Acacia runs `os.Lstat` checks on directories and active files before writing or rotation to prevent symlink hijacking attacks.

### Private/public evidence boundary
Raw performance outputs, local pprof profiles, and fuzz corpora belong in `.private/`, which is excluded from git versioning, keeping the public repository clean.

---

## Performance Snapshot

These numbers are observations from the Final SSD Hard Validation run on one local darwin/amd64 SSD environment. They are useful for comparison and regression review, not as universal performance guarantees.

| Path | What it is for | Observed Result (darwin/amd64) |
| :--- | :--- | :--- |
| **Direct `[]byte`** | Already-formatted payloads | 181.0 ns/op \| 0 B/op \| 0 allocs/op |
| **Direct `string`** | Simple text logs | 183.7 ns/op \| 0 B/op \| 0 allocs/op |
| **Direct `[]byte` 1KB** | Medium payload bytes | 926.3 ns/op \| 0 B/op \| 0 allocs/op |
| **Direct `string` 1KB** | Medium text payload | 937.2 ns/op \| 16 B/op \| 1 alloc/op |
| **Flat JSON Builder** | Typed structured event | 522.0 ns/op \| 29 B/op \| 1 alloc/op |
| **Object Builder** | Nested typed object | 611.6 ns/op \| 42 B/op \| 2 allocs/op |
| **RawJSON** | Trusted pre-serialized JSON | 539.9 ns/op \| 26 B/op \| 1 alloc/op |
| **Any** | Dynamic fallback | 3229.7 ns/op \| 1635 B/op \| 25 allocs/op |
| **Disabled Level** | Short-circuit path | 13.4 ns/op \| 0 B/op \| 0 allocs/op |
| **Legacy Map Structured Baseline** | Compatibility path | 1126.1 ns/op \| 323 B/op \| 2 allocs/op |

*The validation suite also includes benchmarks and tests for redaction, backpressure, filesystem safety, rotation, Sync/Close behavior and post-close lifecycle paths.*

---

## OpenTelemetry Compatibility

Acacia does not ship a native OpenTelemetry adapter in this release candidate.

The current integration model is:
- Emit structured JSON logs that can be collected by an external agent or log shipper.
- Use `io.Writer` / standard `log.Logger` compatibility where frameworks expose writer-based logging hooks.
- Keep trace/span identifiers as normal structured fields when your application already has them.
- Keep a future OTel adapter as optional, outside the core hot path.

*This keeps the core logger small and avoids adding mandatory OpenTelemetry dependencies to the main module.*

---

## When to Use Acacia

* **High-concurrency file logging:** Highly multi-threaded environments seeking to minimize lock contention.
* **Saturated/Memory-constrained environments:** Resource-limited containers requiring strict queue boundaries and deterministic drop behaviors.
* **Security audits:** Systems requiring native, reflection-free sensitive key redaction and path traversal protections.

---

## When Not to Use Acacia

* **Native OTel tracing exports needed:** Systems that require direct, native export to OpenTelemetry collectors without external shippers.
* **Remote sink logging:** Writing directly to remote network sockets (e.g. Elasticsearch, Loki) without a local sidecar agent.
* **Infinite queue memory:** When memory consumption limits are not a concern and queues are expected to grow dynamically under saturation.

---

## Validation and Reports

Detailed guides, plan contexts, and validation results are structured as follows:
- [Quality and Validation](docs/QUALITY_AND_VALIDATION.md)
- [Migration Strategy](docs/MIGRATION_STRATEGY.md)
- [Structured JSON Builder Guide](docs/guides/structured-json-builder.md)
- [Redaction Guide](docs/guides/structured-json-redaction.md)
- [Final SSD Hard Validation Report](docs/reports/final-ssd-hard-validation-report.md)
- [Release Readiness Runbook](docs/runbooks/release-readiness-runbook.md)

---

## Reproducing Benchmarks

To reproduce the benchmark suite under default and GOMAXPROCS limits, run:

```bash
# Default GOMAXPROCS
go test ./test -bench 'Benchmark(byte|string|structured|EventBuilder|JSONEncoder|Redaction|Backpressure|ValidateLogFilename|SafeJoinLogPath|Rotation|Sync|Close|WriteAfterClose|ConcurrentSync)' -benchmem -run '^$' -count=10

# Single-core GOMAXPROCS
GOMAXPROCS=1 go test ./test -bench 'Benchmark(byte|string|structured|EventBuilder|JSONEncoder|Redaction|Backpressure|ValidateLogFilename|SafeJoinLogPath|Rotation|Sync|Close|WriteAfterClose|ConcurrentSync)' -benchmem -run '^$' -count=10

# Dual-core GOMAXPROCS
GOMAXPROCS=2 go test ./test -bench 'Benchmark(byte|string|structured|EventBuilder|JSONEncoder|Redaction|Backpressure|ValidateLogFilename|SafeJoinLogPath|Rotation|Sync|Close|WriteAfterClose|ConcurrentSync)' -benchmem -run '^$' -count=10
```

---

## License

Acacia is a part of the HumanJuan Go ecosystem. Licensed under the **MIT License**.
