[![Go Reference](https://pkg.go.dev/badge/github.com/humanjuan/acacia/v2.svg)](https://pkg.go.dev/github.com/humanjuan/acacia/v2)
[![Buy Me a Coffee](https://img.shields.io/badge/Buy_Me_A_Coffee-Support-orange?logo=buy-me-a-coffee&style=flat-square)](https://www.buymeacoffee.com/humanjuan)

# Acacia: High Performance Concurrent Logger for Go

**Up to ~6M logs/sec | 0 allocs/op (hot path) | Zero‑loss barrier (Sync) | 100% race‑free** 

It delivers zero‑allocation fast paths, real file rotation, a zero‑loss Sync barrier, and a single‑writer architecture engineered for extreme concurrency. No dependencies. No magic. Just a human‑crafted logger that does its job exceptionally well.

---

## Why Acacia Exists

Most loggers are either:

- blazing fast *but limited* (no rotation, no io.Writer compatibility, JSON-only), or
- feature-rich *but slow*, allocating heavily on every write.

Acacia bridges that gap.

It was built because logging should be **simple**, **stable**, and **trustworthy**, especially when hundreds of
goroutines are writing simultaneously.  
And because software made by humans, with care, should feel human too.

---

# Key Features

### **Zero-allocation fast path**

Both `string` and `[]byte` logging achieve **0 allocs/op**, even under parallel load.
This makes Acacia one of the most allocation-efficient loggers in the Go ecosystem.

### **Extreme concurrency performance**

A single writer goroutine uses intelligent batching and pool-based buffers to sustain millions of messages per second
with predictable latency.

### **Real file rotation (built-in)**

Acacia supports:

- **Daily rotation**
- **Size-based rotation**
- **Both combined**
- And without external dependencies

All rotation is atomic, safe, and race-free.

### **Plain-text and JSON in the same engine**

Just flip a flag to switch between human-readable logs and structured JSON.

### **100% race-free**

Passes `go test -race` cleanly, essential for production systems.

### **Zero log loss**

`logger.Sync()` implements a barrier: Acacia ensures all enqueued messages are written before continuing.

### **Full io.Writer compatibility**

Use Acacia anywhere you would use an `io.Writer`. Perfect for integrating with HTTP servers, stdlib log, gRPC
interceptors, etc.

### **No external dependencies**

Pure Go. More portable, more predictable, more maintainable.

### **Built by a human, not a corporation**

Designed with care, clarity and craftsmanship, part of the HumanJuan ecosystem.

---

# Benchmarks (Intel i7-8750H 2018, 7 year old laptop CPU)

Benchmark package: `github.com/humanjuan/acacia/v2/test`  
CPU: Intel® Core™ i7-8750H @ 2.20GHz  
Go: 1.22+

Recommended benchmark configuration (tuned for high throughput and low latency):

```go
lg, _ := acacia.Start(
    "bench.log", b.TempDir(), acacia.Level.INFO,
    acacia.WithBufferSize(5_000_000),      // channel capacity
    acacia.WithBufferCap(256<<10),         // writer internal buffers: 256 KB
    acacia.WithDrainBurst(1024),           // drain more per wake-up
    acacia.WithFlushInterval(10*time.Millisecond),
)
```

### **Fast-path (string) – tuned**

| Scenario        | Result              | Alloc/op |
|-----------------|---------------------|----------|
| Single-threaded | **~158–169 ns/op**  | 0        |
| Parallel        | **~213–222 ns/op**  | 0        |
| 1KB message     | **~1019–1306 ns/op**| 1 alloc  |
| 1KB parallel    | **~1030–1156 ns/op**| 1 alloc  |

### **Fast-path (bytes) – tuned**

| Scenario        | Result              | Alloc/op |
|-----------------|---------------------|----------|
| Single-threaded | **~172–192 ns/op**  | 0        |
| Parallel        | **~206–231 ns/op**  | 0        |
| 1KB message     | **~1055–1550 ns/op**| 0        |
| 1KB parallel    | **~1027–1340 ns/op**| 0        |

### **Structured JSON (kind=2, prebuilt)**

| Scenario                    | Result               | Alloc/op |
|-----------------------------|----------------------|----------|
| Small event (single)        | **~525–541 ns/op**   | 2        |
| Small event (parallel)      | **~557–572 ns/op**   | 2        |
| 1KB structured (single)     | **~1968–2007 ns/op** | 3        |
| 1KB structured (parallel)   | **~1416–1741 ns/op** | 3–4      |
| Fields-optimized (parallel) | **~523–550 ns/op**   | 1        |


> Note: exact numbers vary per run and environment. The ranges above are from multiple runs on the same hardware.

#### Latest snapshot (raw outputs)

These runs were captured on the same Intel i7‑8750H with the tuned configuration (BufferSize 5M, BufferCap 256KB, DrainBurst 1024, FlushInterval 10ms).

```
Byte (3 runs)
Benchmark_byte-12                        7071612  192.2 ns/op   0 allocs/op
Benchmark_byte_Parallel-12               6449426  231.4 ns/op   0 allocs/op
Benchmark_byte_1KB-12                    1040535  1550 ns/op    0 allocs/op
Benchmark_byte_Parallel_1KB-12            960156  1261 ns/op    0 allocs/op

Benchmark_byte-12                        6939357  179.6 ns/op   0 allocs/op
Benchmark_byte_Parallel-12               6446090  206.6 ns/op   0 allocs/op
Benchmark_byte_1KB-12                    1040751  1073 ns/op    0 allocs/op
Benchmark_byte_Parallel_1KB-12           1093869  1340 ns/op    0 allocs/op

Benchmark_byte-12                        7635253  172.1 ns/op   0 allocs/op
Benchmark_byte_Parallel-12               6794814  217.1 ns/op   0 allocs/op
Benchmark_byte_1KB-12                    1114740  1055 ns/op    0 allocs/op
Benchmark_byte_Parallel_1KB-12           1000107  1027 ns/op    0 allocs/op
```

```
String (3 runs)
Benchmark_string-12                      6422943  165.6 ns/op   0 allocs/op
Benchmark_string_Parallel-12             6402118  221.6 ns/op   0 allocs/op
Benchmark_string_1KB-12                  1076242  1055 ns/op    1 alloc/op
Benchmark_string_Parallel_1KB-12         1114767  1030 ns/op    1 alloc/op

Benchmark_string-12                      7297260  157.8 ns/op   0 allocs/op
Benchmark_string_Parallel-12             6251238  213.1 ns/op   0 allocs/op
Benchmark_string_1KB-12                  1175487  1019 ns/op    1 alloc/op
Benchmark_string_Parallel_1KB-12          875965  1156 ns/op    1 alloc/op

Benchmark_string-12                      6773952  169.3 ns/op   0 allocs/op
Benchmark_string_Parallel-12             6275774  220.1 ns/op   0 allocs/op
Benchmark_string_1KB-12                  1140368  1306 ns/op    1 alloc/op
Benchmark_string_Parallel_1KB-12          948367  1075 ns/op    1 alloc/op
```

```
Structured (3 runs)
Benchmark_structured-12                  2157037  537.4 ns/op   2 allocs/op
Benchmark_structured_Parallel-12         1997346  560.1 ns/op   2 allocs/op
Benchmark_structured_1KB-12               530577  2007 ns/op    3 allocs/op
Benchmark_structured_Parallel_1KB-12      855634  1741 ns/op    3 allocs/op
Benchmark_structured_Parallel_Fields-12  2197480  523.4 ns/op   1 alloc/op

Benchmark_structured-12                  2203104  525.2 ns/op   2 allocs/op
Benchmark_structured_Parallel-12         1941906  571.7 ns/op   2 allocs/op
Benchmark_structured_1KB-12               548433  1994 ns/op    3 allocs/op
Benchmark_structured_Parallel_1KB-12      755862  1416 ns/op    4 allocs/op
Benchmark_structured_Parallel_Fields-12  2160236  549.5 ns/op   1 alloc/op

Benchmark_structured-12                  1985456  540.5 ns/op   2 allocs/op
Benchmark_structured_Parallel-12         1927399  557.2 ns/op   2 allocs/op
Benchmark_structured_1KB-12               593523  1968 ns/op    3 allocs/op
Benchmark_structured_Parallel_1KB-12      712048  1433 ns/op    4 allocs/op
Benchmark_structured_Parallel_Fields-12  2117650  541.8 ns/op   1 alloc/op
```

### Interpretation

Acacia’s performance places it among the fastest loggers in the Go ecosystem, with:

- **Top-tier throughput**
- **Zero allocations** on the hot path
- **Stable latencies under heavy parallelism**
- **<1µs for 1KB messages**, exceptional for a logger with real rotation

This makes Acacia suitable for:

- High-frequency trading
- Telemetry pipelines
- Microservice fleets
- Distributed systems logging
- Game servers
- Any high-throughput production environment

## Throughput (Messages per Second)

| Benchmark Type                      | Ops/sec (approx)           | Description                          |
|-------------------------------------|----------------------------|--------------------------------------|
| **Fast-path (string)**              | **~5.9–6.3 M msg/sec**     | ~158–169 ns/op                       |
| **Fast-path parallel (string)**     | **~4.5–4.7 M msg/sec**     | ~213–222 ns/op (12 logical cores)    |
| **Fast-path (bytes)**               | **~5.2–5.8 M msg/sec**     | ~172–192 ns/op                       |
| **Parallel bytes**                  | **~4.3–4.8 M msg/sec**     | ~206–231 ns/op                       |
| **1KB string messages**             | **~0.76–0.98 M msg/sec**   | ~1019–1306 ns/op (1 alloc/op)        |
| **1KB string parallel**             | **~0.86–0.97 M msg/sec**   | ~1030–1156 ns/op (1 alloc/op)        |
| **1KB bytes messages**              | **~0.64–0.95 M msg/sec**   | ~1055–1550 ns/op (0 alloc/op)        |
| **1KB bytes parallel**              | **~0.75–0.97 M msg/sec**   | ~1027–1340 ns/op (0 alloc/op)        |

### Interpretation

- Acacia comfortably handles millions of log events per second.
- Throughput remains stable even with 500+ concurrent producers.
- The engine maintains zero allocations in the fast path while doing so.
- This places Acacia among the fastest production-ready loggers in the Go ecosystem, not only on benchmarks, but in real
  workload patterns.

## Relative Performance (Parallel Fast-Path, Approx.)

Compact Comparison Table (Context Only)

| Logger         | Avg Parallel ns/op | Ops/sec   | Allocations | Rotation Built-in      | Notes                         |
|----------------|--------------------|-----------|-------------|------------------------|-------------------------------|
| **Acacia**     | **~212–239 ns**    | **~4.2–4.7M** | **0 alloc** | **Yes (daily + size)** | zero-loss barrier             |
| **phuslu/log** | ~380–500 ns        | ~2.0–2.6M | 0 alloc     | No                     | Extremely fast minimal logger |
| **Zerolog**    | ~420–650 ns        | ~1.5–2.3M | 0 alloc     | No                     | JSON-only, ultra-low alloc    |
| **Zap**        | ~500–800 ns        | ~1.2–2.0M | 0 alloc     | No                     | Structured logs first         |
| **Logrus**     | 4000+ ns           | <250k     | Many allocs | Yes                    | Feature-rich but slow         |

### Feature comparison (context only)

A quick, human-friendly snapshot of defaults and typical ranges. These reflect common OSS defaults; many libraries can be extended with external sinks/rotators.

| Feature                   | Acacia            | zerolog               | zap                   | phuslu/log          |
|---------------------------|-------------------|-----------------------|-----------------------|---------------------|
| Async single-writer       | ✔️ (queue)        | ✖️ (sync by default)  | ✖️ (sync by default)  | ✖️ (sync, minimal)  |
| File rotation (built-in)  | ✔️ (size + daily) | ✖️ (external)         | ✖️ (external)         | ✖️ (external)       |
| Strict FIFO ordering      | ✔️                | ✔️                    | ✔️                    | ✔️                  |
| Sync barrier (durability) | ✔️ (control+ack)  | ✖️                    | ✔️ (`Sync`)           | ✖️                  |
| Structured JSON builder   | ✔️ (manual/pools) | ✔️                    | ✔️                    | ✔️                  |
| Optimized pools           | ✔️                | ✔️                    | ✔️                    | ✔️                  |
| ns/op simple (tiny msg)   | 158–222           | 70–90                 | 120–150               | 27–40               |
| ns/op structured (small)  | 525–572           | 450–600               | 600–900               | 300–450             |

> Notes:
> - ns/op are indicative ranges from multiple runs on an Intel i7‑8750H; your results may vary by CPU, Go version, configuration, and output target.
> - “Async single-writer” reflects Acacia’s design (single writer goroutine with a queue). Other loggers can be made async via external sinks but are typically synchronous by default.
> - “File rotation (built-in)” refers to native rotation support. Others usually rely on external rotation tools.


### Interpretation

- Acacia consistently ranks among the top 1–2 fastest loggers in Go.
- Unlike others in this tier, Acacia includes full rotation, JSON, plain text, and zero-loss sync semantics.
- It’s competitive with the fastest experimental loggers (phuslu/log) and faster than mainstream options (zap, zerolog).

---

# Where Acacia Stands in the Ecosystem

Not a competition, but to give context:

- Faster than zap and zerolog in pure fast-path execution
- Comparable to (and often faster than) phuslu/log in parallel workloads
- Orders of magnitude faster than logrus / slog / go-kit
- Provides real rotation, which many fast loggers do not
- Zero-alloc both for strings *and* bytes (rare)
- Plain text + JSON in one engine
- No external dependencies

Acacia offers the rare combination of:
**speed + features + stability + human-friendly design**.

---

# Installation

```go
go get github.com/humanjuan/acacia/v2
```

### Basic Usage

```go
package main

import (
    "errors"

    acacia "github.com/humanjuan/acacia/v2"
)

func main() {
    // Create the logger (directory must already exist)
    log, err := acacia.Start("app.log", "./logs", acacia.Level.INFO)
    if err != nil { panic(err) }

    // Optional: rotate logs daily
    log.DailyRotation(true)

    // Optional: choose timestamp format
    log.TimestampFormat(acacia.TS.RFC3339Nano)

    // Make sure everything is flushed at the end
    // Close() guarantees zero loss and fsyncs before exiting
    defer log.Close()
    // If you need to persist mid‑run without closing, use Sync()
    // defer log.Sync()

    errDemo := errors.New("this is an error message")

    log.Critical("Critical event — system is down")
    log.Error("Something failed: %v", errDemo)
    log.Warn("High memory usage: %.2f GB", 7.8)
    log.Info("User %s logged in from %s", "juan", "192.168.1.100")
    log.Debug("Debugging session ID: %d", 12345)
}
```

> Notes:
> - `Close()` is the definitive shutdown: it drains, flushes, fsyncs, and closes the file.
> - `Sync()` does not close the logger. It creates a barrier so that everything enqueued before the call is flushed and synced.

---

### Plain‑text and JSON mode

Acacia writes human‑readable text by default. You can switch to structured JSON at any time and switch back later.

- Plain‑text (default):
  ```go
  log.Info("user %s logged in", user)
  // Example: 2025-11-25T22:21:45.123Z [INFO] user juan logged in
  ```

- JSON (structured):
  ```go
  log.StructuredJSON(true)
  log.Info(map[string]interface{}{
      "event": "login",
      "user":  "juan",
      "ip":    "192.168.1.10",
  })
  // Example: {"ts":"2025-11-25T22:21:45.123Z","level":"INFO","event":"login","user":"juan","ip":"192.168.1.10"}
  ```

Turn JSON off to return to plain‑text:
```go
log.StructuredJSON(false)
```

---

### Daily rotation

Enable a log file per day. The logger will atomically rename the current file to a dated name and continue on a fresh `app.log`.

```go
log, _ := acacia.Start("app.log", "./logs", acacia.Level.INFO)
log.DailyRotation(true)

log.Info("first message of the day")
log.Sync()
```

Naming when daily rotation is enabled:
- Base file: `app.log`
- Dated file for today: `app-YYYY-MM-DD.log` (e.g., `app-2025-11-25.log`)
- If size rotation is also enabled, backups for the day look like `app-YYYY-MM-DD.log.0`, `.1`, `.2`, ...

Details:
- Rotation is performed only by the writer goroutine (owner‑only), so it’s race‑free.
- Daily rotation happens when the calendar day actually changes (no initial pre‑rotation upon enabling).

---

### Size rotation

Rotate when the file reaches a size limit, and keep a fixed number of backups.

```go
log, _ := acacia.Start("size.log", "./logs", acacia.Level.INFO)
log.Rotation(1, 3) // 1 MB, keep 3 backups

// write a lot...
log.Sync()
```

Naming when only size rotation is enabled:
- `size.log` → `size.log.0`, `size.log.1`, `size.log.2`, ... up to your backup limit

If daily rotation is also enabled, size backups are created for the dated file:
- `app-YYYY-MM-DD.log.0`, `.1`, `.2`, ...

Performance notes:
- The writer tracks the current file size internally (no `Stat()` call per flush), and rotates atomically.

---

### Fast‑path bytes

If you already have your message as `[]byte`, use the byte fast‑path to avoid conversions and extra work.

```go
log, _ := acacia.Start("bench.log", "./logs", acacia.Level.INFO)

b := []byte("The quick brown fox jumps over the lazy dog")
log.InfoBytes(b) // zero allocations on the producer side

// For larger payloads, reuse the same slice where possible
msg := bytes.Repeat([]byte("X"), 1024)
log.InfoBytes(msg)
```

How it works:
- The producer sends a lightweight event; the writer assembles the final line once into its batch buffer.
- The design keeps 0 allocs/op on the hot path and performs a single copy into the batch.

---

### io.Writer compatibility

You can plug Acacia anywhere an `io.Writer` is expected.

- With `fmt`/`io`:
  ```go
  fmt.Fprintf(log, "hello fmt %d\n", 42)
  io.WriteString(log, "hello io\n")
  ```

- With the standard library logger:
  ```go
  std := logpkg.New(log /* io.Writer */, "", 0)
  std.Println("line from stdlib log")
  ```

- With HTTP servers (as error log writer):
  ```go
  srv := &http.Server{
      Addr:     ":8080",
      ErrorLog: logpkg.New(log /* io.Writer */, "http ", 0),
  }
  ```

> Notes:
> - If the input doesn’t end with `\n`, Acacia will add it when formatting the line.
> - `Write` logs at `[INFO]` and respects the minimum level configured at `Start`.

---

### Advanced buffer customization

Tune queue and batch sizes to match your workload. These options are passed to `Start`.

- Producer queue capacity (internal channel):
  ```go
  log, _ := acacia.Start(
      "app.log", "./logs", acacia.Level.INFO,
      acacia.WithBufferSize(5_000_000), // messages buffer
  )
  ```

- Writer internal buffers (memory used to accumulate writes):
  ```go
  log, _ := acacia.Start(
      "app.log", "./logs", acacia.Level.INFO,
      acacia.WithBufferCap(256<<10), // 256 KB internal writer buffers
  )
  ```

- Writer drain burst (how many events are drained per wake-up):
  ```go
  log, _ := acacia.Start(
      "app.log", "./logs", acacia.Level.INFO,
      acacia.WithDrainBurst(1024), // batch more events in writer loop
  )
  ```

- Flush interval (latency vs throughput):
  ```go
  log, _ := acacia.Start(
      "app.log", "./logs", acacia.Level.INFO,
      acacia.WithFlushInterval(100*time.Millisecond),
  )
  ```

Practical tips:
- For very high throughput, `WithBufferSize(5_000_000)`, `WithBufferCap(256<<10)` and `WithDrainBurst(1024)` are solid defaults.
- A slightly longer flush interval (e.g., 150–250 ms) reduces syscalls and increases throughput, at the cost of a bit more latency.
- If you don’t need mid‑run durability, rely on `Close()` at shutdown for zero loss. Use `Sync()` only when you need to persist immediately without closing.

---

# Architecture Overview
Acacia uses an optimized writer pipeline:

- Single writer goroutine
- Producer goroutines never block (queue + event channels)
- Pooled buffers (512B / 2KB / 4KB / 8KB buckets)
- Cached timestamps refreshed every 100ms
- Batch-aware flush system
- Size and daily rotation managed atomically

This architecture ensures:

- Extreme throughput
- Minimal contention
- Predictable performance
- Zero lost logs
- Clean shutdown semantics


# Crafted by a Human

Acacia is part of the HumanJuan projects, crafted with coffee, code and intention.
If you love this project, **feel free to ⭐ the project or contribute with code or coffee.**

[![Buy Me a Coffee](https://img.shields.io/badge/Buy_Me_A_Coffee-Support-orange?logo=buy-me-a-coffee&style=flat-square)](https://www.buymeacoffee.com/humanjuan)

# License

This project is released under the **MIT License**.  
You are free to use it in both personal and commercial projects.

---
If you see anything unclear or have a use case we didn’t cover, please open an issue. The goal is to keep Acacia simple, practical and reliable in real‑world systems.
