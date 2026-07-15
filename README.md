[![Go Reference](https://pkg.go.dev/badge/github.com/humanjuan/acacia/v2.svg)](https://pkg.go.dev/github.com/humanjuan/acacia/v2)
[![Buy Me a Coffee](https://img.shields.io/badge/Buy_Me_A_Coffee-Support-orange?logo=buy-me-a-coffee&style=flat-square)](https://www.buymeacoffee.com/humanjuan)

# Acacia: High Performance Concurrent Logger for Go

**High-performance concurrent logging | Typed structured JSON builder | Sync/Close semantics validated by documented tests**

Acacia delivers documented low-allocation fast paths, rename-based secure file rotation, a Sync/Close barrier for accepted events under documented semantics, and a single‑writer architecture engineered for extreme concurrency. No dependencies. No magic. Just a human‑crafted logger that does its job exceptionally well.

---

## Why Acacia Exists

Most loggers are either:
- blazing fast *but limited* (no rotation, no io.Writer compatibility, JSON-only), or
- feature-rich *but slow*, allocating heavily on every write.

Acacia bridges that gap.

It was built because logging should be **simple**, **stable**, and **trustworthy**, especially when hundreds of goroutines are writing simultaneously.  
And because software made by humans, with care, should feel human too.

---

# Key Features

### **Allocation-conscious fast path**
Plain `string` and `[]byte` logging have shown **0 allocs/op** in documented hot-path benchmarks. This makes Acacia allocation-conscious fast paths for high-throughput Go services.

### **Fluent Builder API**
Structured logging via a type-safe builder pattern (`InfoEvent`, `WarnEvent`, etc.) avoiding dynamic maps or reflection overhead in the hot path.

### **Automated Redaction**
Built-in protection of sensitive credentials (e.g. `password`, `token`, `api_key`) and explicit `.Secret()` and `.StrRedacted()` builders to secure logging data inline.

### **Queuing & Backpressure Policies**
Configurable behaviors under congestion (`DropNewest`, `BlockOnFull`, `DropOldest`, `DropDebugOnly`) to maintain stability under mass saturation.

### **Hardened Filesystem Controls**
Checks file basenames to prevent directory traversal, rejects base directory symlinks, and enforces secure defaults (`0600` for files, `0700` for folders).

### **Deterministic Sync & Close**
`logger.Close()` shuts down the logger idempotently, draining all accepted events in the queue to disk, while post-close operations fail safely without panics or locks.

### **Race detector suite passed under documented tests**
Passes `go test -race` cleanly, verified under extreme parallel load.

---

# Installation

```bash
go get github.com/humanjuan/acacia/v2
```

---

# Usage Documentation

### 1. Basic Usage

```go
package main

import (
    acacia "github.com/humanjuan/acacia/v2"
)

func main() {
    // Create the logger (directory must exist)
    log, err := acacia.Start("app.log", "./logs", acacia.Level.INFO)
    if err != nil { panic(err) }

    // Flushes accepted/enqueued messages according to the documented Close semantics, and closes cleanly
    defer log.Close()

    log.Info("Hello, this is a plain text log with %d parameters", 42)
}
```

---

### 2. Structured JSON Builder

Acacia provides a typed structured JSON builder for high-frequency logging paths.

Recommended fast paths:
- flat fields with `InfoEvent().Str(...).Int(...).Msg(...)`;
- nested objects with `Object`;
- arrays with primitive values and object elements;
- groups as semantic aliases for objects;
- namespaces for flat prefixed keys.

Convenience / slow paths:
- `RawJSON` for trusted pre-serialized JSON;
- `Any` as explicit slow fallback for dynamic values;
- legacy `StructuredJSON(true)` map path for compatibility.

#### Flat Fields Example
```go
lg.InfoEvent().
    Str("service", "payments").
    Int("status", 200).
    Bool("cache_hit", true).
    Msg("request completed")
```

#### Nested Object Example
```go
lg.InfoEvent().
    Object("user", func(u *acacia.Object) {
        u.Str("id", "u_123")
        u.Str("country", "CL")
    }).
    Msg("user loaded")
```

#### Array Example
```go
lg.InfoEvent().
    Array("items", func(a *acacia.Array) {
        a.Object(func(i *acacia.Object) {
            i.Str("sku", "ABC")
            i.Int("qty", 2)
        })
        a.Str("manual-note")
    }).
    Msg("cart evaluated")
```

#### Redaction Example
```go
lg.InfoEvent().
    Str("password", "secret").
    Str("authorization_header", "Bearer token").
    Msg("credentials checked")
```
Sensitive keys such as `password` and `authorization_header` are automatically redacted to `"[REDACTED]"` under the audited suite.

For detailed guides, please see:
* [Structured JSON Builder Guide](docs/guides/structured-json-builder.md)
* [Redaction Policy Guide](docs/guides/structured-json-redaction.md)
* [Migration Strategy Guide](docs/guides/structured-json-migration.md)

---

### 3. Sensitive Data Redaction

Acacia can redact credentials automatically based on key names or explicitly via fluent builders:

```go
// 1. Auto-redacted key names (matching "password", "token", "api_key", "secret", etc.)
log.InfoEvent().
    Str("password", "rawPassword123"). 
    Send() // Outputs: "password":"[REDACTED]"

// 2. Explicit redaction for custom keys
log.InfoEvent().
    Secret("custom_key", "highlySensitiveValue").
    StrRedacted("auth_token", tokenString).
    Send()
```

---

### 4. Queuing and Backpressure

Control logger behavior under high volumes to prevent memory exhaustion:

```go
log, _ := acacia.Start("app.log", "./logs", acacia.Level.INFO,
    // Drop newest log events if the channel queue gets saturated
    acacia.WithBackpressurePolicy(acacia.DropNewest),
    // Or: Block callers until the queue has space
    // acacia.WithBackpressurePolicy(acacia.BlockOnFull),
)
```
Available policies: `DropNewest`, `BlockOnFull`, `DropOldest`, `DropDebugOnly`.

---

### 5. Filesystem Safety and Permissions

Enforce strict directory and file permissions:

```go
log, _ := acacia.Start("app.log", "./logs", acacia.Level.INFO,
    acacia.WithFileMode(0600), // restrict log file read/write
    acacia.WithDirMode(0700),  // restrict backup directory access
)
```
*Note: Filenames are validated to reject traversal characters (`../`) and base symlinks to mitigate hijacking attacks.*

---

### 6. Sync and Close Semantics

* **`Close()`:** Shuts down the logger, stops background tickers, drains all accepted events in the queue, flushes buffers, and runs fsync. Idempotent and concurrency-safe.
* **`Sync()`:** A synchronization barrier. Ensures that all events accepted *prior* to calling `Sync()` are flushed to the file descriptor.
* **Post-Close Writes:** Methods like `Info` or `Send` called after closure return immediately without blocking or panicking, incrementing `DroppedTotal`.

---

## Daily Rotation

Enable daily rotation to group files by date:

```go
log.DailyRotation(true)
// Main file: app.log
// Archived day file: app-YYYY-MM-DD.log
```

---

## Size Rotation

```go
log.Rotation(1, 3) // 1 MB max size, keep 3 backup files (size.log.0, size.log.1, size.log.2)
```

---

## io.Writer Compatibility

Plug Acacia into standard library logs or web servers:

```go
std := logpkg.New(log /* io.Writer */, "prefix: ", 0)
std.Println("Line printed via stdlib log")
```

---

# Performance Claims and Scope

Acacia is designed with low allocation overhead in mind. Below are representative documented benchmarks:

> [!NOTE]
> These metrics represent benchmark observations on developer environments and are environment-dependent. They do not constitute a final release validation. Final SSD hard validation remains pending.

### **String Hot-Path (GMP=2)**
* **Single-threaded:** ~75-90 ns/op (**0 allocs/op observed**)
* **Parallel:** ~130-160 ns/op (**0 allocs/op observed**)

### **Fluent Builder (GMP=2)**
* **Structured mixed event:** ~1.3 µs/op (~1 alloc/op observed under sync.Pool controls)

### **Lifecyle Ops (GMP=2)**
* **Write after close:** ~18 ns/op (**0 allocs/op observed**)
* **Sync after close:** ~0.6 ns/op (**0 allocs/op observed**)

---

# Performance Claim Boundaries

1. **0 allocs/op is observed** in documented benchmarks for plain string fast path (`log.Info`), byte-path (`log.InfoBytes`), disabled levels, and post-close/drop paths under local testing.
2. **Legacy dynamic structured path** (`log.Info(map[string]interface{}{...})`) uses reflection and will perform heap allocations.
3. **Builder API** maintains low allocation overhead under developer tests by utilizing reusable JSON buffers from a pooled resource allocator.

---

# Security Hardening Summary

Acacia has been hardened against the following security vectors:
* **SEC-01-001 (JSON Control Characters):** Hardened escape sequences preventing JSON format injection.
* **SEC-06-001 (Path Traversal):** Rejection of filenames containing traversal sequences.
* **SEC-06-002 (Symlink Attacks):** `os.Lstat` path analysis before open/rotation prevents symlink hijacking.
* **SEC-06-003 (Permissive Log Modes):** Default file permissions are locked down to `0600`/`0700`.
* **SEC-07-001 (Sync/Close Deadlocks):** Emitters do not deadlock during logger shutdowns.

---

# Reproducing Benchmarks

To reproduce these benchmarks on your local system, run:

```bash
# GMP = default
go test ./test -bench 'Benchmark(EventBuilder|JSONEncoder|Redaction|Backpressure|ValidateLogFilename|SafeJoinLogPath|Rotation|Sync|Close|WriteAfterClose|ConcurrentSync|string|structured)' -benchmem -run '^$' -count=10

# GMP = 1
GOMAXPROCS=1 go test ./test -bench 'Benchmark(EventBuilder|JSONEncoder|Redaction|Backpressure|ValidateLogFilename|SafeJoinLogPath|Rotation|Sync|Close|WriteAfterClose|ConcurrentSync|string|structured)' -benchmem -run '^$' -count=10
```

---

# Compatibility and Migration

For detailed instructions on migrating legacy code to the new fluent builders or handling path traversal and file permission modifications, please refer to the [Migration Strategy Guide](docs/MIGRATION_STRATEGY.md).

---

# Architecture Overview

* **Single Writer pipeline** reduces lock contention.
* **Lock-free queues** are used to shuttle events asynchronously.
* **Pooled buffers** reduce garbage collection pressure.
* **Timestamp cache** is updated on a 100ms background interval.

---

# Crafted by a Human

Acacia is part of the HumanJuan projects, crafted with coffee, code, and intention. If you love this project, **feel free to ⭐ the project or contribute with code or coffee.**

---

# License

This project is released under the **MIT License**.
