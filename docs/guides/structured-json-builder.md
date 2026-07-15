# Structured JSON Builder Guide

## Overview
Acacia provides a fluent, type-safe structured JSON builder API optimized for high-throughput, low-allocation logging paths. This guide documents how to write structured events, utilize nested builders, and choose between fast and slow path APIs.

---

## When to Use the Typed Builder
While plain format strings are useful for text messages, typed builders should be preferred when:
1. Emitting JSON logs to log collectors or search indexes (e.g., Elasticsearch, Google Cloud Logging).
2. Minimizing heap allocations under heavy load. The typed builders write fields directly to a pooled buffer.
3. Performing inline, case-insensitive redaction of sensitive credentials.

---

## Event Lifecycle
Structured logging revolves around the following lifecycle:
1. **Acquisition**: Call `InfoEvent()`, `WarnEvent()`, `ErrorEvent()`, `DebugEvent()`, or `CriticalEvent()` to obtain a pooled `*Event` builder.
2. **Field Appending**: Chain fields such as `.Str()`, `.Int()`, `.Bool()`, `.Object()`, or `.Array()`.
3. **Flushing**: Call `.Msg("message")` or `.Send()` to terminate the event, write standard timestamp/level metadata, push it to the queue, and recycle the builder structure.

> [!WARNING]
> Builders are not thread-safe and must be used sequentially within a single goroutine. Once `Msg()` or `Send()` is called, the builder instance is recycled; you must not store or access it again.

---

## Field Types

### Flat Fields
Chain scalar fields directly to the event builder:
```go
lg.InfoEvent().
    Str("service", "billing").
    Int("attempt", 2).
    Bool("fallback", false).
    Float64("discount_ratio", 0.15).
    Msg("payment transaction started")
```

### Nested Objects
Represent nested structures by passing a callback function:
```go
lg.InfoEvent().
    Object("metadata", func(o *acacia.Object) {
        o.Str("region", "us-east-1").
        Int("host_id", 4096)
    }).
    Msg("host metrics gathered")
```

### Arreglos (Arrays)
Arreglos can contain primitive values or structured object elements:
```go
lg.InfoEvent().
    Array("tags", func(a *acacia.Array) {
        a.Str("production").
        Str("api").
        Object(func(o *acacia.Object) {
            o.Str("meta-key", "meta-val")
        })
    }).
    Msg("resource tagged")
```

---

## Scopes: Group vs Namespace

### Group
A semantic alias for `Object` that groups fields inside a real nested JSON structure:
```go
lg.InfoEvent().
    Group("network", func(g *acacia.Object) {
        g.Str("interface", "eth0").
        Int("port", 443)
    }).
    Msg("connection established")
```

### Namespace
Appends flat, prefixed keys (`"prefix.key"`) within a dedicated, restricted scope. It does not create real nested JSON brackets:
```go
lg.InfoEvent().
    Namespace("db", func(n *acacia.Namespace) {
        n.Str("query", "SELECT 1").
        Int("connections", 15)
    }).
    Msg("query executed")
```
*Representative Output:*
`{"ts":"...","level":"INFO","db.query":"SELECT 1","db.connections":15,"msg":"query executed"}`

---

## Pre-serialized Payloads: RawJSON
Use `RawJSON` to write pre-serialized JSON data directly to the builder:
```go
jsonData := []byte(`{"ip":"127.0.0.1","user_agent":"Mozilla"}`)
lg.InfoEvent().
    RawJSON("client_info", jsonData).
    Msg("client request captured")
```
> [!IMPORTANT]
> `RawJSON` is contractually treated as **trusted input only**. If the external key (e.g. `"client_info"`) is not sensitive, Acacia does not inspect, parse, or redact keys inside the payload for performance reasons. Invalid or malformed JSON payloads safely degrade to JSON-escaped string literals.

---

## Dynamic Values: Any
For dynamic or unknown types, you can fall back to `Any`:
```go
lg.InfoEvent().
    Any("payload", map[string]interface{}{
        "random_id": 9876,
    }).
    Msg("dynamic event logged")
```
> [!CAUTION]
> `Any` is an explicit **slow path** fallback. It uses standard library serialization (`encoding/json`) and reflection. Do not use `Any` in high-frequency hot paths. Non-serializable values (functions, channels, cyclic structures) will not panic; they serialize to a safe sentinel `{"_acacia_error":"any marshal error"}`.

---

## Error Handling
Append errors using `.Err(err)` or `.ErrKey(key, err)`:
```go
if err != nil {
    lg.ErrorEvent().
        Err(err).
        Msg("operation failed")
}
```

---

## Disabled Level Behavior
If a logging level is disabled (e.g. `DebugEvent()` when configured at `INFO` level), obtaining the event returns `nil`. All chained calls immediately short-circuit as safe no-ops:
```go
// Immediately returns nil, avoiding processing overhead for inner functions
lg.DebugEvent().
    Object("debug_payload", func(o *acacia.Object) {
        // Not executed
        o.Str("heavy_field", calculateHeavyVal())
    }).
    Msg("debug logs")
```

---

## Performance Caveat
All latency and allocation metrics documented or observed in benchmarks are collected under diagnostic runs on developmental external disk environments. Final release validation under controlled production scenarios must be executed using localized SSD drives.
