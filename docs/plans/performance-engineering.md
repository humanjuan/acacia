# Low-Allocation Hot Paths (Performance Engineering)

This document describes the optimization strategies used to achieve low allocation behavior in Acacia's critical write paths.

---

## 1. Traditional Overhead vs. Low-Allocation Design

Performance analysis identifies two primary bottlenecks under high concurrency in standard logging configurations:
1. **Boxing of Interfaces (`interface{}`):** Passing dynamic data types into interface-based signatures triggers heap allocations.
2. **Runtime Reflection (`reflect`):** Dynamic structural inspection slow down throughput and creates garbage collection pressure.

---

## 2. Optimization Pillars for Low-Allocation Logging

### A. Fluent API / Builder Pattern
Chainable typed methods (`Str`, `Int`, etc.) avoid boxing by maintaining strict types.
* **Direct Paths:** Direct string and `[]byte` hot paths achieve **0 allocs/op** in benchmarks when writing pre-formatted payloads.
* **Builder Paths:** Builder events are designed for low allocation behavior; they can trigger `1+ allocs/op` depending on use case specifics (e.g., dynamic key copies, string allocations, or nested callback structures).
* **Slow Paths:** The `Any` fallback relies on reflection and represents a slow path that is not recommended for high-frequency operations.

### B. Buffer Pooling with `sync.Pool`
Struct reuse eliminates constant heap allocations by recycling active write event builders and backing buffers.

### C. Hand-rolled JSON Encoder (Append-only)
Avoids `encoding/json` and `fmt.Sprint` overhead by appending serialized keys and values directly to the buffer slice using `strconv` primitives.

### D. Queue Optimization
Ring buffers are optimized to store structured references, avoiding pointer boxing, and support non-blocking drop options to short-circuit discarded logs.

---

## 3. Validation Environment Rules

* **Resource Restriction:** Core benchmarks are validated with `GOMAXPROCS=1` to force planner contention and expose serialization locks.
* **Allocation Boundaries:**
  - Direct bytes/strings: strict target of 0 allocs/op.
  - Nested builders: low-allocation target (1-2 allocs/op under ordinary cases).

---

## 4. Hard Validation Methodology

To prevent regressions, the following protocols are applied:

### Prerequisites
- **`benchstat` Comparison:** Indispensable to confirm statistical relevance between baseline and current metrics.
- **System Isolation:** Executed on quiet local hardware without resource-intensive background processes.
- **Ignored Profiles:** Profiling outputs (`.pprof`, `.prof`, `.trace`) are saved locally under `.private/` and excluded from the git index.

### Parameters
- **Repetitions:** `count=10` or higher to stabilize variance.
- **Planer Contention:** Tested under `GOMAXPROCS=1`, `GOMAXPROCS=2`, and default systems.
- **Fuzzing:** Run fuzzers for at least 60s per target.

---

## 5. Metrics Matrix

Active benchmarks measure latency (`ns/op`), consumed memory (`B/op`), and allocations (`allocs/op`) across the following capabilities:
1. **Direct `[]byte` Writing:** Low-level raw bytes payloads.
2. **Direct `string` Writing:** Standard raw string logging.
3. **Flat JSON Builder:** Direct typed fields.
4. **Nested Objects:** Object nested structures.
5. **RawJSON:** Trusted pre-serialized JSON validation.
6. **Any Fallback:** Performance profile under dynamic input.
7. **Redaction:** Sanitization overhead.
8. **Backpressure:** Congestion drop policies.
9. **Sync/Close:** Sinking latency and safe shutdown.
10. **Size/Time Rotation:** File rotation impact on hot path.
11. **OTel Adapter:** *Future optional adapter, not part of the current release candidate.*
