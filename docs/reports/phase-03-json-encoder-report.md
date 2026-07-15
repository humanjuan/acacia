# Phase 03 — Append-Only JSON Encoder Report

*This is a historical engineering report. The filename is kept for audit continuity.*

* **Date:** 2026-07-07
* **Branch:** `dev`
* **Commit:** `d3ad0ac feat: implement typed structured event builder and add comprehensive testing`
* **Go version:** `go1.23.2 darwin/amd64`
* **OS/Arch:** `darwin/amd64`
* **Scope:** Optimization of the structured JSON serialization engine via a manual append-only encoder and dynamic pooling.

---

## Objective
Optimize and consolidate structured JSON serialization within the hot path of Acacia's typed builder API, drastically reducing memory allocations and CPU time in small, medium, and large (1KB+) messages, resolving regressions identified in previous milestones.

---

## Scope Decision
* **Optimized:** The JSON string encoding and escaping engine (`escapeJSONInto`), as well as the capacity management and pooling of the byte buffer (`ensureJSONCapacity`) for the structured hot path of `Event`.
* **Keep Unchanged:** The public API of the event builder, the asynchronous write/routing logic, and the call signatures of the legacy path, preserving complete backward compatibility.

---

## Encoder Design
* **Append-only Encoding:** Direct manual writing was implemented without resorting to reflective packages such as `reflect` or dynamic encoders like `encoding/json.Marshal`. Fast native functions like `strconv.AppendInt`, `strconv.AppendFloat`, and `time.AppendFormat` are used.
* **Efficient String Escaping:** In `escapeJSONInto`, an estimation and pre-allocation are performed at the start of the traversal. This ensures that common unescaped characters are written sequentially without additional checking calls or memory growth.
* **Reusable Dynamic Buffer Strategy:** Via `ensureJSONCapacity`, when the byte buffer exceeds its current capacity:
  1. A new buffer with double the capacity is requested from the Acacia pool (`getJSONBufCap`).
  2. The accumulated content is copied.
  3. The old buffer is returned immediately to the corresponding pool (`putJSONBuf`).
  This reduces allocations and explosive buffer growth by reusing Acacia's internal buckets. However, benchmem still reports control allocations associated with sync.Pool/type assertions, especially during capacity jumps.

---

## Files Changed
* **acacia.go**: Modification of `escapeJSONInto` to integrate optimal pre-allocation, insertion of `ensureJSONCapacity`, and updating of all `Event` builder methods to write with guaranteed capacity.
* **test/phase03_json_encoder_test.go**: Unit test suite for the manual encoder (primitive types, escaping, secret redaction, and large payloads).
* **test/phase03_json_encoder_benchmark_test.go**: Benchmark suite to evaluate encoder latency and memory consumption.
* **test/phase03_json_encoder_fuzz_test.go**: Fuzzing suite to evaluate robustness against escaping, mixed fields, and massive inputs.

---

## Public API Impact
* **None.** Public compatibility of v2.x is not broken. The typed structured builder API maintains exactly its previous signatures.

---

## Tests Executed

| Command | Result | Notes |
|---|---:|---|
| `go test ./...` | **PASS** | All tests pass successfully. |
| `go test -race ./...` | **PASS** | No data races detected. |

---

## Fuzzing Summary

| Target | Result | Notes |
|---|---:|---|
| `FuzzJSONEncoderStringEscaping` | **PASS** | Encoder resilience against escaping and control bytes. |
| `FuzzJSONEncoderBuilderMixedFields` | **PASS** | Safe behavior under different combinations. |
| `FuzzJSONEncoderLargePayload` | **PASS** | JSON validity conformance under massive payloads up to 16KB. |

---

## Benchmark Summary

### GOMAXPROCS=1
| Benchmark | ns/op | B/op | allocs/op | Compared To | Notes |
|---|---:|---:|---:|---|---|
| `BenchmarkJSONEncoderDisabledLevel` | 19.3 ns | 0 B | 0 | `Phase 02` | No allocations |
| `BenchmarkJSONEncoderBuilderSmall` | 723.4 ns | 292 B | 1 | `Phase 02` | Fast return |
| `BenchmarkJSONEncoderBuilderMixed` | 770.0 ns | 255 B | 1 | `Phase 02` | Maintains single allocation |
| `BenchmarkJSONEncoderBuilder1KB` | 13552 ns | 17273 B | 4 | `Phase 02 (10 allocs)` | Allocation reduction |
| `BenchmarkJSONEncoderBuilder4KB` | 48593 ns | 76137 B | 6 | - | Pre-allocation debt |
| `BenchmarkJSONEncoderBuilder8KB` | 132953 ns | 151260 B | 7 | - | Pre-allocation debt |

### GOMAXPROCS=2
| Benchmark | ns/op | B/op | allocs/op | Compared To | Notes |
|---|---:|---:|---:|---|---|
| `BenchmarkJSONEncoderDisabledLevel` | 23.3 ns | 0 B | 0 | `Phase 02` | Ultra-fast discard |
| `BenchmarkJSONEncoderBuilderSmall` | 701.3 ns | 271 B | 1 | `Phase 02` | sync.Pool interface allocation |
| `BenchmarkJSONEncoderBuilderMixed` | 734.9 ns | 272 B | 1 | `Phase 02` | 1 single alloc |
| `BenchmarkJSONEncoderBuilder1KB` | 13884 ns | 9771 B | 4 | `Phase 02 (10 allocs)` | 2.5x allocation reduction |
| `BenchmarkJSONEncoderBuilder4KB` | 57420 ns | 77950 B | 6 | - | Controlled growth |
| `BenchmarkJSONEncoderBuilder8KB` | 162102 ns | 153884 B | 7 | - | Controlled growth |

---

## 1KB / Large Payload Analysis

Direct comparison in `BenchmarkJSONEncoderBuilder1KB` (Phase 02 vs Phase 03):

| Phase | Latency (ns/op) | Allocations (allocs/op) | Memory Consumption (B/op) |
|---|---:|---:|---:|
| **Phase 02** (Classic Go append) | ~16,588 ns | 10 | ~31,817 B |
| **Phase 03** (Append-only Encoder Pooling) | **~13,884 ns** | **4** | **~9,771 B** |
| **Improvement** | **16% speedup** | **60% fewer allocations** | **69% memory usage reduction** |

*Note:* The 4 allocations in the 1KB case derive from the interface returned when retrieving buffers from the pool buckets during capacity jumps. However, byte consumption decreased 3.2x due to the immediate reuse of memory released by the old buffers.

---

## pprof Summary
The CPU profiler analysis for the 1KB benchmark indicates that:
* Copy and byte-moving operations (`runtime.memmove`) consume 19.66% of total CPU.
* Serialization of strings and special characters (`escapeJSONInto`) represents 19.26% of CPU.
* Capacity management and buffer re-pooling (`ensureJSONCapacity`) consumes 9.13% of CPU.
* The rest of the CPU is distributed across collection operations (`runtime.scanobject` and `runtime.mallocgc`) arising from contention over the generic return type of `sync.Pool`.

---

## Hot Path Restrictions Check
The hot path of the new typed structured encoder/builder:
* **Does it use reflect?** **NO**.
* **Does it use fmt.Sprint/fmt.Sprintf?** **NO**.
* **Does it use encoding/json.Marshal?** **NO**.
* **Does it use map[string]interface{}?** **NO**.

---

## Acceptance Criteria Status

| Criterion | Status | Evidence |
|---|---|---|
| Manual structured encoder integrated | **FULFILLED** | Optimized methods implemented in `acacia.go`. |
| Disabled level at 0 allocs/op | **FULFILLED** | `BenchmarkJSONEncoderDisabledLevel` reports 0 allocs/op. |
| Small/Mixed builder at <= 1 alloc/op | **FULFILLED** | Both report exactly 1 alloc/op. |
| Drastic allocation reduction in 1KB | **FULFILLED** | Allocations reduced from 10 to 4, and memory reduced by 69%. |
| Absence of fuzzing and JSON regressions | **FULFILLED** | All fuzz targets and unit tests pass successfully. |

---

## Risks / Regressions
* **Boxing in sync.Pool:** Despite successfully reusing buffers, the type assertions required when invoking `sync.Pool.Get()` continue to generate control allocations on the Heap (1 allocation per capacity jump). This behavior will be refined through struct-by-value or thread-local buffers in Phase 05.

---

## Private Evidence References
* `.private/reports/phase-03-private-evidence.md`
* `.private/bench/phase03_json_encoder_bench_default.txt`
* `.private/bench/phase03_json_encoder_bench_gmp1.txt`
* `.private/bench/phase03_json_encoder_bench_gmp2.txt`
* `.private/pprof/phase03_1kb_cpu.pprof`
* `.private/pprof/phase03_1kb_mem.pprof`

---

## Next Phase Recommendation
It is recommended to proceed to **Phase 04 — Redaction and Sensitive Data Security** to design and integrate a zero-allocation, non-reflective redaction system that replaces the reflective denial of the current wrapper, mitigating auditing bottlenecks.
