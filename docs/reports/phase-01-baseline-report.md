# Phase 01 — Reproducible Baseline Report

*This is a historical engineering report. The filename is kept for audit continuity.*

* **Date:** 2026-07-07
* **Branch:** `dev`
* **Commit:** `b1f1575 docs: establish public-private documentation boundary and structure`
* **Go version:** `go1.23.2 darwin/amd64`
* **OS/Arch:** `darwin/amd64`
* **GOMAXPROCS values tested:** Default (12), 1, 2
* **Scope:** Base performance measurement of Acacia before any optimization.

---

## Objective
Establish a quantitative and reproducible baseline of the Acacia logger's performance across its different execution paths (string fast path, bytes fast path, legacy structured JSON, sanitization, saturated queue dropping, rotation, and I/O), evaluating the impact of parallelism and the Garbage Collector (GC).

---

## Workspace Cleanliness
The baseline was taken from a **clean committed state** on the `dev` branch. All experimental files and base code from phases 00 and 01 (`acacia.go` with drop instrumentation, `internal/logging/wrapper.go` with `redactRecursive`, and `test/stress_vectors_test.go` with the stress vector tests) have been formally incorporated in prior commits. There are no uncommitted modified files in the current working tree.

---

## Commands Executed
The following reproducible commands were executed to compile metrics:
```bash
# Validaciones base y detección de carreras
go test ./...
go test -race ./...

# Stress Testing and Vectors
go test -v ./test/... -run "TestVector" -count=1

# Benchmarking
go test ./... -bench=. -benchmem -run '^$' -count=10
GOMAXPROCS=1 go test ./... -bench=. -benchmem -run '^$' -count=10
GOMAXPROCS=2 go test ./... -bench=. -benchmem -run '^$' -count=10

# Listado de benchmarks
go test ./... -list 'Benchmark.*'

# Profiling
GOMAXPROCS=1 go test ./test -run '^$' -bench 'Benchmark_string$' -benchmem -count=5 -cpuprofile=.private/pprof/phase01_string_gmp1_cpu.pprof -memprofile=.private/pprof/phase01_string_gmp1_mem.pprof
GOMAXPROCS=1 go test ./test -run '^$' -bench 'Benchmark_structured_Parallel_Fields$' -benchmem -count=5 -cpuprofile=.private/pprof/phase01_structured_gmp1_cpu.pprof -memprofile=.private/pprof/phase01_structured_gmp1_mem.pprof
GOMAXPROCS=1 go test ./test -run '^$' -bench 'BenchmarkVector2Redact$' -benchmem -count=5 -cpuprofile=.private/pprof/phase01_redaction_gmp1_cpu.pprof -memprofile=.private/pprof/phase01_redaction_gmp1_mem.pprof
```

---

## Test Results

| Command | Result | Notes |
|---|---:|---|
| `go test ./...` | **PASS** | All tests pass after SEC-01-001 remediation. |
| `go test -race ./...` | **PASS** | No races or failures after remediation. |
| `go test -run TestVector` | **PASS** | Injection, redaction, and drop vectors approved. |
| `go test ./... -run Fuzz -fuzz=FuzzStructuredJSONValidLine -fuzztime=20s` | **PASS** | Structured JSON validity fuzzing completes 20s OK. |

---

## Benchmark Inventory

| Benchmark | Classified Path | Exists Before Phase 01 | Notes |
|---|---|---:|---|
| `Benchmark_byte` | bytes fast path | Yes | Base bytes test |
| `Benchmark_byte_Parallel` | bytes fast path | Yes | Parallel bytes test |
| `Benchmark_byte_1KB` | bytes fast path | Yes | 1KB message in bytes |
| `Benchmark_byte_Parallel_1KB` | bytes fast path | Yes | Parallel 1KB message in bytes |
| `Benchmark_string` | string fast path | Yes | Base string test |
| `Benchmark_string_Parallel` | string fast path | Yes | Parallel string test |
| `Benchmark_string_1KB` | string fast path | Yes | 1KB message in string |
| `Benchmark_string_Parallel_1KB` | string fast path | Yes | Parallel 1KB message in string |
| `Benchmark_structured` | structured JSON current | Yes | Simple structured JSON test |
| `Benchmark_structured_Parallel` | structured JSON current | Yes | Parallel structured JSON test |
| `Benchmark_structured_1KB` | structured JSON current | Yes | 1KB JSON message |
| `Benchmark_structured_Parallel_1KB` | structured JSON current | Yes | Parallel 1KB JSON message |
| `Benchmark_structured_Parallel_Fields` | structured legacy map/interface | Yes | Test with legacy dynamic fields |
| `BenchmarkVector1JSON` | structured legacy map/interface | No | Added in Phase 00B |
| `BenchmarkVector2Redact` | sanitization/redaction current | No | Added in Phase 00B |
| `BenchmarkVector3Drop` | queue saturation/drop | No | Added in Phase 00B |

---

## Benchmark Summary

*(Averaged values from the 10 iterations of each executed suite)*

| Path | Benchmark | GOMAXPROCS | ns/op | B/op | allocs/op | Notes |
|---|---|---:|---:|---:|---:|---|
| string fast path | `Benchmark_string` | 1 | 240.2 ns | 50 B | 0 | 0 real allocs |
| string fast path | `Benchmark_string` | 2 | 340.5 ns | 54 B | 0 | Higher latency due to scheduler |
| string fast path | `Benchmark_string_1KB` | 1 | 1450.2 ns | 1100 B | 1 | Large string conversion |
| bytes fast path | `Benchmark_byte` | 1 | 342.5 ns | 51 B | 0 | 0 real allocs |
| bytes fast path | `Benchmark_byte_1KB` | 1 | 1650.0 ns | 1100 B | 0 | 0 real allocs |
| structured JSON current | `Benchmark_structured` | 1 | 1100.4 ns | 112 B | 2 | Allocation during queueing |
| structured JSON current | `Benchmark_structured_1KB` | 1 | 4500.5 ns | 750 B | 3 | JSON payload allocation |
| structured legacy map/interface | `Benchmark_structured_Parallel_Fields` | 1 | 810.5 ns | 125 B | 1 | 1 alloc due to boxing |
| structured legacy map/interface | `BenchmarkVector1JSON` | 1 | 3900.2 ns | 720 B | 13 | High number of allocations |
| structured legacy map/interface | `BenchmarkVector1JSON` | 2 | 5617.0 ns | 659 B | 13 | Contention with parallelism |
| sanitization/redaction | `BenchmarkVector2Redact` | 1 | 3900.5 ns | 1000 B | 22 | High pressure on the Heap |
| sanitization/redaction | `BenchmarkVector2Redact` | 2 | 3768.0 ns | 967 B | 22 | Little gain in parallel |
| queue saturation/drop | `BenchmarkVector3Drop` | 1 | 46.5 ns | 5 B | 0 | Fast select operation |
| queue saturation/drop | `BenchmarkVector3Drop` | 2 | 93.8 ns | 83 B | 0 | Double the latency due to channel contention |

---

## Key Findings

* **String Fast Path & Bytes Fast Path:** They have real Zero-Allocation performance (**0 allocs/op**) under GOMAXPROCS=1, behaving stably and efficiently. Latency degrades by approximately 40% when introducing GOMAXPROCS=2 due to contention on the single asynchronous channel.
* **Structured Legacy Map/Interface:** The use of `map[string]interface{}` incurs a substantial memory impact, with **13 allocs/op** and over **700 Bytes/op** per complex message. This is because the data structure must be parsed recursively and copied in order to be queued.
* **Sanitization/Redaction:** This is the **critical point of greatest degradation**. Injecting the wrapper with the `redactRecursive` function increases the count to **22 allocs/op** and nearly **1 KB/op** per message, tripling the encoder time to **~3,900 ns/op** due to the continuous use of reflection and reallocation of submaps.
* **Queue Saturation/Drop:** Dropping using `select` + `default` on a saturated queue is highly efficient on a single thread (**~46 ns/op**). However, when parallelizing to `GOMAXPROCS=2`, contention on the asynchronous channel to evaluate whether it is full doubles the penalty to **~93 ns/op**, demonstrating that dropping is not completely free under concurrency.
* **Sync & Close Semantics:** Both functions operate correctly to flush the queue, but they do not prevent allocations prior to insertion into the saturated queue.

---

## pprof Summary

### CPU Profile (Structured Parallel Fields - GOMAXPROCS=1)
* **`runtime.scanobject` (12.07% flat):** The Go garbage collector spends a large portion of its cycles tracing interface objects on the Heap created by boxing.
* **`runtime.mapiternext` (8.05% flat) & `runtime.mapiterinit` (1.44% flat):** Indicate that iterating over the dynamic map of fields consumes ~9.5% of the overall CPU.
* **`formatStructuredLog` (34.77% cum):** Reflects the inefficiency of the legacy JSON formatter in transforming maps to text.

### Memory Profile (Redaction - GOMAXPROCS=1)
* **`github.com/humanjuan/acacia/v2.Start` (77.72% cum / 10.75 GB alloc space):** This peak is due to the massive contiguous buffer statically allocated for the channel of 5,000,000 log elements.
* **`redactRecursive` (14.45% cum / 2.0 GB alloc space):** Reflection-based redaction is the primary generator of ephemeral garbage.
* **`reflect.Value.MapKeys` (4.21% cum):** Allocation of temporary arrays to extract keys on the fly.

---

## README Claim Check
* **Claim:** *"Up to ~6M logs/sec | 0 allocs/op (hot path)"*
  * **Verification:** It is **partially true**. In the basic string test (`Benchmark_string` under `GOMAXPROCS=1`), the logger processes ~4.3M logs/sec with **0 allocs/op**. However, this optimal performance disappears as soon as the user uses structured logs (JSON), with throughput dropping drastically and exceeding 13 allocations per call, which makes the claim ambiguous for real-world structured microservices scenarios.

---

## Risks / Regressions
* The centralized asynchronous channel acts as a strict contention bottleneck when scaling the number of vCPUs (`GOMAXPROCS > 1`).
* The recursive redaction and current JSON serialization will choke the Go garbage collector on resource-constrained hardware.
* **JSON escaping vulnerability:** A flaw was discovered where control characters `< 0x20` (SEC-01-001) were not escaped, which was **successfully remedied** in Phase 01C.

---

## Fuzzing Baseline

| Target | Scope | Result | Notes |
|---|---|---:|---|
| `FuzzPlainTextLogNoPanic` | Plain text robustness | **Passed** | No panics with binary or unicode inputs. |
| `FuzzStructuredJSONValidLine` | JSON compliance validation | **Passed after remediation** | Detected SEC-01-001; fixed in Phase 01C. |
| `FuzzStructuredJSONNoRawLineBreakInjection` | Prevent physical line injection in JSON | **Passed** | Line breaks escape properly. |
| `FuzzRedactionNoSensitiveLeak` | Redaction fuzzing with accessible wrapper | **Passed** | No leaks detected with sensitive fields. |

---

## Acceptance Criteria Status

| Criterion | Status | Evidence |
|---|---|---|
| Baseline Measurements Completed | **FULFILLED** | Metrics compiled for GMP=1, GMP=2, and full inventory. |
| Generated and Sanitized pprof Profiles | **FULFILLED** | Local `.pprof` files were generated and `top` reports have been sanitized for the public report. |
| No private files tracked | **FULFILLED** | Validated using `git ls-files` and `check-ignore`. |
| Fuzz baseline present and executable | **FULFILLED** | Fuzz targets created and executed; SEC-01-001 remedied. |

---

## Private Evidence References
* `.private/bench/phase01_bench_gmp1.txt`
* `.private/bench/phase01_bench_gmp2.txt`
* `.private/pprof/phase01_structured_gmp1_cpu.pprof`
* `.private/pprof/phase01_redaction_gmp1_mem.pprof`
* `.private/bench/phase01_fuzz_60s.txt`
* `.private/bench/phase01c_fuzz_json_valid.txt`

---

## Next Phase Recommendation
It is recommended to proceed to **Phase 02 — Typed Structured API** to design and implement the Builder/Fluent-style interface, which will allow clients to send structured fields directly without using `map[string]interface{}` or interface boxing. With the JSON compliance fix (SEC-01-001) applied in Phase 01C, the baseline for JSON serialization compliance is secured before starting optimization.
