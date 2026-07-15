# Phase 02 — Typed Structured API Report

*This is a historical engineering report. The filename is kept for audit continuity.*

* **Date:** 2026-07-07
* **Branch:** `dev`
* **Commit:** `13b5b5b fix: remediate JSON control character escaping vulnerability and document Phase 01 baseline`
* **Go version:** `go1.23.2 darwin/amd64`
* **OS/Arch:** `darwin/amd64`
* **Scope:** Design and implementation of the new typed structured Fluent API (Builder).

---

## Objective
Design and implement a typed structured Fluent/Builder API for Acacia that enables high-performance structured logging without converting to interfaces (`interface{}`), dynamic mapping (`map[string]interface{}`), reflection, or dynamic JSON marshaling on the hot path. All of this must coexist without breaking backward compatibility with the classic v2.x API.

---

## API Compatibility Decision
Since method overloading is not permitted in Go, it was not possible to redefine `Info(data interface{}, args ...interface{})` to return a builder. A typed API for structured fields was designed.
Explicit event methods were introduced instead:
* `Event(level string) *Event` (Generic)
* `InfoEvent() *Event`
* `WarnEvent() *Event`
* `ErrorEvent() *Event`
* `DebugEvent() *Event`
* `CriticalEvent() *Event`

This nomenclature is clean, compatible, and follows the industry standard (e.g., zerolog/zap), allowing users to gradually migrate their critical paths to the new typed API while keeping the rest of their application on the traditional legacy API.

Design Note: The generic `Event(level string)` method receives the level as a string to maintain compatibility with Acacia's current representation of levels, but is established as an evaluation point for transitioning to a stricter (typed) API in v3 or a later phase.

---

## Public API Added
The following public methods were added to `Log`:
* `func (l *Log) Event(level string) *Event`
* `func (l *Log) InfoEvent() *Event`
* `func (l *Log) WarnEvent() *Event`
* `func (l *Log) ErrorEvent() *Event`
* `func (l *Log) DebugEvent() *Event`
* `func (l *Log) CriticalEvent() *Event`

And the `Event` struct with its chainable builder methods:
* `func (e *Event) Str(key, val string) *Event`
* `func (e *Event) Int(key string, val int) *Event`
* `func (e *Event) Int64(key string, val int64) *Event`
* `func (e *Event) Uint64(key string, val uint64) *Event`
* `func (e *Event) Bool(key string, val bool) *Event`
* `func (e *Event) Float64(key string, val float64) *Event`
* `func (e *Event) Err(err error) *Event`
* `func (e *Event) Dur(key string, val time.Duration) *Event`
* `func (e *Event) Time(key string, val time.Time) *Event`
* `func (e *Event) Secret(key, val string) *Event` (Initial redaction Phase 02)
* `func (e *Event) StrRedacted(key, val string) *Event`
* `func (e *Event) Msg(msg string)` (Finalizes and sends the log)
* `func (e *Event) Send()` (Finalizes and sends the log without a main message)

---

## Implementation Summary
* **Reusable Buffers:** `Event` retrieves byte buffers `[]byte` from the internal Acacia pool (`getJSONBufCap`) and writes the JSON payload incrementally using fast methods (`strconv.AppendInt`, `escapeJSONInto` without allocations).
* **Struct Pool:** `Event` structs are retrieved from and returned to a `sync.Pool` (`eventPool`), reducing the control allocation to a maximum of **1 alloc/op** due to the pool's interface escape.
* **Early Evaluation:** If the log level is disabled, `NewEvent` immediately returns `nil`. Chained calls on a `nil` pointer are discarded at no cost, resulting in **0 allocs/op** and zero computation.
* **Integration:** The serialized event is queued as a message of type `kind: 2` (preformatted JSON), reusing the existing asynchronous channel and disk writer backend.

---

## Files Changed
* **acacia.go**: `Event` struct and initialization/writing methods.
* **test/phase02_event_builder_test.go**: Suite of conformity tests, escaping control, coexistence, and rotation.
* **test/phase02_event_builder_benchmark_test.go**: Benchmark suite.
* **test/phase02_event_builder_fuzz_test.go**: Fuzzing for line injection, valid JSON, and no leakage of secrets.

---

## Tests Executed

| Command | Result | Notes |
|---|---:|---|
| `go test ./...` | **PASS** | Full suite passes. |
| `go test -race ./...` | **PASS** | No race conditions detected. |

---

## Benchmark Summary

### Scenario with GOMAXPROCS=1
| Benchmark | GOMAXPROCS | ns/op | B/op | allocs/op | Compared To | Notes |
|---|---:|---:|---:|---:|---|---|
| `BenchmarkEventBuilderDisabledLevel` | 1 | 18.9 ns | 0 B | 0 | `Benchmark_structured` | Disabled level discard |
| `BenchmarkEventBuilderSmall` | 1 | 709.1 ns | 297 B | 1 | `BenchmarkVector1JSON` | 13x fewer allocations |
| `BenchmarkEventBuilderStringFields` | 1 | 685.6 ns | 248 B | 1 | `BenchmarkVector1JSON` | 13x fewer allocations |
| `BenchmarkEventBuilderMixedFields` | 1 | 758.8 ns | 269 B | 1 | `BenchmarkVector1JSON` | 13x fewer allocations |
| `BenchmarkEventBuilder1KB` | 1 | 14371.0 ns | 35247 B | 10 | `BenchmarkVector1JSON` | Latency/memory regression |
| `BenchmarkVector1JSON` (Legacy Map) | 1 | 3541.0 ns | 750 B | 13 | Previous baseline | 13 allocations due to boxing |

### Scenario with GOMAXPROCS=2 (Low Parallelism)
| Benchmark | GOMAXPROCS | ns/op | B/op | allocs/op | Compared To | Notes |
|---|---:|---:|---:|---:|---|---|
| `BenchmarkEventBuilderDisabledLevel` | 2 | 21.4 ns | 0 B | 0 | `Benchmark_structured` | Ultra-fast discard |
| `BenchmarkEventBuilderSmall` | 2 | 636.6 ns | 209 B | 1 | `BenchmarkVector1JSON` | 13x fewer allocations |
| `BenchmarkEventBuilderStringFields` | 2 | 937.0 ns | 155 B | 1 | `BenchmarkVector1JSON` | 13x fewer allocations |
| `BenchmarkEventBuilderMixedFields` | 2 | 1298.0 ns | 172 B | 1 | `BenchmarkVector1JSON` | 13x fewer allocations |
| `BenchmarkEventBuilder1KB` | 2 | 16588.0 ns | 31817 B | 10 | `BenchmarkVector1JSON` | Latency/memory regression |
| `BenchmarkVector1JSON` (Legacy Map) | 2 | 4713.0 ns | 648 B | 13 | Previous baseline | 13 allocations due to boxing |

---

## Fuzzing Summary

| Target | Result | Notes |
|---|---:|---|
| `FuzzEventBuilderJSONValid` | **PASS** | Generates JSON lines conforming to RFC 8259. |
| `FuzzEventBuilderNoLineInjection` | **PASS** | Avoids physical line injection via line break. |
| `FuzzEventBuilderSecretNoLeak` | **PASS** | Secret values are correctly masked as `[REDACTED]`. |

---

## Hot Path Restrictions Check
The critical path of the new structured Fluent API:
* **Does it use reflect?** **NO**.
* **Does it use fmt.Sprint/Sprintf?** **NO**.
* **Does it use encoding/json.Marshal?** **NO**.
* **Does it use map[string]interface{}?** **NO**.

---

## Acceptance Criteria Status

| Criterion | Status | Evidence |
|---|---|---|
| Fluent API compatible with v2 implemented | **CUMPLIDO** | `InfoEvent` and builder methods added in `acacia.go`. |
| Disabled level at 0 allocs/op | **CUMPLIDO** | `BenchmarkEventBuilderDisabledLevel` returns 0 allocs/op. |
| Builder with maximum 1 alloc/op in standard cases | **CUMPLIDO** | `BenchmarkEventBuilderSmall` reports 1 alloc/op. |
| Robust fuzzing suite integrated | **CUMPLIDO** | New fuzz targets created and passed successfully. |

---

## Risks / Regressions
* **Regression in Large Logs (1KB):** The new typed structured API exhibits a **latency and memory consumption regression** in large messages (as in `BenchmarkEventBuilder1KB`), due to dynamic buffer resizing through successive Go `append` operations (10 allocations and 35 KB/op). The builder is ideal for the common `small/mixed` case, but this inefficient behavior on massive payloads remains registered as technical debt to be resolved in **Phase 03** (Append-only encoder) and **Phase 05** (Static buffer pools).

---

## Private Evidence References
* `.private/bench/phase02_builder_bench_gmp1.txt`
* `.private/bench/phase02_builder_bench_gmp2.txt`
* `.private/reports/phase-02-private-evidence.md`

---

## Next Phase Recommendation
It is recommended to proceed to **Phase 03 — Append-Only JSON Encoder** to optimize and unify buffer pools, which will mitigate dynamic buffer growth and consolidate a high-performance centralized encoder for massive scenarios.
