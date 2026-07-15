# Phase 04 — Redaction and Sensitive Data Security Report

*This is a historical engineering report. The filename is kept for audit continuity.*

* **Date:** 2026-07-07
* **Branch:** `dev`
* **Commit:** `da9803e perf: implement append-only JSON encoder with benchmarks and fuzzing`
* **Go version:** `go1.23.2 darwin/amd64`
* **OS/Arch:** `darwin/amd64`
* **Scope:** Design and integration of an automatic and explicit redaction system for sensitive data in the structured hot path of Acacia.

---

## Objective
Prevent accidental leaks of secrets in structured logs (e.g., tokens, passwords, cookies) using a case-insensitive and normalized key redaction mechanism (without additional allocations on the builder), without using reflection or regular expressions on the hot path.

---

## Scope Decision
* **Protected:** The hot path of the new typed structured Fluent/Builder API (`Event`). All fields using the typed builder (such as `Str`, `Int`, `Bool`, `Time`, `Dur`, and `ErrKey`) directly benefit from automatic and explicit redaction.
* **Compatibility/Legacy Debt:** The legacy wrapper in `internal/logging/wrapper.go` that uses reflective `redactRecursive` remains intact to avoid breaking legacy clients. Its high CPU/memory cost is recorded under technical debt to be extinguished when the legacy path is completely removed.

---

## Redaction Design
* **Explicit Redaction:** The `Secret(key, val)` and `StrRedacted(key, val)` methods are maintained, directly replacing the value with `"[REDACTED]"` at the byte level, without allocations or scanning.
* **Automatic Redaction by Key:** Each typed field call in `Event` intercepts the key using an ultra-fast internal function `isSensitiveKey(key)`. If a sensitive key is detected, `[REDACTED]` is written immediately and serialization of the original value is aborted.
* **Zero-Allocation Stack-Based Normalization (Zero-Allocation Key Check):** The `isSensitiveKey` function normalizes the key in a case-insensitive manner and discards separating characters (`-`, `_`, `.`) in a local stack variable (64-byte array), avoiding Heap allocations.

---

## Sensitive Key Policy
The following sensitive keywords are protected in any case combination and with any separators:
* `authorization`, `cookie`, `set-cookie`, `token`, `access_token`, `refresh_token`, `password`, `passwd`, `pwd`, `api_key`, `private_key`, `client_secret`, `secret`, `bearer`, `jwt`, `session`, `session_id`, `csrf`, `xsrf`, `x-api-key`.

---

## Public API Impact
* **No Incompatible Changes.** The v2.x API does not undergo signature modifications or broken behavior. `ErrKey(key, err)` was added in `Event` to support error mapping under custom sensitive keys.

---

## Files Changed
* **acacia.go**: Addition of the case-insensitive normalizer `isSensitiveKey`, the formatting helper `appendRedactedField`, and the integration of the automatic filter in all structured field methods of `Event`.
* **test/phase04_redaction_test.go**: Unit conformity test suite.
* **test/phase04_redaction_benchmark_test.go**: Redaction benchmark suite.
* **test/phase04_redaction_fuzz_test.go**: Offensive fuzzing suite for injection and bypass detection.

---

## Tests Executed

| Command | Result | Notes |
|---|---:|---|
| `go test ./...` | **PASS** | Acacia test suite passes. |
| `go test -race ./...` | **PASS** | No race conditions. |

---

## Fuzzing Summary

| Target | Result | Notes |
|---|---:|---|
| `FuzzRedactionSensitiveKeysNoLeak` | **PASS** | Validates that no sensitive key variant leaks its value. |
| `FuzzRedactionKeyVariantsNoBypass` | **PASS** | Validates that hostile normalizations do not bypass the redactor. |
| `FuzzRedactionMixedTypesNoLeak` | **PASS** | Validates that non-string types (Int, Bool, etc.) are securely redacted. |
| `FuzzRedactionNonSensitivePreserved` | **PASS** | Validates that regular non-sensitive data remains unaltered. |

---

## Benchmark Summary

### GOMAXPROCS=1
| Benchmark | ns/op | B/op | allocs/op | Compared To | Notes |
|---|---:|---:|---:|---|---|
| `BenchmarkRedactionDisabledLevel` | 16.3 ns | 0 B | 0 | `Phase 03` | Zero allocations |
| `BenchmarkRedactionExplicitSecret` | 634.3 ns | 282 B | 1 | `Phase 03` | No additional cost |
| `BenchmarkRedactionAutoSensitiveStr` | 647.5 ns | 219 B | 1 | `Phase 03` | Automatic detection in 1 alloc |
| `BenchmarkRedactionAutoSensitiveMixed` | 736.5 ns | 284 B | 1 | `Phase 03` | Redacts 4 keys in 1 alloc |
| `BenchmarkRedactionNonSensitiveStr` | 641.0 ns | 231 B | 1 | `Phase 03` | Normal path without penalty |
| `BenchmarkRedactionKeyCheckSensitive` | 18.0 ns | 0 B | 0 | - | No allocations |
| `BenchmarkRedactionKeyCheckNonSensitive`| 19.2 ns | 0 B | 0 | - | No allocations |
| `BenchmarkRedactionManyFields` | 768.9 ns | 234 B | 1 | - | Buffer reuse |

### GOMAXPROCS=2
| Benchmark | ns/op | B/op | allocs/op | Compared To | Notes |
|---|---|---:|---:|---|---|
| `BenchmarkRedactionDisabledLevel` | 21.4 ns | 0 B | 0 | `Phase 03` | Fast discard |
| `BenchmarkRedactionExplicitSecret` | 727.1 ns | 246 B | 1 | `Phase 03` | No memory impact |
| `BenchmarkRedactionAutoSensitiveStr` | 791.1 ns | 245 B | 1 | `Phase 03` | 1 single allocation |
| `BenchmarkRedactionAutoSensitiveMixed` | 806.6 ns | 225 B | 1 | `Phase 03` | 1 single allocation |
| `BenchmarkRedactionNonSensitiveStr` | 993.6 ns | 233 B | 1 | `Phase 03` | 1 single allocation |

---

## Legacy Redaction Status
* The `redactRecursive` function of the legacy wrapper (which consumes **22 allocations** and **980 B/op** due to extensive use of reflection and dynamic mapping) remains in **`internal/logging/wrapper.go`** to support backward compatibility, but its use is not recommended on the high-performance hot path.

---

## pprof Summary
The allocation profiler for automated redaction shows that:
* The `isSensitiveKey` call registers no Heap memory usage (**0 B/op**).
* The entire allocated Heap corresponds to the generic `sync.Pool` pointer returned in `NewEvent`.
* In the direct comparison of mixed redaction:
  * **Legacy (`BenchmarkVector2Redact`):** Allocates 1.96 GB of accumulated Heap in profiles.
  * **New (`BenchmarkRedactionAutoSensitiveMixed`):** Allocates 0.67 GB of accumulated Heap (65% less allocated memory and 22x fewer net allocations).

---

## Hot Path Restrictions Check
The typed structured redaction path:
* **Does it use reflect?** **NO**.
* **Does it use regexp?** **NO**.
* **Does it use strings.ToLower with allocation?** **NO**.
* **Does it use fmt.Sprint/fmt.Sprintf?** **NO**.
* **Does it use encoding/json.Marshal?** **NO**.
* **Does it use map[string]interface{}?** **NO**.

---

## Security Findings
* **None.** No vulnerabilities or leaks (*bypasses*) were detected after the complete execution of the fuzzing engine.

---

## Acceptance Criteria Status

| Criterion | Status | Evidence |
|---|---|---|
| Inline redaction without additional allocations on the builder | **FULFILLED** | `isSensitiveKey` returns 0 allocs/op; the complete builder flow maintains 1 alloc/op via `sync.Pool`. |
| Case/separator-insensitive denylist without Heap usage | **FULFILLED** | `isSensitiveKey` returns 0 allocs/op. |
| Automatic redaction for non-string types | **FULFILLED** | Validated in `TestRedactionMixedTypes`. |
| Complete offensive fuzzing in the green | **FULFILLED** | Bypass and leak fuzzing completed. |

---

## Risks / Regressions
* **Behavioral Difference:** The new builder applies automatic redaction to registered sensitive keys immediately, whereas the legacy API does not implement an automatic denylist by default (only optional recursive redaction on wrapper maps). This is documented and accepted as a design constraint for coexistence in v2.

---

## Private Evidence References
* `.private/reports/phase-04-private-evidence.md`
* `.private/bench/phase04_redaction_bench_default.txt`
* `.private/bench/phase04_redaction_bench_gmp1.txt`
* `.private/bench/phase04_redaction_bench_gmp2.txt`
* `.private/pprof/phase04_redaction_cpu.pprof`
* `.private/pprof/phase04_redaction_mem.pprof`

---

## Next Phase Recommendation
It is recommended to proceed to **Phase 05 — Backpressure, Queue, and Memory** to redesign the asynchronous event channel and resolve lock contention, allowing multitasking performance to be maximized in Acacia.
