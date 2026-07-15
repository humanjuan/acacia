# Quality and Validation

## Current Validation Status

Acacia has completed the structured JSON capabilities maturity work and the Final SSD Hard Validation run for the v2.4.0-rc.1 release candidate.

---

## What Was Validated

* **Functional Correctness:** `go test ./...` passed under local execution.
* **Concurrency Safety:** `go test -race ./...` passed under local execution (no data races were observed in the executed suites).
* **Fuzzing Gate:** 7 selective fuzz targets executed for 60 seconds each to verify input escaping and recursive redaction behavior:
  1. `FuzzPhase10FlatJSONStringEscaping` -> PASS
  2. `FuzzPhase10ObjectBuilderEscapingAndRedaction` -> PASS
  3. `FuzzPhase10ArrayBuilderEscapingAndObjectRedaction` -> PASS
  4. `FuzzPhase10NamespaceEscapingAndRedaction` -> PASS
  5. `FuzzPhase10RawJSONValidityAndSingleLineSafety` -> PASS
  6. `FuzzPhase10AnyValidityRedactionAndSingleLineSafety` -> PASS
  7. `FuzzPhase10RecursiveRedactionAuditSensitiveKeys` -> PASS
* **SSD Hard Validation Benchmarks:** Executed with 10 iterations per case on the internal SSD of the host to compare baseline performance trends.
* **Coverage Scope:** Validation paths covered:
  * Flat structured JSON builders
  * Nested object and array builders
  * Group and Namespace scopes
  * RawJSON injection and Any fallback paths
  * Plain byte and string hot paths
  * Median payload sizes (1KB)
  * Legacy structured map compat paths
  * Lifecycle operations (post-close and concurrent sync calls)

---

## Benchmark Snapshot

The following benchmarks are observations from the Final SSD Hard Validation run on a local SSD environment. They are useful for comparison and regression review, and do not constitute a universal performance guarantee.

| Case | Observed Result (darwin/amd64) |
| :--- | :--- |
| **Direct `[]byte`** | 181.0 ns/op \| 0 B/op \| 0 allocs/op |
| **Direct `string`** | 183.7 ns/op \| 0 B/op \| 0 allocs/op |
| **Direct `[]byte` 1KB** | 926.3 ns/op \| 0 B/op \| 0 allocs/op |
| **Direct `string` 1KB** | 937.2 ns/op \| 16 B/op \| 1 alloc/op |
| **Flat JSON Builder** | 522.0 ns/op \| 29 B/op \| 1 alloc/op |
| **Object Builder** | 611.6 ns/op \| 42 B/op \| 2 allocs/op |
| **RawJSON** | 539.9 ns/op \| 26 B/op \| 1 alloc/op |
| **Any** | 3229.7 ns/op \| 1635 B/op \| 25 allocs/op |
| **Disabled Level Short-Circuit** | 13.4 ns/op \| 0 B/op \| 0 allocs/op |
| **Legacy Map Structured Baseline** | 1126.1 ns/op \| 323 B/op \| 2 allocs/op |

---

## Boundaries

* **Environment Specificity:** Benchmark metrics are local environment-specific observations and will vary depending on system hardware, CPU scheduling, and disk I/O performance.
* **Concurrency Bug Limits:** Passing the Go race detector suite indicates no data races were observed under the tested workloads; it is not a formal mathematical proof of the complete absence of all possible concurrency edge cases.
* **Fuzzing Coverage:** Fuzz testing improves input boundary coverage but does not constitute a formal proof of safety under all inputs.
* **Redaction Policy:** Sensitive data redaction is robust under the audited pattern matchers; it does not represent a formal perfect redaction guarantee over arbitrary, complex custom structs or pointer networks.
* **Production Validation:** Downstream production confidence requires integration and validation in your specific services.

---

## Reports

* [Final SSD Hard Validation Report](reports/final-ssd-hard-validation-report.md)
* [Structured JSON Integration Report](reports/structured-json-integration-validation-report.md)
* [Documentation and Examples Audit Report](reports/structured-json-documentation-examples-report.md)
