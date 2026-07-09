# Fase 09 — Performance Recovery, Architecture Review & OpenTelemetry Compatibility

## Purpose
Fase 09 has been established to systematically analyze and recover the performance characteristics of Acacia to match or exceed the v2.3.0 baseline. The primary goal is to achieve this recovery without sacrificing the critical security controls, concurrency stability, sanitization/redaction rules, filesystem hardening, backpressure policies, or Sync/Close semantics implemented during Fases 01–08.

## Background
The hard performance validation conducted on a dedicated SSD clone identified stability under high concurrency, but documented noticeable regressions:
- **Raw string/bytes paths**: An increase of +21% to +23% in latency compared to the v2.3.0 baseline.
- **Large payloads (1KB)**: A significant regression, showing up to +92% increased overhead in bytes and a sharp increase in bytes allocated per operation (`B/op`).
- **New Features Validation**: The new JSON Builder API validates our design direction by significantly outperforming the legacy map path, and all fuzzing, stress, and race detector suites pass successfully.
- **Gaps**: A lack of systematic `benchstat` execution, isolation of the `io.Writer` layer, backup/daily rotation performance profiling, and multi-platform validation.

## Non-Negotiable Requirements
Any modification or optimization introduced during this phase must preserve the following behaviors and semantics:
- **JSON Escaping**: Secure and valid escaping of control characters and inputs.
- **Redaction**: Strict, zero-leak redaction of sensitive fields.
- **Path Traversal Prevention**: Absolute rejection of traversal sequences in log filenames.
- **Symlink Protection**: Robust verification to prevent symlink bypasses on active and rotated logs.
- **Secure Permissions**: Strict filesystem masks (e.g., `0600` for active logs).
- **Backpressure Correctness**: Fully functional `BlockOnFull`, `DropNewest`, `DropOldest`, and `DropDebugOnly` policies.
- **Sync/Close Barrier**: Thread-safe synchronization and zero-loss barrier for events accepted before closure.
- **Post-Close Safety**: Post-close writes must be handled as safe no-op/drop-accounted operations, without blocking or panicking.
- **Race and Fuzz Cleanliness**: Zero Go race detector warnings and clean fuzz testing results.
- **API Compatibility**: Backward compatibility with public v2.x APIs.
- **OpenTelemetry (OTel)**: Compatibility must be optional, with no measurable overhead in documented benchmarks on the core logger when disabled.

---

## Workstream 09A — Regression Diagnosis
The objective of this workstream is to isolate the exact sources of latency and allocation regressions.

### Proposed Testing & Benchmarking Files
- `test/phase09_atomic_cost_benchmark_test.go`
- `test/phase09_append_line_bytes_benchmark_test.go`
- `test/phase09_buffer_growth_benchmark_test.go`
- `test/phase09_writer_flush_benchmark_test.go`
- `test/phase09_queue_enqueue_benchmark_test.go`
- `test/phase09_state_check_benchmark_test.go`
- `test/phase09_stats_counter_benchmark_test.go`

### Proposed Benchmarks
- `BenchmarkAtomicStateLoad`: Quantify the CPU cost of atomics used for state checking.
- `BenchmarkAtomicEnqueuedSeqAdd` / `BenchmarkAtomicDroppedSeqAdd`: Assess the performance footprint of queue counters.
- `BenchmarkAtomicMultiCounterFalseSharing`: Diagnose whether multiple atomic variables share the same cache line.
- `BenchmarkAppendLineBytesSmall` / `BenchmarkAppendLineBytes1KB` / `BenchmarkAppendLineBytes4KB` / `BenchmarkAppendLineBytes8KB`: Profile the performance of the appending logic across variable payload sizes.
- `BenchmarkAppendLineBytesWithPreGrow`: Measure if growing the slice buffer early prevents incremental allocations.
- `BenchmarkAppendLineBytesUnderWriterLag`: Simulate consumer slow-downs to measure the impact of buffer pressure.
- `BenchmarkQueueEnqueueOpenState`: Benchmarking queue enqueue latency in the normal state.
- `BenchmarkQueueDropNewestFastPath`: Measure the cost of dropping events on a saturated queue.
- `BenchmarkWriterFlushSmallBatch` / `BenchmarkWriterFlushLargeBatch`: Measure write batching efficiency.

---

## Workstream 09B — Architecture Review
We will evaluate the following structural options to resolve the regressions:
1. **Incremental Optimization**: Fine-tuning current code (e.g., pre-growing slices, inline optimization, reducing atomic lookups).
2. **Writer-Owned Sequencing**: Moving state transitions and sequencing logic into the background writer goroutine to free the producer hot path from synchronization overhead.
3. **Multi-Mode Engine**: Providing optimized fast-path engines for simple logging modes (e.g., bypassing advanced features when they are not configured).
4. **Partial Rebuild**: Reconstructing parts of the writer pipeline, buffer manager, or sequencing logic. If diagnostics show that the regressions stem from core sequencing design, a partial rebuild is recommended.

---

## Workstream 09C — Performance Recovery
Optimizations will target the following priorities:
1. **Large Payload Optimization**: Minimize buffer growth overhead for messages >= 1KB.
2. **Atomic/Checks Reduction**: Inline or cache state checks to avoid repeated atomic loads in the hot path.
3. **JSON Builder Improvements**: Refine the builder's allocation characteristics and pool recycling.
4. **Rotation Allocation Reduction**: Minimize allocations during log rotation.
5. **False Sharing Elimination**: Ensure hot-path state fields are aligned to cache-line boundaries (e.g., adding padding).

### Targets
- Recover at least **50% of the regression** on raw string/bytes paths.
- Maintain **0 allocs/op** on the hot paths.
- Reduce `B/op` for 1KB payloads by at least **60%**.
- Ensure the builder performance is equal to or better than the current baseline.
- Do not disrupt Sync/Close correctness or backpressure behaviors.

---

## Workstream 09D — Security & Stability Regression Guard
To ensure optimizations do not introduce security or concurrency regressions, a strict suite of guard tests will be defined.

### Proposed Regression Test Files
- `test/phase09_security_regression_test.go`
- `test/phase09_sync_close_regression_test.go`
- `test/phase09_filesystem_regression_test.go`
- `test/phase09_redaction_regression_test.go`

### Proposed Regression Tests
- `TestPhase09NoJSONInjectionRegression`: Verify control characters are escaped correctly under optimized paths.
- `TestPhase09NoSecretLeakRegression`: Ensure redacted keywords are not written to the output.
- `TestPhase09NoPathTraversalRegression`: Validate that directory traversal escapes are rejected.
- `TestPhase09NoSymlinkRegression`: Test that symlink checks remain active.
- `TestPhase09PostCloseNoPanicRegression`: Guarantee that writing after `Close()` does not cause a panic.
- `TestPhase09SyncBarrierRegression`: Stress test concurrent `Sync()` calls under maximum load.
- `TestPhase09CloseAcceptedEventsRegression`: Confirm that all events enqueued prior to `Close()` are fully persisted.

---

## Workstream 09E — OpenTelemetry Compatibility Evaluation
We will plan the integration model for OpenTelemetry (OTel) compatibility.

### Design Principles
- **Optional Dependency**: OTel must not be a required dependency of the core logger module.
- **No-Measurable-Overhead Core Target**: When OTel is disabled, the core logger must be validated to ensure no measurable overhead in documented benchmarks.
- **Optional Adapter**: OTel integration will be exposed as an optional adapter.
- **Redaction Boundary**: Sensitive data redaction must occur before events are sent to the exporter.
- **Fault Isolation**: Failures in the OTel exporter must not interrupt local filesystem logging.
- **Backpressure Isolation**: OTel backpressure must not block Acacia operations unless explicitly configured.

### Evaluated Models
1. **slog.Handler Compatibility**: Wrapping Acacia to expose a standard `slog.Handler` that OTel bridges can consume.
2. **Optional OTel Sink/Exporter**: Implementing an internal exporter adapter that forwards logs to the OTel collector.
3. **Correlation-Only Fields**: Providing methods on the Builder to inject trace and span IDs (`TraceID`, `SpanID`) into the structured JSON output without full SDK integration.
4. **Separate Module**: Placing the adapter in a separate module (e.g., `github.com/humanjuan/acacia/integrations/otel`) or shielding it with Go build tags.

### Proposed Integration Files
- `integrations/otel/otel_adapter_test.go`
- `integrations/otel/otel_adapter_benchmark_test.go`
- `integrations/otel/otel_adapter_fuzz_test.go`

### Proposed Benchmarks
- `BenchmarkOtelDisabledAdapter`: Validate no measurable overhead in documented benchmarks when the adapter is compiled but disabled.
- `BenchmarkOtelNoopProviderOverhead`: Measure the overhead of passing logs through an OTel no-op provider.
- `BenchmarkOtelAttributeConversionSmall` / `BenchmarkOtelAttributeConversion10Fields`: Measure the latency of converting Acacia event builders to OTel log attributes.
- `BenchmarkOtelTraceContextInjection`: Profile trace/span ID parsing and embedding.
- `BenchmarkOtelExportDropOnFull`: Verify performance under exporter buffer saturation.
- `BenchmarkOtelRedactionPreserved`: Validate that redacted fields are stripped before export.

---

## Workstream 09F — Hard Testing Plan
The validation process must follow a strict, repeatable methodology to confirm success.

### Prerequisites
- **Go Version**: Go 1.23.2
- **Tools**: `benchstat` and `go tool pprof` must be installed.
- **System Environment**:
  - Dedicated hardware environment (no background Docker containers, browsers, or heavy services running).
  - Executed on a local SSD.
  - OS-level sleep prevented (e.g., using `caffeinate` on macOS).

### Testing Checklist
1. Run all unit tests: `go test ./...`
2. Run race detector: `go test -race ./...`
3. Execute performance benchmarks: `count=20` under `GOMAXPROCS` profiles (1, 2, and default).
4. Run fuzzing targets for at least 60 seconds.
5. (Optional) Run extended soak testing by setting the environment variable `ACACIA_HARD_SOAK=1`.
6. Profile bottleneck changes using `pprof` (saving profiles strictly in `.private/pprof/`).
7. Perform `benchstat` analysis comparing the new optimized version against `v2.3.0`.

### Proposed Testing Files
- `test/phase09_hotpath_benchmark_test.go`
- `test/phase09_large_payload_benchmark_test.go`
- `test/phase09_buffer_growth_benchmark_test.go`
- `test/phase09_rotation_matrix_benchmark_test.go`
- `test/phase09_soak_test.go`
- `test/phase09_security_regression_test.go`
- `test/phase09_otel_adapter_benchmark_test.go`

---

## Success Criteria
### Performance
- Recover at least 50% of the raw string/bytes regression.
- Reduce 1KB payload memory usage (`B/op`).
- Maintain 0 allocs/op on hot paths.
- Builder performance is equal to or better than the current Release Candidate state.

### Security
- Fuzzing, race detector, and stress suites are completely clean.
- Zero secret leaks, JSON injections, directory traversals, or symlink bypasses.

### Compatibility
- APIs under v2.x public support compile and function without modification.
- Unsafe inputs continue to fail by design.
- OTel integration operates strictly as an opt-in adapter.

---

## Deliverables
1. **Diagnostic Benchmarks**: Baseline profiles and test results capturing the source of regressions.
2. **Private Profiles**: CPU/Memory profiles stored in `.private/pprof/`.
3. **Public Diagnosis Report**: A sanitized public summary outlining findings.
4. **Architectural ADR**: Finalizing the design decisions in a formal ADR.
5. **Implementation Plan**: Step-by-step description of target changes.
6. **Hard Validation Report**: Comparison against `v2.3.0` using `benchstat`.
7. **Release/Tag Recommendation**: Final assessment on release readiness.

---

## Proposed Sprint Breakdown
- **Sprint 09.1 — Diagnostics**: Write diagnostics benchmarks, capture profiles in `.private/`, and pinpoint buffer/atomic overheads.
- **Sprint 09.2 — Architecture ADR**: Formulate the architectural choices and finalize `0003-acacia-performance-recovery-architecture-options.md`.
- **Sprint 09.3 — Hot-Path and Payload Recovery**: Implement target optimizations, focusing on large payloads, state cache, and false sharing.
- **Sprint 09.4 — OpenTelemetry Compatibility**: Design and evaluate the optional OTel adapter.
- **Sprint 09.5 — Hard Validation Final**: Run the full validation suite, generate `benchstat` comparisons, and draft the final release recommendation.

---

## Release Decision Options
Post-Phase 09, the project maintainer will select one of the following paths:
- **A. Ready for tag after Fase 09**: The regressions are resolved, and the code is cleared for release.
- **B. Ready with documented debt**: The release is tagged with known performance characteristics documented in the release notes.
- **C. Requires partial rebuild**: The architecture requires a deeper pipeline restructure, delaying the release until a future phase.
