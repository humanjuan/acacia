# Release Candidate Hard Performance Validation Report

## Objective
The objective of this report is to document the results of the hard performance validation executed on a temporary SSD clone of the Acacia repository, assessing the stability, regression footprint, and overall safety of the Release Candidate (RC) prior to tagging.

## Scope
- Functional and concurrency validation (race detector, stress testing, fuzzing).
- Micro-benchmarking comparison between the current release candidate and the stable baseline.
- Verification of new features (Builder API, Redaction, Backpressure, Filesystem safety, Sync/Close semantics).
- Performance regression identification.

## Environment Summary
The validation was conducted under the following standardized environment:
- **Operating System / Architecture**: macOS / darwin amd64
- **Processor**: Intel i7
- **Storage**: Local Internal SSD
- **Go Version**: Go 1.23.2
- **Concurrency Profiles**: `GOMAXPROCS` set to 1, 2, and system default.

> [!NOTE]
> All paths, usernames, and local folder structures have been sanitized. No absolute paths or local filesystem references are included in this report.

## Baseline Selection
- **Selected Baseline**: `v2.3.0`
- **Rationale**: 
  - Version `v2.3.1` was reviewed but showed a daily rotation test failure, rendering it unsuitable as a stable performance baseline.
  - Version `v2.3.0` compiles cleanly, passes all unit tests, and represents the stable baseline pre-dating the roadmap improvements.

## Methodology
- **Repetitions**: `count=20` to reduce variance and scheduling noise from the OS.
- **Comparison**: Benchmark current Release Candidate vs `v2.3.0`.
- **Fuzzing**: 60 seconds minimum per fuzz target to verify robustness under malformed inputs.
- **Stress**: Concurrent stress validation suite to ensure thread safety under high contention.
- **Race Detection**: Verification with `go test -race ./...`.
- **Evaluation Method**: Due to the temporary absence of `benchstat` in the SSD environment, a custom Python comparison script was used as a fallback. 
- **Future Requirement**: Running standard `benchstat` remains a strict prerequisite for repeating or finalizing this performance validation.

## Old vs New Findings
The following table summarizes the performance metrics comparing the baseline `v2.3.0` against the current Release Candidate.

| Benchmark | Baseline (`v2.3.0`) | Current RC | Status / Comparison |
|---|---|---|---|
| `Benchmark_byte` | Measured | Measured | Regression (~ +22.75% latency) |
| `Benchmark_string` | Measured | Measured | Regression (~ +21.10% latency) |
| `Benchmark_byte_1KB` | Measured | Measured | Major regression (~ +92% bytes / high B/op) |
| `Benchmark_string_1KB` | Measured | Measured | Major regression (high B/op / latency) |
| `Benchmark_structured` | Measured | Measured | Builder path is more optimal than legacy map path |
| `BenchmarkEventBuilderSmall` | N/A | Measured | *Current-only* (Builder API) |
| `BenchmarkRotationSizeSmall` | N/A | Measured | *Current-only* (Filesystem safety) |
| `BenchmarkSyncEmpty` | N/A | Measured | *Current-only* (Sync semantics) |
| `BenchmarkCloseEmpty` | N/A | Measured | *Current-only* (Close semantics) |

> [!IMPORTANT]
> - New features such as the structured Builder, redaction, backpressure, Sync/Close barriers, and filesystem hardening are marked as *current-only* because they did not exist in `v2.3.0`.
> - The new JSON Builder API validates our technical direction, showing improvements over the legacy map path, though it is still subject to the regressions noted below.

## Validated Claims
- **Byte Fast Path**: Approximately ~6M ops/sec was observed during this specific run on the byte fast path.
- **Allocation Behavior**: 0 allocs/op observed in the measured hot paths (plain string/byte writes, disabled level, post-close/drop paths). However, `B/op` can be greater than 0 under certain payload sizes.
- **Event Builder**: The new structured builder consumes ~1 alloc/op (under `sync.Pool` control).
- **Legacy Map**: Remains a significant performance debt, validating the migration path towards the builder.
- **Sync/Close Barrier**: Functionally validated; successfully ensures delivery of events accepted before `Sync()` or `Close()` under high concurrency.

## Regressions Detected
- **Raw String/Bytes**: Approximately +21% to +23% latency regression compared to `v2.3.0`.
- **1KB Payloads**: Significant regression observed (up to ~+92% in bytes/op and overall latency).
- **Memory Overhead**: Sharp increase in `B/op` for large payloads.
- **Suspected Causes**:
  1. Overhead introduced by atomic checks and state management on the hot path.
  2. Buffer growth inefficiencies and memory allocation in `appendLineBytes` or the underlying buffer manager under write pressure.

## Coverage Gaps
- **Benchstat Analysis**: Lack of direct `benchstat` execution.
- **I/O Separation**: The `io.Writer` mode was not benchmarked as a separate isolation layer.
- **Daily Rotation**: Verified functionally but lacks formal `ns/op`, `B/op`, or `allocs/op` profiling.
- **Backup Rotation**: Not measured against real-world backup cardinality.
- **Platform Coverage**: Testing was restricted to macOS Intel; missing validation on Linux (amd64/arm64), darwin arm64, and Windows.
- **Soak Testing**: Long-duration soak testing under memory limits is pending.

## Release Interpretation
- **Conclusion**: **Ready for tag decision with documented performance regressions.**
- The release candidate is functionally stable, passes all safety, fuzzing, and concurrency tests, and contains critical security fixes. However, tagging the final release requires conscious acceptance of the documented performance regressions by the maintainer.
- No final release or tag is recommended without the explicit decision of the maintainer.

## Next Step
- Proceed to **Fase 09 — Performance Recovery, Architecture Review & OpenTelemetry Compatibility** to isolate, diagnose, and optimize these regressions before finalizing the release.
