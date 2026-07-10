# Phase 09 Diagnostics Report

## Objective
The objective of this diagnostics sprint was to isolate, reproduce, and identify the root causes of the documented performance regressions in the Acacia Release Candidate (RC) compared to the stable baseline `v2.3.0` (namely, +21% to +23% latency on raw string/bytes paths and heavy degradation/high allocation count on payloads of 1KB and larger).

## Scope
- Diagnostic profiling of atomic operations, state checks, and counter contention.
- Micro-benchmarking slice append operations and dynamic buffer growth.
- Evaluating the impact of background writer lag and batch flushing behavior.
- Isolating queue enqueuing cost and backpressure policy overhead.
- Profiling E2E hot paths for string/byte payloads of different sizes.
- Assessing the overhead of the `io.Writer` interface boundary.
- **Out of Scope**: Implementing active optimizations, changing production code logic, or publishing version tags.

## Environment
- **Operating System / Architecture**: macOS (Darwin Kernel Version 24.6.0) / amd64
- **Processor**: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
- **Execution Location**: Executed on a primary repository copy located on an **external disk**.
- **Go Version**: `go1.23.2`
- **Concurrency Profiles**: `GOMAXPROCS` set to 1, 2, and default.
- **Statistical Tool**: `benchstat` available.
- **External Disk Caveat**: Since this run was performed on an external disk, raw latency figures include storage scheduling and connection overheads. These results are valuable for diagnostic comparison and allocation analysis, but **do not represent final release performance**. The final release candidate performance validation must be repeated on the internal SSD of the machine under a controlled environment.

## Documents Reviewed
- `README.md`
- `CHANGELOG.md`
- `RELEASE_NOTES.md`
- `docs/README.md`
- `docs/ACACIA_PROJECT_CONTEXT.md`
- `docs/ACACIA_IMPROVEMENT_ROADMAP.md`
- `docs/PHASE_09_PERFORMANCE_RECOVERY_PLAN.md`
- `docs/PERFORMANCE_ENGINEERING_PLAN.md`
- `docs/TESTING_AND_BENCHMARKING.md`
- `docs/MIGRATION_STRATEGY.md`
- `docs/PUBLIC_PRIVATE_DOCUMENTATION_POLICY.md`
- `docs/REPORTING_GUIDE.md`
- `docs/SECURITY_HARDENING_PLAN.md`
- `docs/reports/README.md`
- `docs/reports/release-candidate-hard-performance-validation-report.md`
- `docs/reports/phase-08-docs-migration-release-report.md`
- `docs/adr/0001-public-private-documentation-boundary.md`
- `docs/adr/0002-performance-claims-must-be-reproducible.md`
- `docs/adr/0003-acacia-performance-recovery-architecture-options.md`

All files were located and successfully read. No documents were missing.

## Diagnostic Benchmark Files Added
The following files were created in the `test/` directory to construct our diagnostic suite:
- `test/phase09_atomic_cost_benchmark_test.go`
- `test/phase09_append_line_bytes_benchmark_test.go`
- `test/phase09_buffer_growth_benchmark_test.go`
- `test/phase09_writer_flush_benchmark_test.go`
- `test/phase09_queue_enqueue_benchmark_test.go`
- `test/phase09_state_check_benchmark_test.go`
- `test/phase09_stats_counter_benchmark_test.go`
- `test/phase09_hotpath_isolation_benchmark_test.go`
- `test/phase09_writer_isolation_benchmark_test.go`

## Diagnostic Methodology
- **Test execution**: Verification with `go test ./test`, `go test -race ./test`, `go test ./...` and `go test -race ./...` to confirm correctness and race cleanliness.
- **Benchmark configuration**: `count=20` repetitions under sequential execution to isolate schedulers.
- **Profiling**: CPU and Memory profiles captured privately under `.private/phase09-diagnostics/pprof/`.
- **Analysis**: Statistical analysis of execution runs using `benchstat` (saved in `.private/phase09-diagnostics/bench/`).

---

## Results Summary
The table below aggregates the observed benchmark statistics for representative areas under `GOMAXPROCS=1` (GMP=1) and `GOMAXPROCS=2` (GMP=2).

| Area | Benchmark | GMP | ns/op | B/op | allocs/op | Interpretation |
|---|---|---:|---:|---:|---:|---|
| **Atomic / State** | `Phase09AtomicStateLoad` | 1 | ~0.34 ns | 0 | 0 | Atomic loads are extremely fast |
| **Atomic / State** | `Phase09AtomicStateLoad` | 2 | ~0.26 ns | 0 | 0 | Low overhead when uncontended |
| **Atomic / State** | `Phase09AtomicStateStore` | 1 | ~5.33 ns | 0 | 0 | Store operations have slight CPU cost |
| **Atomic / State** | `Phase09AtomicStateStore` | 2 | ~4.73 ns | 0 | 0 | Store cost remains in low nanoseconds |
| **Atomic / State** | `Phase09AtomicMultiCounterContention` | 1 | ~11.63 ns | 0 | 0 | Multithreaded counter increments show low contention |
| **Atomic / State** | `Phase09AtomicMultiCounterContention` | 2 | ~29.94 ns | 0 | 0 | Contention overhead increases under concurrency |
| **Append / Buffer** | `Phase09AppendLineBytesSmall` | 1 | ~6.00 ns | 0 | 0 | In-memory line formatting is highly optimal |
| **Append / Buffer** | `Phase09AppendLineBytes1KB` | 1 | ~20.63 ns | 0 | 0 | Reusing buffers avoids append allocations |
| **Buffer Growth** | `DynamicAppend` | 1 | ~3836.00 ns | ~16.14 KiB | 10.0 | Dynamic buffer growth is extremely expensive |
| **Buffer Growth** | `PreGrow` | 1 | ~584.60 ns | 0 | 0 | Pre-allocated slices eliminate allocations |
| **Buffer Growth** | `SyncPool` | 1 | ~470.60 ns | 0 | 0 | pooling buffers offers best performance |
| **Writer Lag** | `Phase09WriterLagSmallPayload` | 1 | ~214.50 ns | 72 | 0 | Fast-path enqueuing has minimal overhead |
| **Writer Lag** | `Phase09WriterLag1KBPayload` | 1 | ~1.49 µs | ~2.38 KiB | 1.0 | Buffer growth under slow consumer triggers allocations |
| **Queue** | `Phase09QueueEnqueueOpenState` | 1 | ~223.10 ns | 66 | 0 | Enqueue operations are fast but have small B/op average |
| **Queue** | `Phase09QueueBlockOnFull` | 1 | ~244.80 ns | 61 | 0 | BlockOnFull policy adds minor coordination cost |
| **Queue** | `Phase09QueueDropDebugOnly` | 1 | ~9.73 ns | 0 | 0 | DropDebugOnly fast-path bypass is extremely efficient |
| **State Check** | `Phase09StateCheck` | 1 | ~0.30 ns | 0 | 0 | Sequential status lookup is negligible |
| **State Check** | `Phase09StateCheckParallel` | 2 | ~0.74 ns | 0 | 0 | Contended state load remains highly optimized |
| **io.Writer** | `Phase09WriterInterfaceSmall` | 1 | ~0.30 ns | 0 | 0 | Interface boundary overhead is negligible |
| **io.Writer** | `Phase09WriterDirectSmall` | 1 | ~0.29 ns | 0 | 0 | Calling concrete pointers has identical speed |
| **Stats** | `Phase09StatsRead` | 1 | ~3.37 ns | 0 | 0 | Atomic reads of counters are extremely fast |
| **Stats** | `Phase09StatsUnderWriteContention` | 2 | ~224.70 ns | 90.50 | 0 | Logging contention adds cost, but reading stats remains fast |

---

## Evidence-Based Findings

### Confirmed
1. **Dynamic Buffer Growth is the Primary Cause of allocations (>0 B/op)**: As proven by `Phase09BufferGrowthDynamic` benchmarks, dynamic slice resizing (`DynamicAppend`) requires 10 allocations and takes 3.8 µs per operation. In contrast, pre-allocated slices (`PreGrow`) or buffer pooling (`SyncPool`) drop allocations to **0 B/op** and latency to ~470-580 ns.
2. **Buffer growth is amplified under slow consumer (Writer Lag)**: In `Phase09WriterLag1KBPayload`, where writing is restricted by external disk speed, memory footprint escalates to 2.38 KiB/op and 1 allocation/op. Memory profiles confirm that 92.75% of all heap allocations are allocated in `appendLine` and `appendLineBytes` because the double-buffer grows unchecked on high-throughput bursts before flushes occur.
3. **The `io.Writer` interface boundary is NOT a cause of the regressions**: The micro-benchmarks comparing `io.Writer` interface invocations with direct struct pointer calls show differences below 0.1 ns.

### Likely
- **Atomics/checks overhead is not a major individual issue, but contributes to overall pipeline complexity**: Atomic status loads (`StateCheck`) consume less than 0.3 ns. However, they add minor synchronization latency under multi-threaded contention.

### Not confirmed
- **False Sharing**: Benchmarking `NoPadding` vs `WithPadding` multi-counter updates showed only a minor performance difference (~3-5%), suggesting it is not a primary driver of the +22% RC regression.

---

## Root Cause Candidates

| Candidate | Evidence | Impact | Recommended Next Step |
|---|---|---|---|
| **Unbounded Double-Buffer Growth** | Memory profile allocates 92.75% of space in `appendLine`/`appendLineBytes` during continuous writes. `DynamicAppend` micro-benchmark confirms massive allocations. | **High** | Implement buffer pre-growing and reuse boundaries or integrate slice size capping. |
| **Flush Delay under High Throughput** | The writer goroutine drains enqueued events sequentially in a select loop. If events arrive constantly, `ticker.C` flushes are delayed, allowing the active buffer to grow to megabytes. | **High** | Introduce explicit flush thresholds based on buffer size (e.g., flush immediately if buffer reaches cap). |
| **Atomic Coordination in Producer** | Status checks, sequence counters, and backpressure coordination require atomic operations. | **Low** | Keep current checks, but cache status values where possible. |

---

## Architecture Implications
These findings directly support the options outlined in **ADR 0003**:
- **Option 1 (Incremental Optimization)**: We can resolve the buffer growth issues by implementing slice size pre-growing and capping policies directly inside `appendLine`/`appendLineBytes` and introducing buffer-size based flushes in the writer loop. This preserves most of the code flow while resolving the regressions.
- **Option 2 (Writer-Owned Sequencing)**: If incremental changes do not completely solve the latency regression on the SSD, moving coordination out of the producer into the background writer remains a viable secondary option.

---

## External Disk Limitation
All benchmarks in this diagnostic report were executed on a repository mounted on an **external disk**.
- This environment artificially inflates file I/O latency, flushing times, and rotation overhead.
- While the CPU and memory allocation metrics (`B/op`, `allocs/op`) and relative comparisons remain highly accurate and useful, **absolute latency values (`ns/op`) must not be used as final release benchmarks**.
- The final validation of the release candidate must be repeated on the internal SSD.

## Private Evidence
All raw benchmark outputs, CPU/memory profiles, and system outputs are stored under the private directory:
- `.private/phase09-diagnostics/bench/` (Raw benchmark results and benchstat reports)
- `.private/phase09-diagnostics/pprof/` (CPU and Memory profiles)
- `.private/phase09-diagnostics/system/` (Captured OS parameters)
- `.private/phase09-diagnostics/reports/phase09-diagnostics-private-evidence.md`

## Recommendation
**Proceed with Option 1 (Incremental Optimization)**. The diagnostics strongly indicate that buffer growth and delayed flushes under write pressure are the primary root causes of the regressions. We recommend:
1. Optimizing buffer allocation by pre-growing slices in the writer.
2. Introducing a buffer capacity check inside the background loop to trigger flushes when the active buffer reaches its cap, instead of relying purely on the timer.
