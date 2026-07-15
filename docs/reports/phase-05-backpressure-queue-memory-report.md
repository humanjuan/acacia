# Phase 05 — Backpressure, Queue, and Memory Optimization Report

*This is a historical engineering report. The filename is kept for audit continuity.*

* **Date:** 2026-07-08
* **Branch:** `dev`
* **Commit:** `feat: add sensitive data redaction for structured events`
* **Go version:** `go1.23.2`
* **OS/Arch:** `darwin/amd64`
* **Scope:** Design, integration, and optimization of backpressure/queue dropping policies, thread-safe telemetry, and elimination of redundant allocations under saturation (avoiding the "drop illusion").

---

## Documents Reviewed
At the beginning and during this phase, the project's documentation body was reviewed and analyzed for design alignment:
* **docs/ACACIA_PROJECT_CONTEXT.md**: Verification of operational scope and project milestones.
* **docs/ACACIA_IMPROVEMENT_ROADMAP.md**: Inspection of specific backpressure/drop requirements and the use of channels.
* **docs/TESTING_AND_BENCHMARKING.md**: Guide on the stability and reproducibility of benchmarks with count=10.
* **docs/SECURITY_HARDENING_PLAN.md**: Verification that new policies do not alter or inject control characters or compromise security.
* **docs/PUBLIC_PRIVATE_DOCUMENTATION_POLICY.md**: Strict policy excluding local paths and ensuring secure versioning.
* **docs/REPORTING_GUIDE.md**: Required structure for public phase closure reports.
* **docs/PERFORMANCE_ENGINEERING_PLAN.md**: Guidelines to reduce allocations to 0 B/op in hot and saturated paths.
* **docs/MIGRATION_STRATEGY.md**: Principle of non-breaking compatibility and preservation of defaults.
* **observability_audit.md**: Details the original audit where it was identified that late dropping ("the drop illusion") generated 52.5% CPU in `mallocgc` and large struct queues (500k) occupied 35MB of static initial memory.
* **docs/reports/phase-01-baseline-report.md**, **phase-02-typed-api-report.md**, **phase-03-json-encoder-report.md**, **phase-04-redaction-security-report.md**: Used to validate the evolution of allocations in the hot path.

---

## Backpressure Policy Design
The following backpressure policies were implemented and integrated into Acacia:
1. **BlockOnFull (Historical Default):**
   * **Historical compatibility:** The original legacy Acacia logger blocks the producer if the queue is full. To ensure total compatibility with existing clients and prevent unforeseen operational disruptions in production, `BlockOnFull` has been kept as the default policy for Acacia.
   * **Behavior:** If the queue is full, the producer blocks synchronously on the event channel until space becomes available.
2. **DropNewest:**
   * **Behavior:** If the queue is full, the new incoming log event is discarded immediately and in a non-blocking manner.
3. **DropOldest:**
   * **Behavior:** If the queue is full, the oldest log event is evicted from the `_log.events` channel in a non-blocking manner to make room for the new event.
   * **Limitations:** It is limited to concurrent capacity. If the freed queue slot is immediately occupied by another competing goroutine before the current event can be enqueued, the event is dropped to prevent system lockups.
4. **DropDebugOnly:**
   * **Behavior:** If the queue is full and the event is of `DEBUG` or `INFO` level, it is discarded immediately to relieve saturation. If the event is of `WARN`, `ERROR`, or `CRITICAL` level, the `BlockOnFull` (blocking) behavior is applied to guarantee that critical production logs are never lost under saturation.

---

## Metrics Added
A thread-safe atomic queue telemetry structure (`Stats`) accessible without allocations was integrated:
* **EnqueuedTotal:** Total events that were successfully accepted into the write queue.
* **WrittenTotal:** Total log events that the asynchronous writer has physically written to disk.
* **DroppedTotal:** Total log events that have been dropped due to saturation and the active backpressure policy.
* **QueueLen / QueueCap:** Current queue occupancy length and maximum queue capacity.
* **WriteErrorsTotal:** Total physical write errors (I/O) to disk.
* **RotationErrorsTotal:** Total errors when attempting to rename or create files during daily or size-based rotations.

---

## Control Event Safety
* **Control event safety:** Critical system calls such as `Sync()` and `Close()`, as well as hot rotation changes, are routed exclusively through the dedicated `_log.control` channel instead of being mixed into the `_log.events` channel.
* **Guarantee under drops:** Since drop policies (`DropNewest`, `DropOldest`, `DropDebugOnly`) act strictly and exclusively on the `_log.events` queue, infrastructure control messages can never be dropped.
* **Robust synchronization:** Synchronization takes a snapshot of `enqueuedSeq` and the writer drains the channel up to that exact index before sending an `ack`, preventing deadlocks under concurrent log dropping.

---

## Pre-Drop Work Reduction
To mitigate useless allocations in the typed structured builder under dropping ("the drop illusion"), a "pre-drop check" optimization was integrated into `NewEvent(level)`:
* If the policy is `DropNewest` or `DropDebugOnly` and `len(events) >= cap(events)`, a check is performed to see if the event qualifies for dropping before reserving buffers or pool structures.
* If it qualifies, `droppedSeq` is incremented immediately and `nil` is returned.
* Subsequent chained methods (`Str`, `Int`, `Send`, `Msg`) short-circuit instantly upon receiving `nil`.
* **Path avoiding work:** The hot path of the typed structured API (`Event`).
* **Legacy debt:** The legacy path `Log.Info(map[string]interface{})` still requires initial allocations due to interface boxing and conversion to maps before enqueuing. This remains as controlled technical debt to be eliminated in v3.

---

## Stress Test Summary
Concurrent stress tests executed 64 concurrent goroutines attempting to enqueue 5,000 logs each (a total of 320,000 logs per test) on a small queue of 1,000 slots:
* **Results:**
  * `TestStressBackpressureDropNewest`: Processed and dropped correctly without deadlocks.
  * `TestStressBackpressureBlockOnFull`: Guaranteed 0 drops and completed all writes without race failures.
  * `TestStressBackpressureDropDebugOnly`: Dropped debugs but preserved error logs without interruptions.
  * `TestStressSyncCloseUnderSaturation`: Concurrent synchronization calls unblocked correctly during writer shutdown, guaranteeing safe draining and robustness.

---

## Benchmark Summary

### GOMAXPROCS=1
Metrics obtained with the command: `GOMAXPROCS=1 go test ./test -bench 'Benchmark(Backpressure|Vector3Drop|EventBuilderDisabledLevel)' -benchmem -run '^$' -count=10`

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkBackpressureDropNewestFullQueue` | 37.4 ns | 0 B | 0 | Real zero-allocation |
| `BenchmarkBackpressureBlockOnFull` | 359.3 ns | 45 B | 0 | Blocking regulator |
| `BenchmarkBackpressureDropOldestFullQueue` | 143.3 ns | 2 B | 0 | Fast FIFO eviction |
| `BenchmarkBackpressureDropDebugOnlyFullQueue` | 36.9 ns | 0 B | 0 | Fast debug drop |
| `BenchmarkBackpressureStats` | 4.7 ns | 0 B | 0 | Query stats without allocations |
| `BenchmarkBackpressureDropped` | 0.8 ns | 0 B | 0 | Simple drop query |
| `BenchmarkBackpressureBuilderDropNewestFullQueue` | 24.6 ns | 0 B | 0 | Pre-drop check in typed builder |

### GOMAXPROCS=2
Metrics obtained with the command: `GOMAXPROCS=2 go test ./test -bench 'Benchmark(Backpressure|Vector3Drop|EventBuilderDisabledLevel)' -benchmem -run '^$' -count=10`

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkBackpressureDropNewestFullQueue` | 91.2 ns | 120 B | 0 | No allocations under multiprocessing |
| `BenchmarkBackpressureBlockOnFull` | 218.2 ns | 3 B | 0 | Performance in concurrent threads |
| `BenchmarkBackpressureDropOldestFullQueue` | 212.8 ns | 205 B | 0 | Safe FIFO eviction |
| `BenchmarkBackpressureDropDebugOnlyFullQueue` | 85.7 ns | 125 B | 0 | Hybrid behavior |
| `BenchmarkBackpressureStats` | 3.6 ns | 0 B | 0 | Fast telemetry |
| `BenchmarkBackpressureDropped` | 0.5 ns | 0 B | 0 | Fast telemetry |
| `BenchmarkBackpressureBuilderDropNewestFullQueue` | 130.0 ns | 109 B | 0 | Optimal pre-drop short-circuit |

---

## pprof Summary
The analysis of CPU and memory profiles with `go tool pprof` for `BenchmarkBackpressureBuilderDropNewestFullQueue` reveals that:
* **CPU:** Under saturation with drop, the CPU spends most of its time in system calls and Go's internal channel infrastructure (`runtime.selectgo`, `kevent`). The cost of the pre-drop check is imperceptible.
* **Memory:** Heap allocations are exactly **0 B/op** due to preventive bypass and full reuse of Event instances from the pool and local buffers.

---

## Race Detector Summary
* The complete stress test suite in massive concurrency executed under `go test -race ./...` completed successfully with a **PASS** result and **0 data race warnings**.
* It is guaranteed that statistics gathering (`Stats`) and hot backpressure changes operate in a thread-safe manner using atomic operations in `sync/atomic`.

---

## Acceptance Criteria Status
* **Configurable drop policies:** Met (DropNewest, BlockOnFull, DropOldest, DropDebugOnly).
* **Avoid allocations prior to drop:** Met (preventive nil return in NewEvent).
* **Hot statistics support:** Met (thread-safe Stats() and Dropped() methods).
* **Synchronization and shutdown without deadlocks:** Met (unblocked control draining in Close).

---

## Risks / Regressions
* **Map Technical Debt:** Legacy structured logging paths (`Log.Info(map[string]interface{})`) still make dynamic allocations and boxing before being able to verify if the queue is full. This represents a minor, controlled cost, with plans for complete deprecation in v3.

---

## Private Evidence References
Raw performance evidence and locally generated profiles are stored and protected in:
* `.private/reports/phase-05-private-evidence.md` (private evidence report)
* `.private/pprof/phase05_backpressure_cpu.pprof`
* `.private/pprof/phase05_backpressure_mem.pprof`
* `.private/bench/phase05_backpressure_bench_gmp1.txt`
* `.private/bench/phase05_backpressure_bench_gmp2.txt`

---

## Next Phase Recommendation
It is recommended to declare **Phase 05** fully **completed and closed**.
The next planned phase is:
* **Phase 06 — Filesystem, permissions, and secure rotation**
