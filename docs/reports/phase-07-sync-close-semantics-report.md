# Phase 07 — Sync and Close Formal Semantics Report

*This is a historical engineering report. The filename is kept for audit continuity.*

Date: 2026-07-08
Branch: `dev`
Commit: `a8843fb`
Go version: `go1.23.2`
OS/Arch: `darwin/amd64`
Scope: Formalization of the synchronization (`Sync()`) and closure (`Close()`) semantics of the Acacia asynchronous logger.

---

## Objective
The objective of Phase 07 is to define, structure, and implement formal semantics for deterministic synchronization (`Sync()`) and secure shutdown (`Close()`) in Acacia, eliminating deadlocks, preventing exceptions from post-close log injection, and guaranteeing the delivery of all log events accepted before these barriers are invoked.

---

## Documents Reviewed
* **docs/ACACIA_PROJECT_CONTEXT.md**
* **docs/ACACIA_IMPROVEMENT_ROADMAP.md**
* **docs/TESTING_AND_BENCHMARKING.md**
* **docs/SECURITY_HARDENING_PLAN.md**
* **docs/PUBLIC_PRIVATE_DOCUMENTATION_POLICY.md**
* **docs/REPORTING_GUIDE.md**
* **docs/PERFORMANCE_ENGINEERING_PLAN.md**
* **docs/MIGRATION_STRATEGY.md**
* **docs/reports/phase-06-filesystem-permissions-rotation-report.md**

---

## Scope Decision
* **In Scope:**
  * Atomic state machine (`stateOpen`, `stateClosing`, `stateClosed`).
  * Avoid physically closing the `events` channel to prevent concurrent write panics.
  * Dedicated control channel (`control`) and structured shutdown protocol (`isClose: true`).
  * Producer blocks released deterministically by closing the `done` channel.
  * Complete asynchronous draining and file fsync before shutdown.
  * Complete unit, zero-loss, stress, and concurrent race test suite for Sync/Close.
* **Out of Scope:**
  * Redesign of general file rotation policies.
  * Redesign of internal asynchrony infrastructure and JSON buffer pooling.

---

## Formal Sync Semantics
* `Sync()` acts as a strict barrier that captures a snapshot of the enqueued sequence (`enqueuedSeq`).
* It waits until the asynchronous writer processes and writes all events up to and including that sequence, executing a physical `Sync()` (fsync) on the descriptor.
* If the logger is closed or closing, `Sync()` returns immediately as a safe no-op to prevent lockups.
* It is concurrent and reentrant (multiple goroutines can call it simultaneously).

---

## Formal Close Semantics
* `Close()` is fully idempotent, controlled via a `sync.Once` instance.
* It atomically transitions the logger state to `stateClosing`, preventing the acceptance of new logs.
* It closes the `done` channel, instantly releasing all producers blocked under the `BlockOnFull` policy.
* It sends a special control event to the asynchronous writer to notify it to begin an orderly shutdown.
* The writer processes the remaining enqueued elements in `events` until the queue is completely empty.
* Finally, the internal buffer is flushed, fsync is executed on the file descriptor, and the physical descriptor is closed.
* Upon completion of the thread draining in `wg.Wait()`, the state is set to `stateClosed`.

---

## Post-Close Behavior
* **`Sync()` after `Close()`:** Instant no-op (returns immediately without blocking).
* **`Close()` after `Close()`:** Idempotent instant no-op protected by `sync.Once`.
* **Log Writes (`Info`, `Warn`, `Error`, `Debug`, `Write`, `InfoBytes`, `Event().Send()`) after `Close()`:**
  * They do not block under any circumstances.
  * They do not throw exceptions of type `panic: send on closed channel`.
  * They increment the atomic drop counter (`DroppedTotal`) and immediately clean up reusable buffers from the pool.

---

## Control Event Protocol
* Control requests are composed via the `controlReq` struct with acknowledgment (`ack`) channels.
* The writer is the sole entity responsible for reading `control` and performing `close(ack)`.
* During the shutdown phase (`isClose: true`), the writer performs a final cleanup by exhaustively draining pending control channels to guarantee that no client threads are blocked.

---

## Zero-Loss Guarantee
* **Attempted Event:** An event sent by the client trying to be processed.
* **Accepted Event:** An event that passes state validation and is successfully inserted into the asynchronous `events` channel.
* **Dropped Event:** An event discarded due to policy saturation or shutdown state.
* **Written Event:** An event successfully transferred to persistent storage.
* **Guarantee:** Every **Accepted** event prior to the initiation of `Sync()` or `Close()` is guaranteed delivery (zero loss) to the physical file.

---

## Public API Impact
* No changes or breakage of public signatures of existing Acacia methods.
* `Sync()` and `Close()` methods retain their v2.x signatures.
* The `Status()` method now reports the exact state based on the logger's lifecycle (`stateOpen`).

---

## Files Changed
* `acacia.go`
* `test/phase07_sync_close_test.go`
* `test/phase07_sync_close_stress_test.go`
* `test/phase07_zero_loss_test.go`
* `test/phase07_sync_close_benchmark_test.go`

---

## Tests Executed

| Command | Result | Notes |
|---|---:|---|
| `go test ./...` | **PASS** | Execution of the complete unit suite |
| `go test -race ./...` | **PASS** | Validation of the race detector across the entire repository |
| `go test ./test -run 'Test.*ZeroLoss' -count=1` | **PASS** | Zero-loss tests on Sync and Close |
| `go test ./test -run TestStressSyncClose -count=1` | **PASS** | Concurrent stress testing of Sync and Close |

---

## Zero-Loss Test Summary

| Test | Result | Notes |
|---|---:|---|
| `TestZeroLossBeforeSync` | **PASS** | N logs written prior to Sync appear intact |
| `TestZeroLossBeforeClose` | **PASS** | N logs written prior to Close appear intact |
| `TestZeroLossConcurrentAcceptedBeforeClose` | **PASS** | Achieved in massive concurrency without loss |
| `TestDropPoliciesNoPromise` | **PASS** | DroppedTotal + EnqueuedTotal equals attempts |

---

## Stress / Race Summary

| Test | Result | Notes |
|---|---:|---|
| `TestStressSyncCloseConcurrentProducers` | **PASS** | 32 concurrent producers writing during Sync/Close |
| `TestStressSyncCloseBlockOnFull` | **PASS** | 64 blocked writers released immediately without deadlock |
| `TestStressSyncCloseDropNewest` | **PASS** | Asynchronous stability under drops and Sync intervals |
| `TestStressSyncCloseDropOldest` | **PASS** | Asynchronous stability under queue head eviction |
| `TestStressSyncCloseDropDebugOnly` | **PASS** | Event priority testing under asynchronous shutdown |
| `TestStressSyncCloseRotation` | **PASS** | Heavy writers rotating and closing concurrently |
| `TestStressRepeatedClose` | **PASS** | 50 goroutines calling Close simultaneously without panics |

---

## Fuzzing Regression Summary

| Target | Result | Notes |
|---|---:|---|
| `FuzzStructuredJSONValidLine` | **PASS** | Stability in legacy structured parsing |
| `FuzzEventBuilderJSONValid` | **PASS** | Stability in the JSON typed builder |
| `FuzzRedactionSensitiveKeysNoLeak` | **PASS** | Automatic redaction of secrets without regression |
| `FuzzValidateLogFilenameNoTraversal` | **PASS** | Persistent blocking of path traversal |

---

## Benchmark Summary

### GOMAXPROCS=1
| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkSyncEmpty` | 1180 | 112 | 1 | Basic barrier |
| `BenchmarkSyncWithBufferedEvents` | 7406 | 128 | 2 | Intermediate synchronization |
| `BenchmarkCloseEmpty` | 22,000,000 | 808 | 10 | Full disk I/O |
| `BenchmarkCloseWithBufferedEvents` | 20,000,000 | 824 | 11 | Full asynchronous I/O |
| `BenchmarkWriteAfterClose` | 17.1 | 0 | 0 | Instant atomic short-circuit |
| `BenchmarkSyncAfterClose` | 0.66 | 0 | 0 | Optimized immediate return |
| `BenchmarkConcurrentSync` | 1192 | 112 | 1 | Parallel sync |
| `BenchmarkCloseUnderBlockOnFull` | 23,000,000 | 808 | 10 | Concurrent release and close |

### GOMAXPROCS=2
| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkSyncEmpty` | 1104 | 112 | 1 | Basic barrier |
| `BenchmarkSyncWithBufferedEvents` | 7787 | 128 | 2 | Intermediate synchronization |
| `BenchmarkCloseEmpty` | 21,000,000 | 858 | 10 | Full disk I/O |
| `BenchmarkCloseWithBufferedEvents` | 21,000,000 | 786 | 10 | Full asynchronous I/O |
| `BenchmarkWriteAfterClose` | 18.9 | 0 | 0 | Instant atomic short-circuit |
| `BenchmarkSyncAfterClose` | 0.62 | 0 | 0 | Optimized immediate return |
| `BenchmarkConcurrentSync` | 1344 | 112 | 1 | Parallel sync |
| `BenchmarkCloseUnderBlockOnFull` | 20,000,000 | 807 | 10 | Concurrent release and close |

---

## Security / Reliability Findings
* **SEC-07-001 (Deadlock in BlockOnFull during Close):** Remediated.
* **SEC-07-002 (Panic due to post-close write):** Remediated.
* **SEC-07-003 (Loss of accepted logs in Close):** Remediated.

---

## Acceptance Criteria Status

| Criterion | Status | Evidence |
|---|---|---|
| Idempotent and concurrent closing | **Complete** | `TestConcurrentClose`, `TestStressRepeatedClose` |
| Producer blocking mitigated | **Complete** | `TestBlockOnFullClose`, `TestStressSyncCloseBlockOnFull` |
| No post-close panics | **Complete** | `TestWriteAfterClose`, `BenchmarkWriteAfterClose` |
| Full draining and zero loss | **Complete** | `TestZeroLossBeforeClose`, `TestZeroLossConcurrentAcceptedBeforeClose` |
| Concurrent and barrier Sync | **Complete** | `TestConcurrentSync`, `TestZeroLossBeforeSync` |

---

## Risks / Regressions
* Synchronous draining of the physical buffer may slow down `Close()` calls on systems under high disk I/O latency (e.g., shared networks or cloud storage), which is the expected correct behavior to avoid data loss.

---

## Private Evidence References
* `.private/reports/phase-07-private-evidence.md`
* `.private/bench/phase07_go_test.txt`
* `.private/bench/phase07_go_test_race.txt`
* `.private/bench/phase07_stress_sync_close.txt`
* `.private/bench/phase07_stress_sync_close_gmp1.txt`
* `.private/bench/phase07_stress_sync_close_race.txt`
* `.private/bench/phase07_zero_loss_final.txt`
* `.private/bench/phase07_final_bench_gmp1.txt`
