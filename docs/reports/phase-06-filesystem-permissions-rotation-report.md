# Phase 06 — Filesystem, Permissions and Secure Rotation Report

*This is a historical engineering report. The filename is kept for audit continuity.*

Date: 2026-07-08
Branch: `dev`
Commit: `899e768`
Go version: `go1.23.2`
OS/Arch: `darwin/amd64`
Scope: Strengthening of Acacia's filesystem subsystem to mitigate path traversal, symlink attacks, insecure permissions, and race conditions under rotation.

---

## Objective
The objective of Phase 06 is to harden Acacia's filesystem and rotation subsystem against path traversal risks, symlink manipulation, and insecure permission assignment, while maintaining public API v2.x compatibility and without degrading asynchronous logger performance.

---

## Documents Reviewed
The following documents and roadmaps were reviewed:
* **docs/ACACIA_PROJECT_CONTEXT.md**
* **docs/ACACIA_IMPROVEMENT_ROADMAP.md**
* **docs/TESTING_AND_BENCHMARKING.md**
* **docs/SECURITY_HARDENING_PLAN.md**
* **docs/PUBLIC_PRIVATE_DOCUMENTATION_POLICY.md**
* **docs/REPORTING_GUIDE.md**
* **docs/PERFORMANCE_ENGINEERING_PLAN.md**
* **docs/MIGRATION_STRATEGY.md**
* **docs/reports/phase-05-backpressure-queue-memory-report.md**

---

## Scope Decision
* **In Scope:**
  * Defensive validation of the log filename.
  * Secure validation of the log base directory.
  * Prevention of malicious absolute and relative paths in the log name.
  * Secure path recombination without directory escapes.
  * Prevention against symlink overwrite attacks using `os.Lstat`.
  * Changing default permissions to secure masks (`0600` for files and `0700` for directories).
  * Inclusion of configurable options `WithFileMode` and `WithDirMode` that omit insecure group or world-writable bits.
  * Incrementing and exposing rotation and write error counters.
* **Out of Scope:**
  * Rewriting the asynchronous logger.
  * Breaking changes to the public signatures of `Start`, `Rotation`, `DailyRotation`.
  * Configuration of allowed symlinks.

---

## Filesystem Security Design
The filesystem subsystem has been redesigned with the following security layers:
1. **Filename Validation:** Validation of the `logName` parameter using `ValidateLogFilename(name)`. Empty names, separators, NUL bytes, or `..` sequences are rejected.
2. **Directory Validation:** Validation of `logPath` using `ValidateLogDirectory(dir)`. Resolves the clean absolute path, verifies it is a real directory, and rejects it if it is a symlink.
3. **Safe Path Join:** The final path is composed by combining the validated clean absolute directory with the validated clean name, guaranteeing that the final file resides strictly within the secure root.
4. **Symlink Policy:** Active file and backup paths are inspected with `os.Lstat` prior to renaming or writing operations, preventing symlink tracking.
5. **Permissions Policy:** Creation permissions are configured by default for owner-exclusive read/write.
6. **Rotation Safety:** Size-based and daily rotation operations validate the source and destination for symlinks, and handle errors by incrementing `RotationErrorsTotal` in a thread-safe manner.

---

## Filename Validation Policy
* **Accepted Examples:**
  * `"app.log"`
  * `"audit.jsonl"`
  * `"service-2026.log"`
* **Rejected Examples (returns `ErrInvalidLogFilename`):**
  * `""` (empty)
  * `"."` or `".."`
  * `"../evil.log"` (path traversal)
  * `"/tmp/evil.log"` (absolute path)
  * `"subdir/app.log"` (directory separators)
  * `"app.log\x00evil"` (NUL byte)

---

## Directory and Symlink Policy
* The base directory must be a pre-existing path.
* If the base directory provided in `Start` points to a symlink, initialization is canceled, returning `ErrUnsafeSymlink`.
* In cross-platform test environments, the defensive resolution of `os.Lstat` is used to ensure timely detection of symlinks.

---

## Permissions Policy
* **Default Masks:**
  * Files created by Acacia: `0600` (owner-exclusive read/write).
  * Validated base directories: Created and validated with the secure mask `0700` if applicable.
* **Configuration Options:**
  * `WithFileMode(mode os.FileMode)`: Customize permissions for the active file and backups. Preventively removes world-writable (`0002`) and group-writable (`0020`) bits.
  * `WithDirMode(mode os.FileMode)`: Customize directory permissions. Removes world-writable (`0002`) and group-writable (`0020`) bits.

---

## Rotation Safety
* During daily and size-based rotations, Acacia iterates in descending order renaming backups.
* Before executing `os.Rename(src, dst)`, `IsSymlink(src)` and `IsSymlink(dst)` are checked.
* If a symlink is detected, the operation for that slot is safely skipped, and an error is added to `RotationErrorsTotal`.
* If the active file (`base`) or the first slot (`datedBase` or `.0`) is a symlink, rotation fails with `ErrUnsafeSymlink`.
* The new active log file is created using the configured `_log.fileMode` to maintain permission consistency.

---

## Public API Impact
* Added options:
  * `WithFileMode(mode os.FileMode) Option`
  * `WithDirMode(mode os.FileMode) Option`
* Exported errors:
  * `ErrInvalidLogFilename`
  * `ErrInvalidLogDirectory`
  * `ErrUnsafeSymlink`
  * `ErrInsecureFileMode`
* Exported helper validation functions:
  * `ValidateLogFilename(name string) error`
  * `ValidateLogDirectory(dir string) (string, error)`
  * `IsSymlink(path string) bool`

---

## Files Changed
* `acacia.go`
* `test/phase06_filesystem_security_test.go`
* `test/phase06_filesystem_fuzz_test.go`
* `test/phase06_filesystem_rotation_stress_test.go`
* `test/phase06_filesystem_benchmark_test.go`

---

## Tests Executed

| Command | Result | Notes |
|---|---:|---|
| `go test ./...` | **PASS** | Execution of the complete unit suite |
| `go test -race ./...` | **PASS** | Validation of the race detector across the entire repository |
| `go test ./test -run FuzzValidateLogFilenameNoTraversal -fuzz=FuzzValidateLogFilenameNoTraversal -fuzztime=20s` | **PASS** | Path evasion fuzzing |
| `go test ./test -run FuzzValidateLogFilenameNoNUL -fuzz=FuzzValidateLogFilenameNoNUL -fuzztime=20s` | **PASS** | NUL byte detection fuzzing |
| `go test ./test -run FuzzValidateLogFilenameAcceptsSafeNames -fuzz=FuzzValidateLogFilenameAcceptsSafeNames -fuzztime=20s` | **PASS** | Safe name stability fuzzing |
| `GOMAXPROCS=1 go test ./test -run TestStressRotation -count=1` | **PASS** | Concurrent rotation stress (GMP=1) |
| `go test -race ./test -run TestStressRotation -count=1` | **PASS** | Race detector on rotation stress |

---

## Security Test Summary

| Test | Result | Notes |
|---|---:|---|
| `TestFilenameValidation` | **PASS** | Blocks traversal and invalid characters; accepts clean names |
| `TestDirectoryValidation` | **PASS** | Validates folder existence and rejects symlink/file bases |
| `TestPermissionsSecurity` | **PASS** | Verifies cleaning of world-writable permissions and default 0600 |
| `TestSymlinkActiveFile` | **PASS** | Prevents Acacia initialization if destination is a symlink |
| `TestSymlinkBackupRotation` | **PASS** | Aborts backup rotation if a symlink is encountered |
| `TestSizeRotationNormal` | **PASS** | Verifies that rotations respect backup limits |
| `TestNoRegressionSyncClose` | **PASS** | No regressions in closing and synchronization control |

---

## Fuzzing Summary

| Target | Result | Notes |
|---|---:|---|
| `FuzzValidateLogFilenameNoTraversal` | **PASS** | Millions of runs without traversal evasion |
| `FuzzValidateLogFilenameNoNUL` | **PASS** | Absolute blocking of NUL byte injections |
| `FuzzValidateLogFilenameAcceptsSafeNames` | **PASS** | Stability under alphanumeric characters |

---

## Stress / Race Summary

| Test | Result | Notes |
|---|---:|---|
| `TestStressRotationConcurrentWriters` | **PASS** | 30 threads writing 5KB logs and rotating intensively |

---

## Benchmark Summary

### GOMAXPROCS=1
| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkValidateLogFilenameSafe` | 90.7 ns | 0 B | 0 | Fast and 0 allocations |
| `BenchmarkValidateLogFilenameTraversal` | 20.1 ns | 0 B | 0 | Instant short-circuit traversal |
| `BenchmarkSafeJoinLogPath` | 104.4 ns | 16 B | 1 | Secure path unification |
| `BenchmarkRotationCheckNoRotate` | 4.3 µs | 466 B | 0 | Normal asynchronous operation |
| `BenchmarkRotationSizeSmall` | 2.59 ms | 10.1 KB | 47 | Full physical I/O operation |

### GOMAXPROCS=2
| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkValidateLogFilenameSafe` | 71.0 ns | 0 B | 0 | Optimal performance in multiprocessing |
| `BenchmarkValidateLogFilenameTraversal` | 18.8 ns | 0 B | 0 | Instant short-circuit traversal |
| `BenchmarkSafeJoinLogPath` | 88.4 ns | 16 B | 1 | Secure path unification |
| `BenchmarkRotationCheckNoRotate` | 4.4 µs | 501 B | 0 | Normal asynchronous operation |
| `BenchmarkRotationSizeSmall` | 2.18 ms | 9.6 KB | 47 | Full physical I/O operation |

---

## pprof Summary
No pprof profiling was generated since validation performance proved to be ultra-fast and with zero additional allocations on the main log writing hot path.

---

## Security Findings
* **SEC-06-001 (Path Traversal in Start):** Remediated.
* **SEC-06-002 (Symlink Overwrite in Rotation):** Remediated.
* **SEC-06-003 (Insecure Permissions in Filesystem):** Remediated.

---

## Acceptance Criteria Status

| Criterion | Status | Evidence |
|---|---|---|
| Filename validation | **Complete** | `TestFilenameValidation`, `FuzzValidateLogFilenameNoTraversal` |
| Secure base directory validation | **Complete** | `TestDirectoryValidation` |
| Path Traversal rejection | **Complete** | `TestFilenameValidation` |
| Symlink control and rejection | **Complete** | `TestSymlinkActiveFile`, `TestSymlinkBackupRotation` |
| Secure default permissions (`0600`/`0700`) | **Complete** | `TestPermissionsSecurity` |
| Atomic and race-free rotation | **Complete** | `TestStressRotationConcurrentWriters` |

---

## Risks / Regressions
* **Windows Filesystems:** The `IsSymlink` function uses `os.Lstat`. On Windows, symlink support requires elevated operating system privileges, so the logger will operate in degraded mode or fail appropriately if it cannot perform the check.

---

## Private Evidence References
* `.private/reports/phase-06-private-evidence.md`
* `.private/bench/phase06_go_test.txt`
* `.private/bench/phase06_go_test_race.txt`
* `.private/bench/phase06_stress_rotation_final_gmp1.txt`
* `.private/bench/phase06_final_bench_gmp1.txt`
* `.private/bench/phase06_fuzz_filename_traversal.txt`
* `.private/bench/phase06_fuzz_filename_nul.txt`
* `.private/bench/phase06_fuzz_filename_safe.txt`

---

## Next Phase Recommendation
It is recommended to declare Phase 06 **completed** and move forward to:
* **Phase 07 — Formal Sync and Close Semantics**
