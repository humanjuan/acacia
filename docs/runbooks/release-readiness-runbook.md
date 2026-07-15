# Release Readiness Runbook

This runbook outlines the validation steps to perform before tagging and releasing a new version of Acacia.

---

## 1. Core and Concurrency Checks
1. **Unit Tests:**
   All package tests must pass:
   ```bash
   go test -v ./...
   ```
2. **Race Detector:**
   No race warnings must be reported:
   ```bash
   go test -race ./...
   ```

---

## 2. Performance Verification (Allocation Checks)
1. **Verify Allocations:**
   Run the benchmark suite and confirm that:
   * String and byte direct paths report **0 allocs/op**.
   * Fluent API builders maintain low-allocation behavior as documented.
2. **Baseline Comparison:**
   Compare benchmark metrics against the baseline version to check for latencies or memory regressions.

---

## 3. Security Checklist
* **JSON Escaping:** Verify that the JSON encoder escapes `\n`, `\r`, and control characters to prevent log injection.
* **File Permissions:** Verify that newly created log files default to restricted `0600` owner permissions.
* **Path Traversal:** Verify that log filenames block directory traversal sequences (e.g. `../`).

---

## 4. Sanitation and Excluded Data Check
Before tagging and pushing the release:
1. Ensure no `.pprof` files or private reports are tracked:
   ```bash
   git ls-files .private docs/private reports/private
   ```
   *This command must output nothing.*
2. Check that no source code comments or documentation references local URLs, IP addresses, or internal development directories.
3. Generate the final public validation report and update `CHANGELOG.md` and `RELEASE_NOTES.md`.
