# Release Notes (v2.x Candidate)

Welcome to the new release candidate of **Acacia**, the high-performance concurrent logger for Go.

This release represents a significant engineering effort focused on performance optimization, filesystem safety, robust lifecycle controls, and sensitive data protection.

---

## Key Highlights

### 1. High-Performance Fluent Builder API
Acacia introduces a new **Fluent Builder API** (`InfoEvent()`, `WarnEvent()`, etc.) coupled with an **Append-Only JSON Encoder**. This allows structured logging with lower and predictable allocation behavior than the legacy map/interface path under heavy concurrent loads.

### 2. Built-in Sensitive Data Redaction
Protect user credentials, API keys, and authorization headers automatically. The new engine automatically redacts keys matching sensitive patterns (e.g. `password`, `api_key`, `token`) and provides explicit `.Secret()` and `.StrRedacted()` builders to secure critical values without introducing execution-time reflection overhead.

### 3. Hardened Filesystem and Symlink Protection
We have hardened the log-writing subsystem:
* **Traversal Prevention:** Log filenames are validated to ensure they cannot escape the configured base directory.
* **Symlink Check:** Active files and backup logs are checked via `os.Lstat` before write or rotation, preventing symlink hijacking attacks.
* **Secure Defaults:** Log files now default to exclusive owner permissions (`0600`) and directories to `0700`.

### 4. Robust Sync/Close Concurrency Semantics
Lifecycle controls have been formalized around an atomic state machine:
* **Idempotency:** `Close()` can be called multiple times concurrently without panicking or deadlocking.
* **Log Durability:** Closing the logger flushes accepted events according to documented Close semantics.
* **No Panics:** Writing logs or calling `Sync()` after a logger is closed returns safely without blocking or triggering a panic.

### 5. Advanced Structured JSON Builders
This candidate release adds first-class structured JSON controls currently implemented and documented:
* **Nesting Controls:** Added `Object` for nested dictionaries and `Array` for record elements.
* **Semantic Scopes:** Added `Group` (for JSON-nested namespaces) and `Namespace` (for flat prefixed keys, avoiding dynamic compound key concatenation).
* **Flexible Fallbacks:** Added `RawJSON` (trusted pre-serialized input) and `Any` (slow path reflection-based dynamic fallback).
* **Audited Redaction:** Audited under the documented recursive redaction suite covering all structured builders (e.g. including case-insensitive `authorization_header`).

---

## Performance Reference
These are benchmark observations from the Final SSD Hard Validation run on a local SSD environment (darwin/amd64), useful for regression review and comparison:
* **Direct `[]byte`:** 181.0 ns/op (0 B/op, 0 allocs/op observed)
* **Direct `string`:** 183.7 ns/op (0 B/op, 0 allocs/op observed)
* **Direct `[]byte` 1KB:** 926.3 ns/op (0 B/op, 0 allocs/op observed)
* **Direct `string` 1KB:** 937.2 ns/op (16 B/op, 1 alloc/op observed)
* **Flat JSON Builder:** 522.0 ns/op (29 B/op, 1 alloc/op observed)
* **Object Builder:** 611.6 ns/op (42 B/op, 2 allocs/op observed)
* **RawJSON Payload (Small):** 539.9 ns/op (26 B/op, 1 alloc/op observed)
* **Any Fallback (Small Map):** 3229.7 ns/op (1635 B/op, 25 allocs/op observed)
* **Disabled Level Short-Circuit:** 13.4 ns/op (0 B/op, 0 allocs/op observed)
* **Legacy Map Structured Baseline:** 1126.1 ns/op (323 B/op, 2 allocs/op observed)

Refer to [docs/reports/final-ssd-hard-validation-report.md](docs/reports/final-ssd-hard-validation-report.md) for full metrics.

---

## Known Boundaries
* **OpenTelemetry Adapter:** Not included in this release candidate.
* **Any Fallback:** Reflection-based dynamic slow path, not recommended for high-performance hot paths.
* **RawJSON Security:** Trusts the caller’s payload to contain valid JSON.
* **Production Confidence:** Final production readiness requires integration and verification within downstream services.

---

## Upgrade Guide
v2.x source compatibility is preserved for supported safe inputs; unsafe filenames, symlinks, and insecure modes may now be rejected by design. Check [MIGRATION_STRATEGY.md](docs/MIGRATION_STRATEGY.md) for details on how to migrate your codebase to the new Fluent Builder API and configure secure file permissions.
