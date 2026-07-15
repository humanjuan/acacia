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
* **Log Durability:** Closing the logger ensures all accepted events in the queue are drained and synced to disk.
* **No Panics:** Writing logs or calling `Sync()` after a logger is closed returns safely without blocking or triggering a panic.

### 5. Advanced Structured JSON Builders (Phase 10)
This candidate release adds the Phase 10 structured JSON controls currently implemented and documented:
* **Nesting Controls:** Added `Object` for nested dictionaries and `Array` for record elements.
* **Semantic Scopes:** Added `Group` (for JSON-nested namespaces) and `Namespace` (for flat prefixed keys, avoiding dynamic compound key concatenation).
* **Flexible Fallbacks:** Added `RawJSON` (trusted pre-serialized input) and `Any` (slow path reflection-based dynamic fallback).
* **Audited Redaction:** Conducted a comprehensive recursive redaction audit covering all structured builders (e.g. including case-insensitive `authorization_header`).

---

## Performance Reference
These are representative documented benchmark observations, not final release validation. Final SSD hard validation remains pending.
Under high concurrency benchmark runs on an Intel Core i7 processor:
* **Plain string logging:** ~220 ns/op (**0 allocs/op observed**).
* **Builder-based structured logging:** ~1.3 µs/op (~1 alloc/op observed).
* **Write after close:** ~18 ns/op (**0 allocs/op observed**).

---

## Upgrade Guide
v2.x source compatibility is preserved for supported safe inputs; unsafe filenames, symlinks, and insecure modes may now be rejected by design. Check [MIGRATION_STRATEGY.md](docs/MIGRATION_STRATEGY.md) for details on how to migrate your codebase to the new Fluent Builder API and configure secure file permissions.
