# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased] (v2.x Candidate)

### Added
* **Phase 10 Structured JSON Builders:** Fully integrated structured builders: `Object` for nesting, `Array` for records, `Group` for semantic scopes, and `Namespace` for flat prefixed key scopes.
* **Pre-serialized RawJSON:** Support for injecting pre-serialized JSON bytes (`RawJSON`) as trusted input only, with CR/LF compaction fallback.
* **Any Slow Fallback:** Dynamic `interface{}` fallback (`Any`) for un-typed values with recursive best-effort redaction, and cyclic reference / non-serializable safety limits.
* **Recursive Redaction Audit:** Fully audited and hardened recursive case-insensitive redaction matching family variants (e.g. `authorization_header` / `authorizationheader`).
* **Typed Builder API:** Introduced a fluent builder API (`InfoEvent`, `WarnEvent`, `ErrorEvent`, `DebugEvent`, `CriticalEvent`, `Event`) for structured logging with lower and predictable allocation behavior compared to the legacy map/interface path.
* **Sensitive Data Redaction:** Native redaction of sensitive credentials and tokens via key-matching (e.g. "password", "token") or explicit builders (`Secret`, `StrRedacted`).
* **Explicit Backpressure Policies:** Configurable queuing behaviors under congestion (`DropNewest`, `BlockOnFull`, `DropOldest`, `DropDebugOnly`).
* **Observability Stats:** Track queuing metrics through `Stats()` exposing `DroppedTotal`, `EnqueuedTotal`, `WrittenTotal`, `WriteErrorsTotal`, and `RotationErrorsTotal`.
* **Filesystem Modes:** Customizable directory and file permissions (`WithFileMode`, `WithDirMode`) with world/group writable safety sanitization.

### Changed
* **Append-only JSON Encoder:** Replaced the legacy map json serialization with a high-performance append-only JSON encoder reusing pools.
* **Sync and Close Semantics:** Formalized thread-safe lifecycle methods using a state machine (`stateOpen`, `stateClosing`, `stateClosed`) and non-panicking write-after-close policies.
* **Safe Log Path Union:** Hardened internal file opening logic to prevent directory traversal and ensure files are written strictly under the validated base directory.

### Fixed
* **JSON Control Characters:** Resolved potential JSON format corruptions and SIEM ingestion errors by properly escaping control characters (`\x00` - `\x1f`).
* **Symlink Overwrites:** Mitigated symlink attacks by verifying path descriptors with `os.Lstat` before starting the logger or rotating files.
* **Sync/Close Deadlocks:** Prevented emitter goroutines from blocking indefinitely during logger shutdowns.

### Security
* **SEC-01-001:** Hardening JSON escape sequences for control characters.
* **SEC-06-001:** Directory traversal prevention on log filenames.
* **SEC-06-002:** Symlink follow mitigation on active log and backups.
* **SEC-06-003:** Overly permissive world-readable log permissions hardened.
* **SEC-07-001:** Deadlock in BlockOnFull policy during Close remediated.
* **SEC-07-002:** Panic on post-close writes remediated.
* **SEC-07-003:** Silent log loss of accepted events on close remediated.
