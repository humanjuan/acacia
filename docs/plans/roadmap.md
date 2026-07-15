# Acacia Roadmap

## Completed for the v2.x Release Candidate
- **Async File Logging Hardening:** Single-writer pipeline with decoupled ring buffers to resolve thread-contention bottlenecks.
- **Typed Structured Logging Builder:** First-class nested JSON builders (`Object`, `Array`, `Group`, `Namespace`) designed for low-allocation performance.
- **JSON Escaping & Single-Line Safety:** Prevention of log manipulation and injection attacks by escaping control characters (`\x00` - `\x1f`) and Compact CR/LF inputs.
- **Sensitive Data Redaction:** Integrated key-pattern matching (e.g. `password`, `token`, `authorization_header`) and explicit builders (`Secret`, `StrRedacted`) with case-insensitive variants.
- **Backpressure Policies:** Configurable congestion policies (`DropNewest`, `BlockOnFull`, `DropOldest`, `DropDebugOnly`) under heavy load.
- **Filesystem Safety & Permissions:** Traversal checks (`../` blocking), directory symlink rejection, and secure permission defaults (`0600` file, `0700` directory).
- **Sync/Close Lifecycle Semantics:** Atomic thread-safe shutdown state machine with non-blocking flushes of accepted events.
- **Performance Recovery:** Systematically resolved latency and allocation regressions for hot-path plain writes and medium payload bytes (1KB).
- **Final SSD Hard Validation:** Comprehensive performance verification run on internal host SSD.

## Candidate Release Readiness
- **Documentation Refinement:** Finalise public index maps, validation guides, and capability-focused specs.
- **Release Tag Preparation:** Prepare the public release candidate (RC) tag.
- **Downstream Integration:** Validate and verify downstream services integration (Falcon/Wolf/Golyn) before asserting full production stability.

## Future Work
- **Optional OpenTelemetry Adapter:** Optional integration adapter wrapper for OTel trace/metric contexts (not native to the current RC).
- **Multi-Platform Benchmarking:** Validation of write speeds and disk concurrency across diverse container environments and operating systems.
- **Soak Testing:** Long-running stress workloads to analyze log rotation behavior and memory stability over extended operational run times.
- **Additional Sinks & Adapters:** Support for network-based adapters, syslog, or structured remote endpoints.

---

## Known Boundaries & Caveats
* **OTel Integration:** The OpenTelemetry adapter is planned as a future optional extension, not bundled with this release candidate.
* **Production Validation:** Real-world stability depends on downstream configuration; direct production validation requires end-to-end testing in the consumer services.
* **Redaction Scope:** Credentials redaction is robust under the audited pattern matchers, but does not represent a mathematical guarantee of perfect redaction over arbitrary custom struct pointer graphs.
* **Performance environment:** Benchmarks are hardware-specific. Measured allocation figures are for regression comparison and not absolute guarantees across different hardware.
