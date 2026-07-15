# Phase 10 — Structured JSON Maturity Roadmap Report

*This is a historical engineering report. The filename is kept for audit continuity.*

## Objective
The objective of this planning phase was to design a robust, secure, and high-performance structured JSON logging architecture for Acacia. This design aims to make Acacia market-competitive with production-grade loggers in the Go ecosystem by supporting first-class structured JSON use cases (nested objects, arrays, groups/namespaces, and raw JSON payloads), while aiming for zero-allocation targets on documented benchmarks for hot paths and robust sensitive data redaction.

## Scope
- Architectural design and public API specification for Phase 10.
- Defining Sprints 10.0 to 10.10.
- Documenting acceptance criteria and testing policies.
- Identifying and analyzing tradeoffs (e.g., initial memory footprint vs hot path efficiency).
- **Out of Scope**: Implementing productive code, running E2E release performance validation, or establishing OpenTelemetry compliance during this phase.

## Environment
- **Operating System / Architecture**: macOS (Darwin Kernel Version 24.6.0) / amd64
- **Processor**: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
- **Execution Location**: Executed on a primary repository copy located on an **external disk**.
- **Go Version**: `go1.23.2`

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
- `docs/PUBLIC_PRIVATE_DOCUMENTATION_POLICY.md`
- `docs/REPORTING_GUIDE.md`
- `docs/reports/README.md`
- `docs/reports/phase-09-diagnostics-report.md`
- `docs/reports/phase-09-incremental-optimization-report.md`
- `docs/reports/release-candidate-hard-performance-validation-report.md`
- `docs/adr/0001-public-private-documentation-boundary.md`
- `docs/adr/0002-performance-claims-must-be-reproducible.md`
- `docs/adr/0003-acacia-performance-recovery-architecture-options.md`

All files were located and successfully read. No documents were missing.

---

## Proposed Architecture & Sprints
The structured JSON roadmap is divided into 11 logical sprints:

1. **Sprint 10.0 — Discovery & API Contract**: Establish the API contract for flat and multi-level JSON fields, maintaining strict backward compatibility with v2.x.
2. **Sprint 10.1 — Flat JSON Builder Hardening**: Harden the fast flat builder path with support for control character escaping, duplicate key handling, and aiming for zero-allocation primitive fields in benchmarks.
3. **Sprint 10.2 — Object Builder**: Add nested object builder callbacks (`func(u *acacia.Object)`) without reflection.
4. **Sprint 10.3 — Array Builder**: Add callback-driven typed arrays and arrays of objects.
5. **Sprint 10.4 — Groups / Namespaces**: Support namespace prefixing (`Namespace`) and nested objects (`Group`) to organize fields cleanly.
6. **Sprint 10.5 — RawJSON Seguro**: Support pre-serialized JSON bytes with strict structure validation to prevent injections, documented as trusted input.
7. **Sprint 10.6 — Any / Interface Fallback**: Provide an explicit reflection-based path for convenience in non-performance-critical logging.
8. **Sprint 10.7 — Recursive Redaction Audit**: Establish recursive redaction rules to ensure credentials are automatically filtered out across all nested objects and arrays.
9. **Sprint 10.8 — Fuzzing & Security Validation**: Integrate fuzz tests to validate the JSON encoder against corrupt sequences and nesting depth issues.
10. **Sprint 10.9 — Benchmarks & Performance Gate**: Set up isolated diagnostic benchmarks to compare builders against the legacy map-based path.
11. **Sprint 10.10 — Documentation & Examples**: Add clear use cases, warnings, and code snippets to public user guides.

---

## Tradeoffs
- **API Surface vs Simplicity**: Introducing fluent callback APIs (`Object`, `Array`) increases the complexity of the builder struct, but avoids the heavy boxing and allocation penalties associated with dynamic map interfaces (`map[string]interface{}`).
- **Memory Footprint**: Maintaining pre-allocated capacity (64KB minimum) reduces resize CPU overhead on the hot path but slightly increases the baseline memory usage per active logger instance.
- **Safety vs Speed (RawJSON)**: Validating raw JSON syntax prevents malformed logs from corrupting the stream, but incurs a validation overhead. Payloads that cannot be recursively parsed will be escaped as strings.

---

## Testing Policy
- **Deferred Release Benchmarking**: E2E release candidate validation on the internal SSD is deferred. External-disk diagnostics may be used only for relative, localized performance comparison.
- **Implementation Progress Gate**: Every productive sprint in Phase 10 must pass the repository verification gate:
  - `go test ./...`
  - `go test -race ./...`
  - Fuzzing target execution for at least 60 seconds.
  - Verification that no private profiles or evidence files leak into git.

---

## Recommendation
**Proceed with Phase 10 Sprint planning**. The architectural choices defined in ADR 0004 provide the right balance between modern usability and execution speed. We recommend initiating Sprint 10.0 to refine the API contract before committing to code implementation.
