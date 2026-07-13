# Phase 10 — API Contract Report (Sprint 10.0)

## Objective
The objective of this sprint was to define and establish the formal public API contract for Acacia Logger's structured JSON maturity phase (Phase 10). The design covers flat fields, nested objects, typed arrays, namespace handling, raw JSON payload injection, dynamic interface fallback, recursive redaction logic, and error/panic boundary safety before writing any productive code.

## Scope
- Discovery and API Contract specification for Sprint 10.0.
- Defining design principles, fluent builder API syntax, and border case behaviors.
- Refining decisions regarding panic boundaries, duplicate keys, and RawJSON security.
- **Out of Scope**: Productive implementation in Go, final release SSD validation, or OpenTelemetry adapter integration during this sprint.

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
- `docs/PHASE_10_STRUCTURED_JSON_MATURITY_PLAN.md`
- `docs/PERFORMANCE_ENGINEERING_PLAN.md`
- `docs/TESTING_AND_BENCHMARKING.md`
- `docs/PUBLIC_PRIVATE_DOCUMENTATION_POLICY.md`
- `docs/REPORTING_GUIDE.md`
- `docs/MIGRATION_STRATEGY.md`
- `docs/adr/0001-public-private-documentation-boundary.md`
- `docs/adr/0002-performance-claims-must-be-reproducible.md`
- `docs/adr/0003-acacia-performance-recovery-architecture-options.md`
- `docs/adr/0004-structured-json-maturity.md`
- `docs/reports/README.md`
- `docs/reports/phase-10-structured-json-roadmap-report.md`
- `docs/reports/phase-09-incremental-optimization-report.md`

All files were located and successfully read. No documents were missing.

---

## API Contract Summary
The API Contract defines a high-performance fluent interface:
- **Flat Fields (Fast Path)**: Typed primitives (`Str`, `Int`, `Bool`, etc.) with a target of 0 allocs/op in documented benchmarks for the fast path.
- **Nested Structures**: Callback-based `Object` and `Array` builders (postponing nested arrays for future extensions) that append directly to the buffer, avoiding reflection or intermediate map allocations.
- **Group vs Namespace**: Dual support for creating physical nested structures (`Group`) or flat prefixed fields (`Namespace` using a dedicated callback scope without `Msg` or `Send`).
- **RawJSON**: Secure byte injection with syntax validation but no internal redaction (trusted input only).
- **Any Fallback**: Explicit reflection-based slow path for dynamic inputs.
- **Recursive Redaction**: Automatic Case-Insensitive filtering of credentials throughout nested callback layers.

---

## Main Decisions
1. **Duplicate Keys**: Duplicate keys will not be deduplicated in the fast path to avoid CPU and memory allocations. They will be written directly to the JSON output, following a "last write wins" convention at the ingestion layer.
2. **Callback Panics**: User callbacks that panique inside `Object` or `Array` will be recovered safely. The partially written buffer segment is rolled back to prevent corrupt JSON output, and a safe constant error field (e.g., `"_acacia_error": "object callback panic"` or `"array callback panic"`) is injected. Raw panic details are not logged to prevent accidental credential leakage.
3. **RawJSON Security**: `RawJSON` payloads are validated for syntax correctness, accepting any valid JSON value (object, array, string, number, bool, null) according to `json.Valid`. It remains explicitly classified and documented as **trusted input only**, with no internal redaction, and invalid payloads degrade to safely escaped strings.

---

## Pending Decisions
- **Homogeneous vs Mixed Arrays**: Deciding whether to enforce type safety on arrays or permit mixed Go types at runtime.
- **Array Nesting Depth**: Confirming the level of recursion supported for arrays inside arrays in initial implementations.

---

## Risks & Mitigations
- **Corruption on Rollback**: If a panic occurs deep inside nested callbacks, rolling back the buffer requires tracking write offsets. Mitigation: We will store the initial write offset before entering the callback, allowing simple slice truncation on recovery.
- **Bypass of Redaction**: Untrusted third-party JSON injected via `RawJSON` could contain sensitive credentials. Mitigation: Clear warnings in documentation and recommended usage patterns.

---

## Non-Goals & Exclusions
- No OpenTelemetry implementation in this phase.
- No SSD release validation in this phase.
- No breaking API changes in the v2.x branch.

---

## Testing Policy & Progress Gates
SSD Hard Validation remains deferred. During Phase 10, Sprint 10.0 was validated using documentation checks. All future code implementation sprints must pass the progress gate:
- `go test ./...`
- `go test -race ./...`
- Fuzz targets validation for encoder and redaction.
- Ensuring no private system traces or `.private/` directory artifacts leak into Git.

---

## Confirmation of No Productive Code Changes
We confirm that **no productive code** inside `acacia.go`, the `internal/` package, or the `test/` directory has been modified or created during this contract design sprint.

---

## Next Sprint Recommendation
**Proceed with Sprint 10.1 — Flat JSON Builder Hardening**. We recommend implementing the hardening of Flat JSON builder primitive types, escape logic, and validation rules in the codebase based on the accepted contract.
