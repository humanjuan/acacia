# Acacia Project Context

## 1. Purpose

This document is the public operational context for AI agents, maintainers and contributors working on **Acacia**, the Go logger from the HumanJuan ecosystem.

Its goal is to provide enough project context to continue work safely across sessions, without exposing private local evidence or relying on external memory.

This file is safe to commit and safe to upload to AI project contexts.

---

## 2. Project Summary

**Acacia** is a high-performance asynchronous logger for Go.

The current engineering effort focuses on improving:

- runtime stability under high concurrency;
- allocation behavior under hostile workloads;
- structured logging performance;
- secure redaction of sensitive data;
- JSON encoding safety;
- queue backpressure and drop visibility;
- filesystem safety;
- rotation behavior;
- `Sync()` and `Close()` semantics;
- public performance claims and reproducibility.

---

## 3. Current Release Focus

Current release candidate focus:

```text
Structured logging maturity, final SSD validation, and release candidate preparation.
```

Current status:

```text
Completed / Final SSD Hard Validation ready for release candidate review
```

Next action:

```text
Commit final public documentation updates, then prepare the release candidate tag.
```

Historical performance recovery summary:
- **Stability**: Retained. All E2E correctness and regression suites pass under Go test and race detector.
- **Performance Recovery**: Substantially recovered on the external disk diagnostics run. Hot-path and 1KB/4KB latency decreased by 42% to 44%, and memory allocations (B/op) on formatting methods were eliminated (dropping to 0 B/op).
- **Diagnostics & Tooling**: Fully integrated `benchstat` comparisons and pprof CPU/memory profiling.
- **SSD Caveat**: All historical recovery results were verified on an external disk repository copy. Final release candidate performance validation must be repeated on the internal SSD.

---

## 4. Main Technical Problem Identified

The hostile observability audit found that the current structured/legacy logging path can generate significant runtime pressure under stress.

The problematic areas are:

* `interface{}` boxing in legacy APIs;
* `map[string]interface{}` structured logging;
* reflection-based sanitization;
* fallback formatting using `fmt.Sprint`;
* map sorting and dynamic traversal;
* allocations before drop when the queue is saturated;
* excessive heap pressure under `GOMAXPROCS=1`.

The improvement direction is to move the recommended hot path toward:

* typed fluent/builder API;
* append-only JSON encoding;
* buffer reuse;
* explicit sensitive-field redaction;
* measured backpressure behavior;
* reproducible benchmark claims.

---

## 5. Public / Private Boundary

Acacia uses a strict documentation boundary.

Public and versioned:

```text
docs/
```

Private and local only:

```text
.private/
```

Public documentation may include:

* plans;
* roadmaps;
* ADRs;
* runbooks;
* sanitized reports;
* reproducible benchmark summaries;
* security hardening plans;
* migration strategy;
* AI project context.

Private local evidence must stay under `.private/` and must never be committed.

Private material includes:

* `.pprof`;
* `.prof`;
* `.trace`;
* raw benchmark outputs if they expose local details;
* raw logs;
* memory dumps;
* local system traces;
* tokens;
* secrets;
* credentials;
* private infrastructure URLs;
* absolute local paths;
* unsanitized evidence.

---

## 6. Required Working Rules for AI Agents

Agents working on this repository must follow these rules:

1. Do not invent metrics.
2. Do not claim success without commands and evidence.
3. Do not commit or recommend committing `.private/`.
4. Do not commit `.pprof`, `.prof`, `.trace`, dumps or raw local evidence.
5. Do not mix core logger changes with documentation-only commits.
6. Keep documentation-only commits separate from code changes.
7. Before each commit, check private-file exclusion.
8. Every engineering milestone must produce:

   * one public sanitized report under `docs/reports/`;
   * one private raw evidence report under `.private/reports/`, when evidence exists.
9. Public reports must not include secrets, absolute local paths or unsanitized logs.
10. Performance claims must be reproducible through documented commands.
11. Write public technical reports in English to keep the public repository accessible.
12. Be direct, technical and evidence-driven.

---

## 7. Current Repository Organization

Expected public documentation:

```text
docs/
  README.md
  ACACIA_PROJECT_CONTEXT.md
  QUALITY_AND_VALIDATION.md
  MIGRATION_STRATEGY.md
  plans/
    roadmap.md
    performance-engineering.md
    security-hardening.md
    performance-recovery.md
    structured-json-maturity.md
    structured-json-api-contract.md
  policies/
    PUBLIC_PRIVATE_DOCUMENTATION_POLICY.md
    REPORTING_GUIDE.md
    TESTING_AND_BENCHMARKING.md
  adr/
  reports/
    final-ssd-hard-validation-report.md
  guides/
  runbooks/
  templates/
```

Expected private local structure:

```text
.private/
  README.md
  ssd-hard-validation/
  reports/
  evidence/
  bench/
  pprof/
  fuzz/
  stress/
  tmp/
```

---

## 8. Historical Engineering Timeline

The historical engineering timeline is organized as follows:

```text
Phase 00 — Repository Organization
Phase 00B — Scope Alignment and Final Validation
Phase 01 — Reproducible Baseline
Phase 02 — Typed Structured API
Phase 03 — Append-only JSON Encoder
Phase 04 — Sensitive Data Redaction and Security
Phase 05 — Queue, Backpressure, and Memory
Phase 06 — Filesystem, Permissions, and Rotation
Phase 07 — Sync and Close Semantics
Phase 08 — Documentation, Migration, and Release
Phase 09 — Performance Recovery, Architecture Review & OpenTelemetry Compatibility
Phase 10 — Structured JSON Maturity
```

---

## 9. Migration Direction

Current migration direction:

* `v2.x` keeps backward compatibility.
* A bridge version such as `v2.4.x` or `v2.5.x` may introduce the builder/fluent API while keeping the legacy API.
* A possible `v3.0.0` may become the clean break if the legacy interface/map-based API is removed or moved to a compatibility package.
* Do not describe `v3.0.0` as final unless the project explicitly decides it.

---

## 10. Commands Agents Should Ask For

Repository and privacy validation:

```bash
git status --short
git diff --stat
git diff --name-only
git ls-files '*.pprof'
git ls-files .private docs/private reports/private
git check-ignore -v .private/README.md
git check-ignore -v .private/reports/phase-00-private-evidence.md
```

Basic correctness:

```bash
go test ./...
go test -race ./...
```

Baseline phase commands:

```bash
GOMAXPROCS=1 go test ./... -bench=. -benchmem -run '^$' -count=10
GOMAXPROCS=2 go test ./... -bench=. -benchmem -run '^$' -count=10
go test ./... -bench=. -benchmem -run '^$' -count=10
```

Profiles must be stored only under `.private/pprof/`.

---

The Phase 00 commit should remain documentary/configurational.

Do not stage these files as part of Phase 00 unless a maintainer explicitly changes the scope:

```text
acacia.go
internal/logging/wrapper.go
test/stress_vectors_test.go
```

These are considered experimental or implementation-phase files and should be handled in later phases or separate commits.

---

## 12. Preferred AI Output Style

When assisting this project:

* use English for public files, and keep prompt context direct;
* avoid praise or marketing language;
* identify risks directly;
* separate public, private and non-committable material;
* provide AGY prompts that are executable;
* require commands and evidence before accepting success;
* keep recommendations tied to the current phase;
* do not move to the next phase until the current one is cleanly closed.

---

## 13. Current Next Action

Current Next Action:

```text
Commit final public documentation updates, then prepare the release candidate tag.
```

Recommended commit message:

```text
docs: polish and organize public documentation for release candidate review
```

After that:

```text
Prepare release candidate tag.
```
