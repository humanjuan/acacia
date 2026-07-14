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

## 3. Current Phase

Current phase:

```text
Fase 10 — Structured JSON Maturity
```

Current status:

```text
In Progress / Implementing Structured JSON builders
```

Next action:

```text
Review Sprint 10.4 Group and Namespace, then decide whether to proceed to Sprint 10.5 — RawJSON.
```

Summary of Phase 09 results:
- **Stability**: Retained. All E2E correctness and regression suites pass under Go test and race detector.
- **Performance Recovery**: Substantially recovered on the external disk diagnostics run. Hot-path and 1KB/4KB latency decreased by 42% to 44%, and memory allocations (B/op) on formatting methods were eliminated (dropping to 0 B/op).
- **Diagnostics & Tooling**: Fully integrated `benchstat` comparisons and pprof CPU/memory profiling.
- **SSD Caveat**: All Phase 09 results were verified on an external disk repository copy. Final release candidate performance validation must be repeated on the internal SSD.

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
6. Keep Fase 00 documentary/configurational only.
7. Before each commit, check private-file exclusion.
8. Every phase must produce:

   * one public sanitized report under `docs/reports/`;
   * one private raw evidence report under `.private/reports/`, when evidence exists.
9. Public reports must not include secrets, absolute local paths or unsanitized logs.
10. Performance claims must be reproducible through documented commands.
11. Prefer Spanish for planning and AGY prompts unless the task explicitly requires English.
12. Be direct, technical and evidence-driven.

---

## 7. Current Repository Organization

Expected public documentation:

```text
docs/
  README.md
  ACACIA_PROJECT_CONTEXT.md
  ACACIA_IMPROVEMENT_ROADMAP.md
  PUBLIC_PRIVATE_DOCUMENTATION_POLICY.md
  TESTING_AND_BENCHMARKING.md
  REPORTING_GUIDE.md
  SECURITY_HARDENING_PLAN.md
  PERFORMANCE_ENGINEERING_PLAN.md
  MIGRATION_STRATEGY.md
  adr/
  reports/
  runbooks/
  templates/
```

Expected private local structure:

```text
.private/
  audits/
  reports/
  evidence/
  pprof/
  bench/
  fuzz/
  stress/
  tmp/
```

---

## 8. Roadmap Summary

The improvement roadmap is organized as follows:

```text
Fase 00 — Organización del repositorio
Fase 00B — Corrección y validación final de alcance
Fase 01 — Baseline reproducible
Fase 02 — API estructurada tipada
Fase 03 — Encoder JSON append-only
Fase 04 — Redacción y seguridad de datos sensibles
Fase 05 — Backpressure, cola y memoria
Fase 06 — Filesystem, permisos y rotación segura
Fase 07 — Semántica formal de Sync y Close
Fase 08 — Documentación, migración y release
Fase 09 — Performance Recovery, Architecture Review & OpenTelemetry Compatibility
Fase 10 — Structured JSON Maturity
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

## 11. Files That Must Not Be Mixed Into Fase 00 Commit

The Fase 00 commit should remain documentary/configurational.

Do not stage these files as part of Fase 00 unless a maintainer explicitly changes the scope:

```text
acacia.go
internal/logging/wrapper.go
test/stress_vectors_test.go
```

These are considered experimental or implementation-phase files and should be handled in later phases or separate commits.

---

## 12. Preferred AI Output Style

When assisting this project:

* use Spanish by default;
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
Review Sprint 10.4 Group and Namespace, then decide whether to proceed to Sprint 10.5 — RawJSON.
```

Recommended commit message:

```text
feat: add group and namespace structured JSON builders
```

After that:

```text
Decide whether to proceed to Sprint 10.5 — RawJSON.
```
