# Technical Documentation

Welcome to the public documentation for **Acacia**, the asynchronous logging subsystem for Go.

---

## Start Here
* [AI Agent Project Context](ACACIA_PROJECT_CONTEXT.md): Public context for human contributors and AI coding assistants.
* [Quality and Validation](QUALITY_AND_VALIDATION.md): Overview of testing, security controls, and limitations.
* [Migration Strategy](MIGRATION_STRATEGY.md): Practical steps to transition legacy code to structured builders.
* [Structured JSON Builder Guide](guides/structured-json-builder.md): Quick reference for builder APIs.
* [Redaction Policy Guide](guides/structured-json-redaction.md): Credentials redaction guide and safety limits.
* [Final SSD Hard Validation Report](reports/final-ssd-hard-validation-report.md): Execution results on local SSD.

---

## Public vs Private Documentation Boundary
* **Public Documentation (`docs/`)**: Specs, guides, plans, ADRs, public reports, and test instructions. Safe to commit to Git.
* **Private Evidence (`.private/`)**: Ignored by Git. Contains local CPU/memory profiles (`.pprof`), raw benchmark output, stress test logs, and secrets. Must never be committed.

---

## Guides
* [Structured JSON Builder Guide](guides/structured-json-builder.md): API details for flat and nested JSON events.
* [Redaction Policy Guide](guides/structured-json-redaction.md): Redaction keyword rules and custom redactors.
* [Client Migration Guide](guides/structured-json-migration.md): Code recipes to replace maps with event builders.

---

## Plans and Architecture
* [Improvement Roadmap](plans/roadmap.md): Features completed, ready for release candidate, and future work.
* [Performance Engineering Plan](plans/performance-engineering.md): Guidelines for low-allocation logging.
* [Security Hardening Plan](plans/security-hardening.md): Path traversal and symlink validation design.
* [Performance Recovery Plan](plans/performance-recovery.md): Diagnostic steps to resolve benchmark regressions.
* [Structured JSON Maturity Plan](plans/structured-json-maturity.md): API contract goals for v2.x.
* [Structured JSON API Contract](plans/structured-json-api-contract.md): Detailed layout specifications for JSON outputs.

### Architecture Decision Records (ADR)
* [ADR 0001: Public/Private Documentation Boundary](adr/0001-public-private-documentation-boundary.md)
* [ADR 0002: Reproducible Performance Claims](adr/0002-performance-claims-must-be-reproducible.md)
* [ADR 0003: Performance Recovery Options](adr/0003-acacia-performance-recovery-architecture-options.md)
* [ADR 0004: Structured JSON Maturity](adr/0004-structured-json-maturity.md)

### Technical Archive
* [Historical Phase Roadmap](archive/engineering-phase-roadmap.md): Engineering roadmap organized by historical phases (Phase 00 to Phase 10).
* [Historical Performance Recovery Plan](archive/performance-recovery-plan.md): Performance recovery steps during release preparation.
* [Structured JSON Sprint Plan](archive/structured-json-implementation-plan.md): Sprint planning details for structured JSON.

---

## Engineering Reports
* [Engineering Reports Index](reports/README.md): Index of execution reports.
* [Final SSD Hard Validation Report](reports/final-ssd-hard-validation-report.md): Execution results on internal SSD.
* [Structured JSON Integration Report](reports/structured-json-integration-validation-report.md): Pre-release integration test results.
* [Documentation and Examples Audit Report](reports/structured-json-documentation-examples-report.md): Documentation review and code example validations.

---

## Policies
* [Public and Private Documentation Policy](policies/PUBLIC_PRIVATE_DOCUMENTATION_POLICY.md): Classification rules for git versioning.
* [Reporting Guide](policies/REPORTING_GUIDE.md): Documentation rules when completing milestones.
* [Testing and Benchmarking Guide](policies/TESTING_AND_BENCHMARKING.md): Shell commands for running tests, benchmarks, and fuzzing.

---

## Runbooks
* [Local Audit Runbook](runbooks/local-audit-runbook.md): How to analyze local performance and check for regressions.
* [Release Readiness Runbook](runbooks/release-readiness-runbook.md): Pre-tag checklist for release engineers.

---

## Templates
* [Public Report Template](templates/public-phase-report.template.md)
* [Private Evidence Template](templates/private-evidence-report.template.md)
* [Benchmark Summary Template](templates/benchmark-summary.template.md)
* [Security Finding Template](templates/security-finding.template.md)

---

## Current Status
* **Active Task:** Release candidate validation (Completed and verified on internal SSD).
* **Next Step:** Release candidate tagging.
