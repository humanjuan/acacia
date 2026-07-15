# Engineering Reports

This directory contains public, sanitized engineering reports for Acacia.

*Note: Current release candidate reports serve as active public documentation. Historical engineering reports are preserved for audit continuity. Historical filenames keep their phase-based naming for traceability, but their contents are presented in English.*

---

## Current Release Candidate Reports
* [Final SSD Hard Validation Report](final-ssd-hard-validation-report.md): Consolidated results of correctness, fuzzing, and benchmarks on SSD.
* [Structured JSON Integration Report](structured-json-integration-validation-report.md): Integration validation results for the structured implementation.
* [Documentation and Examples Audit Report](structured-json-documentation-examples-report.md): Review of technical guides and user examples.

---

## Historical Engineering Reports
* [Phase 00 Report: Repository Organization](phase-00-repo-organization-report.md)
* [Phase 01 Report: Reproducible Baseline](phase-01-baseline-report.md)
* [Phase 02 Report: Typed Structured API](phase-02-typed-api-report.md)
* [Phase 03 Report: Append-only JSON Encoder](phase-03-json-encoder-report.md)
* [Phase 04 Report: Sensitive Data Redaction](phase-04-redaction-security-report.md)
* [Phase 05 Report: Queue, Backpressure, and Memory](phase-05-backpressure-queue-memory-report.md)
* [Phase 06 Report: Filesystem, Permissions, and Rotation](phase-06-filesystem-permissions-rotation-report.md)
* [Phase 07 Report: Sync and Close Semantics](phase-07-sync-close-semantics-report.md)
* [Phase 08 Report: Documentation and Release Readiness](phase-08-docs-migration-release-report.md)
* [Release Candidate Hard Performance Report](release-candidate-hard-performance-validation-report.md)
* [Phase 09 Diagnostic Report](phase-09-diagnostics-report.md)
* [Phase 09 Incremental Optimization Report](phase-09-incremental-optimization-report.md)
* [Phase 10 Roadmap Report](phase-10-structured-json-roadmap-report.md)
* [Phase 10 API Contract Report](phase-10-api-contract-report.md)
* [Phase 10 Flat JSON Hardening Report](phase-10-flat-json-builder-hardening-report.md)
* [Phase 10 Object Builder Report](phase-10-object-builder-report.md)
* [Phase 10 Array Builder Report](phase-10-array-builder-report.md)
* [Phase 10 Group and Namespace Report](phase-10-group-namespace-report.md)
* [Phase 10 RawJSON Report](phase-10-rawjson-report.md)
* [Phase 10 Any Fallback Report](phase-10-any-report.md)
* [Phase 10 Recursive Redaction Audit Report](phase-10-recursive-redaction-audit-report.md)

---

## Security Findings
* [SEC-01-001: JSON Control Characters Escaping Failure](security/SEC-01-001-json-control-characters-escaping-failure.md)
* [SEC-06-001: Filesystem Path Traversal](security/SEC-06-001-filesystem-path-traversal.md)
* [SEC-06-002: Symlink Rotation Overwrite](security/SEC-06-002-symlink-rotation-overwrite.md)
* [SEC-06-003: Insecure Log File Permissions](security/SEC-06-003-insecure-log-file-permissions.md)
* [SEC-07-001: Sync/Close Deadlock](security/SEC-07-001-sync-close-deadlock.md)
* [SEC-07-002: Write After Close Panic](security/SEC-07-002-write-after-close-panic.md)
* [SEC-07-003: Accepted Log Loss on Close](security/SEC-07-003-accepted-log-loss-on-close.md)

---

## Rules for New Reports
* Every report should follow the format defined in [Public Report Template](../templates/public-phase-report.template.md).
* Verify that no raw environment details, local file paths, or private credentials are included before committing.
