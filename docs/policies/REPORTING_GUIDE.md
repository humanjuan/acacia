# Engineering Milestone Reporting Guide

When completing a development milestone, we document our findings and metrics in both public and private locations.

---

## 1. Dual Report Structure

Each completed milestone requires two distinct reports:

```text
Completed Milestone
  ├── Public Report (Sanitized)  -> docs/reports/<milestone>-report.md
  └── Private Evidence (Raw)     -> .private/reports/<milestone>-private-evidence.md
```

*Note: Historical engineering reports and archived plans may preserve their phase-based names (e.g. `phase-XX-report.md`) for audit consistency.*

### A. Public Report (`docs/reports/`)
* **Purpose:** To inform contributors of progress, performance trends, and security audits.
* **Security:** Must not contain secrets, internal infrastructure logs, or absolute paths (e.g., `/Volumes/...` or `/Users/...`).
* **Reproducibility:** Performance claims (ns/op, B/op, allocs/op) must include the exact command used so that anyone with similar hardware can reproduce the results.

### B. Private Evidence (`.private/reports/`)
* **Purpose:** To keep raw traces, memory dumps, and logs specific to the development host.
* **Contents:**
  * Raw terminal outputs.
  * Paths to profiling artifacts in `.private/pprof/`.
  * Verbose stress test outputs.
  * Details of security issues before they are fixed.

---

## 2. Writing Principles

* **Honest Metrics:** If a test or benchmark fails or regresses, we report it. We do not hide performance issues or integration failures.
* **Templates:** Public reports must follow [Public Report Template](../templates/public-phase-report.template.md).
* **Exclusion Check:** Before committing, verify that local raw evidence is gitignored.
