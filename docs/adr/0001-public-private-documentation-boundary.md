# ADR 0001: Public and Private Documentation Boundary

## Status
Accepted

## Context
During performance and security audits of Acacia, profiling files (pprof), log dumps, and detailed technical reports are generated. These files contain local usernames, absolute hard drive paths, and potentially credentials or tokens from development infrastructure. Unintentionally uploading this information to the Git repository exposes sensitive data and violates security best practices.

## Decision
Establish a strict boundary between public repository content and local evidence:
1. **Directory `.private/` and Local `private/` Folders:** Will not be versioned in Git. All content stored here (pprof, traces, logs, secrets) is strictly excluded via `.gitignore`.
2. **Sanitized Public Reports:** Stage reports in Git (under `docs/reports/`) will only include consolidated statistics and reproducible metrics without exposing host-specific information.
3. **Systematic Validation:** The use of `git check-ignore` and `git ls-files` is established as a mandatory pre-commit checklist step to ensure no confidential files are tracked by Git.

## Consequences
* **Positive:** Improved security against local data leaks, and a repository free of heavy pprof binary files.
* **Negative:** Local profiles must be manually archived by developers on their workstations if they wish to keep them long-term.
