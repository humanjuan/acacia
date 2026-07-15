# Security Finding

*This is a historical engineering report. The filename is kept for audit continuity.*

## Finding ID: SEC-06-001

### Metadata
- **Severity:** High
- **Vector Type:** Path Traversal
- **Date Identified:** 2026-07-08
- **Status:** Remediated

### Vulnerability Description
The original Acacia logger resolved the destination path by joining the base directory and the log file name using the function `filepath.Join(logPath, logName)` without validating or sanitizing the `logName` parameter. If an application used a dynamically configurable filename controlled by malicious users, an attacker could inject directory traversal sequences like `../` or absolute paths to force the logger to write structured logs outside the secure base directory.

### Impact
A local or remote attacker with control over the log filename configuration parameter could overwrite or corrupt critical system files in arbitrary directories on the server (e.g., injecting SSH keys into `.ssh/authorized_keys` or altering system configuration files), leading to privilege escalation or Remote Code Execution (RCE).

### Code Snippet Reference
Before the fix:
```go
fullPath := filepath.Join(logPath, logName)
f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
```

### Proof of Concept (PoC)
```go
// Attempting to write a file outside the configured base path
logger, err := acacia.Start("../../../etc/cron.d/evil_log", "/var/log/acacia", "INFO")
// If successful, a malicious cron job is written or a sensitive configuration is overwritten.
```

### Remediation Plan
A pure defensive validation function `ValidateLogFilename(name)` was implemented. It checks that the filename does not contain null bytes (`\x00`), directory traversal sequences (`..`), separators (`/` or `\`), and verifies that `filepath.Clean` and `filepath.Base` return exactly the same original name. This validation was integrated before opening any file in `Start`.
