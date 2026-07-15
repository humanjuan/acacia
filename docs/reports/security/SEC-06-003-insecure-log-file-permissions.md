# Security Finding

*This is a historical engineering report. The filename is kept for audit continuity.*

## Finding ID: SEC-06-003

### Metadata
- **Severity:** Medium
- **Vector Type:** Insecure Log File Permissions
- **Date Identified:** 2026-07-08
- **Status:** Remediated

### Vulnerability Description
The Acacia logger originally created and opened all log files with mask permissions of `0644`. This configuration grants public read permissions (world-readable) to any user on the local system, which is insecure for logs containing sensitive or confidential information. Additionally, the logger did not provide options to explicitly configure file or directory permission masks.

### Impact
Leakage of confidential information (credentials, access tokens, trade secrets, or customer identification data) to non-privileged local users sharing the same physical execution environment.

### Code Snippet Reference
Before the fix:
```go
f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
```

### Proof of Concept (PoC)
1. The logger writes logs containing access tokens to `/var/log/acacia/app.log`.
2. A non-privileged local user reads the file: `cat /var/log/acacia/app.log` and steals the secrets due to the `0644` permissions.

### Remediation Plan
Default permissions for new files were changed to `0600` (exclusive read/write for the owner) and for directories to `0700` (exclusive read/write/execution for the owner). Additionally, configurable options `WithFileMode(mode)` and `WithDirMode(mode)` were added, which sanitize permissions to prevent setting public-writable or group-writable bits.
