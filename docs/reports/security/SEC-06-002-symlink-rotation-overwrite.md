# Security Finding

*This is a historical engineering report. The filename is kept for audit continuity.*

## Finding ID: SEC-06-002

### Metadata
- **Severity:** High
- **Vector Type:** Symlink Attack
- **Date Identified:** 2026-07-08
- **Status:** Remediated

### Vulnerability Description
During the log rotation process (both daily and size-based), Acacia renamed files and backups (`log.N -> log.(N+1)`) using `os.Rename` and opened/created the new active log with `os.OpenFile` without validating whether the source or destination path pointed to a symbolic link (symlink). If a local attacker with access to the log directory pre-created a symlink pointing to a critical system file, the rotation followed the symbolic link and overwrote the contents of the target file.

### Impact
Destruction, corruption, or manipulation of key operating system configuration files or data from other applications hosted on the same physical machine, enabling denial of service (DoS) attacks or local privilege escalation.

### Code Snippet Reference
Before the fix:
```go
for i := maxRot - 1; i >= 0; i-- {
    src := fmt.Sprintf("%s.%d", target, i)
    dst := fmt.Sprintf("%s.%d", target, i+1)
    if _, err := os.Stat(src); err == nil {
        _ = os.Rename(src, dst)
    }
}
```

### Proof of Concept (PoC)
1. Attacker creates a symlink: `ln -s /etc/shadow /var/log/acacia/app.log.0`
2. The active logger reaches the size limit and triggers rotation.
3. The logger executes `os.Rename("app.log", "app.log.0")`, which follows the symbolic link and overwrites `/etc/shadow` with the contents of the active log.

### Remediation Plan
A helper `IsSymlink(path)` based on `os.Lstat` was implemented to check if a file is a symbolic link. Validations were added before any rename or file open operations in `rotateByDate` and `logRotate`. If any path involved is a symlink, the operation is canceled non-blockingly, the atomic counter `RotationErrorsTotal` is incremented, and `ErrUnsafeSymlink` is returned.
