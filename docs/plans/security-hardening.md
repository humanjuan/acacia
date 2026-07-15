# Security Hardening

This document summarizes the active security controls in Acacia and the tests used to guard them.

---

## 1. Active Security Controls and Mitigations

### A. Log Injection Prevention (Log Forgery Mitigation)
* **Risk:** Injection of control characters (`\n` or `\r`) in user parameters to mimic system logs or alter audit records.
* **Mitigation:** The append-only JSON encoder aggressively escapes control characters (`\x00` - `\x1f`) and backslashes, ensuring that log payloads remain isolated on a single physical line and within valid JSON boundaries.

### B. Path Traversal & Arbitrary Write Prevention
* **Risk:** Directory traversal characters (`../`) injected into dynamic filenames to write logs to critical system directories.
* **Mitigation:** Secure base path checking verifies log destinations before file descriptors are opened, rejecting filenames with traversal path segments.

### C. Secure Default Filesystem Permissions
* **Risk:** Generous file creation permissions (e.g. `0666`, `0777`) exposing logs to other local users.
* **Mitigation:** Enforced strict permissions by default (`0600` for log files, `0700` for created directories) to prevent unauthorized read/write access.

### D. Symlink Rotation Attack Mitigation
* **Risk:** Symlink hijacking where a local attacker replaces logs with symbolic links to trick the logger into overwriting critical system files during rotation.
* **Mitigation:** The logger performs `os.Lstat` checks on targeted files before opening or rotating them, immediately halting execution if a directory symlink is detected on active logs.

---

## 2. Sensitive Data Redaction Strategy

* **Key Pattern Matching:** Built-in credentials detection scans for sensitive keys (e.g. `password`, `token`, `authorization_header`) and automatically replaces matching values with `"[REDACTED]"`.
* **Explicit Builder Control:** Developers can secure critical values explicitly using `.Secret()` or `.StrRedacted()`, preventing credential leakage without execution-time reflection overhead.

---

## 3. Continuous Security Validation

* **Fuzz Testing:** Multiple selective fuzz targets actively execute to identify edge cases, verify character escaping, and ensure robust redaction under unexpected or malformed inputs.
* **Traversal Test Suite:** Dedicated test suites verify path traversal rejection, ensuring the logger correctly halts when presented with directory traversal sequences or symlinks.
