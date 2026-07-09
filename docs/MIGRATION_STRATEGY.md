# Acacia Migration Strategy

This document guides developers migrating from classic v2.x dynamic dynamic logging to the hardened, typed builder APIs with lower and predictable allocation behavior than the legacy map/interface path and safe filesystem operations introduced in this release candidate.

---

## 1. Compatibility Policy

* **API Coexistence (v2.x):** v2.x source compatibility is preserved for supported safe inputs; unsafe filenames, symlinks, and insecure modes may now be rejected by design. No existing public methods or options have been deleted or signature-modified.
* **Legacy Map Path:** Calling `logger.Info(map[string]interface{}{...})` remains fully supported. However, it is documented as performance debt due to `interface{}` boxing and runtime reflection overhead.
* **Next Major Version (v3.0.0):** Moving legacy methods to a separate compatibility package (`compat`) is a future possibility, but no final decision has been made. For this release, they coexist inside the root package.

---

## 2. Recommended Migration Path

### Step 1: Replace Dynamic Maps with Fluent Builders
Convert legacy key-value logging to the typed builder API to reduce interface boxing, reflection overhead, and heap pressure. Some builder benchmarks still report ~1 alloc/op due to sync.Pool/runtime accounting:

* **Before (Legacy Map):**
  ```go
  log.Info(map[string]interface{}{
      "event":      "user_login",
      "user_id":    12345,
      "ip_address": "192.168.1.100",
  })
  ```
* **After (Builder):**
  ```go
  log.InfoEvent().
      Str("event", "user_login").
      Int("user_id", 12345).
      Str("ip_address", "192.168.1.100").
      Send()
  ```

### Step 2: Leverage Sensitive Data Redaction
Replace manual scrubbing with built-in redaction keys or explicit secret builders:

* **Before (Manual scrub):**
  ```go
  log.InfoEvent().
      Str("password", sanitize(rawPassword)).
      Send()
  ```
* **After (Built-in or Explicit Redaction):**
  ```go
  // Built-in (auto-redacts matching sensitive keys like "password", "token", "secret", "api_key")
  log.InfoEvent().
      Str("password", rawPassword).
      Send()

  // Explicit secret marking (redacted regardless of key name)
  log.InfoEvent().
      Secret("custom_key", rawSecret).
      Send()
  ```

### Step 3: Configure Backpressure and Flush Options Explicitly
Adopt a backpressure policy tailored for your application's reliability needs:

```go
log, _ := acacia.Start("app.log", "./logs", acacia.Level.INFO,
    acacia.WithBackpressurePolicy(acacia.DropNewest), // Drop newest logs when queue is full
    // Or: acacia.WithBackpressurePolicy(acacia.BlockOnFull) // Block producers when queue is full
)
```

### Step 4: Upgrade Sync and Close Lifecycle Management
Integrate `Close()` and `Sync()` correctly into your application shutdown and persistence flows:

```go
func main() {
    log, _ := acacia.Start("app.log", "./logs", acacia.Level.INFO)
    
    // Guarantees all enqueued messages are flushed and synced, and shuts down safely
    defer log.Close()

    log.Info("Service started")
    
    // Force persistence at key checkpoints
    log.Sync()
}
```

---

## 3. Breaking Behavior to Watch

* **Log Filename Validation:** Filenames passed to `Start` must be plain filenames (e.g., `"app.log"`). Passing names containing path separators (e.g., `"subdir/app.log"`) or back-references (e.g., `"../evil.log"`) will return `ErrInvalidLogFilename`.
* **Symlink Protections:** If the base path directory or target log file is a symlink, initial startup or rotation will return `ErrUnsafeSymlink`.
* **Restrictive Permissions:** Newly created directories default to `0700` and log files default to `0600`. Use `WithFileMode` and `WithDirMode` if group access is strictly necessary.
* **Writes after Close:** Invocations of log methods after `Close()` returns will be silently ignored (incrementing `DroppedTotal`) without blocking or panicking.
