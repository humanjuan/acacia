# Migration Strategy

This guide helps you migrate from legacy v2.x map-based logging to the new typed builder APIs and safe filesystem operations.

---

## 1. Compatibility Policy

* **API Coexistence:** v2.x source compatibility is preserved. However, unsafe filenames, symlinks, and insecure file modes are now rejected by design. No existing public methods or options have been deleted or modified.
* **Legacy Map Path:** Calling `logger.Info(map[string]interface{}{...})` remains fully supported, but it allocates more memory due to reflection.
* **Next Major Version (v3.0.0):** We might move legacy methods to a separate compatibility package in the future, but they remain in the root package for now.

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
    
    // Flushes accepted/enqueued messages according to the documented Close semantics,
    // then shuts down safely.
    defer log.Close()

    log.Info("Service started")
    
    // Force persistence at key checkpoints
    log.Sync()
}
```

### Structured JSON Builder Migration

For high-frequency structured logs, prefer the typed builder over `map[string]interface{}`.

#### Recommended Structured Logging Path
To get the most performance and security out of Acacia's structured logging engine, follow this path:
* **Event Builder (Hot Path):** Use flat typed fields (`.Str()`, `.Int()`, etc.) directly on the event for maximum efficiency.
* **Object/Array Builders:** Use `.Object()` and `.Array()` callbacks for nested structures to avoid reflection.
* **RawJSON:** Use `.RawJSON()` only when handling pre-serialized JSON that is already validated and trusted (note: no internal redaction occurs on raw payloads).
* **Any Fallback:** Use `.Any()` only for dynamic values or low-frequency paths (reflection-based and slower).
* **Legacy Map Path:** Calling `logger.Info(map[string]interface{}{...})` is kept for v2.x source compatibility but is not recommended for high-performance paths due to reflection boxing.

---

## 3. Breaking Behavior to Watch

* **Log Filename Validation:** Filenames passed to `Start` must be plain filenames (e.g., `"app.log"`). Passing names containing path separators (e.g., `"subdir/app.log"`) or back-references (e.g., `"../evil.log"`) will return `ErrInvalidLogFilename`.
* **Symlink Protections:** If the base path directory or target log file is a symlink, initial startup or rotation will return `ErrUnsafeSymlink`.
* **Restrictive Permissions:** Newly created directories default to `0700` and log files default to `0600`. Use `WithFileMode` and `WithDirMode` if group access is strictly necessary.
* **Writes after Close:** Invocations of log methods after `Close()` returns will be silently ignored (incrementing `DroppedTotal`) without blocking or panicking.
