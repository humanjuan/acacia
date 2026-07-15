# Security Finding

*This is a historical engineering report. The filename is kept for audit continuity.*

## Finding ID: SEC-07-002

### Metadata
- **Severity:** High
- **Vector Type:** Send on Closed Channel
- **Date Identified:** 2026-07-08
- **Status:** Remediated

### Vulnerability Description
The Acacia logger originally closed the asynchronous event channel `_log.events` directly within the exclusion block of `Close()`. Since multiple production goroutines queued logs concurrently without mutual synchronization locks, any attempt to write logs using `Info()`, `Warn()`, `Error()`, etc., concurrently with or after the invocation of `Close()` generated a `send on closed channel` panic in the Go runtime.

### Impact
Abrupt shutdown and complete crash of the application process in production due to uncaught exceptions when emitting log traces during or after the service shutdown phase.

### Code Snippet Reference
Before the fix:
```go
func (_log *Log) Close() {
    _log.closeOnce.Do(func() {
        ...
        if _log.events != nil {
            close(_log.events)
        }
        ...
    })
}
```

### Proof of Concept (PoC)
1. Start a goroutine writing concurrent logs indefinitely in Acacia.
2. Concurrently call `lg.Close()` from the main goroutine.
3. The Go runtime throws `panic: send on closed channel`, causing the application to crash.

### Remediation Plan
Acacia's shutdown semantics were redesigned to **never physically close** the `_log.events` channel. Instead, an atomic state machine control (`stateOpen`, `stateClosing`, `stateClosed`) was implemented. The `Close()` method notifies the writer via a dedicated control channel (`_log.control`) using the `isClose: true` signal, and any log call subsequent to `Close()` atomically checks the state and immediately discards the message, clearing the buffers without causing panics.
