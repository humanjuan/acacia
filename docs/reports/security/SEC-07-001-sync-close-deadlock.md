# Security Finding

*This is a historical engineering report. The filename is kept for audit continuity.*

## Finding ID: SEC-07-001

### Metadata
- **Severity:** High
- **Vector Type:** Deadlock
- **Date Identified:** 2026-07-08
- **Status:** Remediated

### Vulnerability Description
In its previous version, the Acacia logger allowed producer log threads to block indefinitely under the `BlockOnFull` policy if the logger was closed via `Close()`. This occurred because producers were left trapped trying to send to the `_log.events` channel without alternative selectors, and the channel was never drained or unblocked asynchronously, causing a complete hang in critical application threads.

### Impact
Denial of Service (DoS) of the production process or the entire application service, as execution threads emitting logs under congestion hang indefinitely, exhausting thread pools or other server connections.

### Code Snippet Reference
Before the fix:
```go
case BlockOnFull:
    _log.events <- ev
    atomic.AddUint64(&_log.enqueuedSeq, 1)
```

### Proof of Concept (PoC)
1. Start the logger with a buffer size of 5 and the `BlockOnFull` policy.
2. Launch 10 goroutines writing logs concurrently to fill the buffer.
3. Call `lg.Close()` from the main goroutine.
4. The producer goroutines blocked on the channel hang permanently, preventing a clean shutdown and hanging the application.

### Remediation Plan
The queueing function `enqueueEvent` was redesigned using `select` statements along with `case <-_log.done:`. By closing the `_log.done` channel at the beginning of `Close()`, all blocked sending goroutines are instantly released in a safe manner, discarding pending messages to the queue and terminating without deadlocks.
