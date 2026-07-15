# Security Finding

*This is a historical engineering report. The filename is kept for audit continuity.*

## Finding ID: SEC-07-003

### Metadata
- **Severity:** Medium
- **Vector Type:** Leak / Log Loss
- **Date Identified:** 2026-07-08
- **Status:** Remediated

### Vulnerability Description
The asynchronous logger did not guarantee the complete draining (flush) of log events that had been formally accepted into Acacia's buffer/queue before completing the closure of the physical file. This occurred because the writer routine terminated prematurely when the channel was closed, without ensuring the sequential processing of the remaining queued logs.

### Impact
Silent loss of critical audit records and diagnostic traces from the application shutdown phase. This is critical for security audits, as logs immediately preceding a shutdown (e.g., fatal error traces, intrusion alerts, service disconnection messages) were irreversibly lost.

### Code Snippet Reference
Before the fix:
```go
		case ev, ok := <-_log.events:
			if !ok {
				_log.flush()
				return
			}
```

### Proof of Concept (PoC)
1. Queue 50 identifiable log messages.
2. Immediately call `lg.Close()`.
3. Validate the resulting physical file and notice that only a portion of the 50 lines were written, silently losing the final events.

### Remediation Plan
A deterministic draining protocol was implemented within the handler for the `isClose` signal from the control channel. The asynchronous writer executes a non-blocking selective read loop over `_log.events` to completely drain all elements currently in the queue before proceeding with the final `flush` and the physical `f.Close()` of the file descriptor.
