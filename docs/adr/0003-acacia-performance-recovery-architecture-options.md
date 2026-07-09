# ADR 0003: Acacia Performance Recovery Architecture Options

* **Status**: Proposed
* **Date**: 2026-07-09
* **Authors**: Senior Go Architecture & Performance Engineering Agent

---

## Context
Following the completion of Fase 08, a hard performance validation was executed on a temporary SSD clone of the repository. While the release candidate was proven to be functionally stable, thread-safe, and secure, performance profiling revealed regressions compared to the pre-roadmap baseline `v2.3.0`. 

Specifically:
- Raw string and raw byte paths degraded by +21% to +23% in latency.
- Messages containing 1KB payloads degraded significantly, with bytes/op increasing up to +92% and higher B/op.
- State checks, queue coordination, and buffer growth under write pressure are suspected to be the primary causes of this overhead.

This ADR outlines the architectural alternatives proposed to recover the performance baseline while strictly maintaining the security fixes, backpressure correctness, and Sync/Close behaviors established in Fases 01–08.

---

## Options Considered

### Option 1: Incremental Optimization
This option focuses on minor, surgical adjustments to the existing pipeline without altering the core architecture.
- **Actions**:
  - Optimize `appendLineBytes` to pre-grow slice capacity based on heuristic message sizes, avoiding incremental heap re-allocations.
  - Inline state-checking operations.
  - Cache atomic reads where possible to minimize processor bus locks in the hot path.
  - Introduce cache-line padding to prevent false sharing on internal state counters.
- **Pros**: Low risk; fast implementation; preserves most of the current code flow.
- **Cons**: Might not resolve the root architectural overhead of scheduling and atomic coordination; may hit a performance ceiling.

### Option 2: Writer-Owned Sequencing
In the current design, the producer thread performs atomic operations, state checks, and some serialization coordinates. This option moves sequencing and state validation logic into the background writer goroutine.
- **Actions**:
  - The producer goroutine performs minimal checks and enqueues events.
  - The background writer goroutine acts as the single source of truth for ordering, rotation boundaries, and state transitions.
  - Sincronization barriers (Sync/Close) are handled entirely within the writer's event loop.
- **Pros**: Substantially reduces the CPU cycles spent by the producer (caller) in the hot path.
- **Cons**: Increases the complexity of the background writer loop; synchronization details for Sync/Close require careful design to avoid deadlock regressions.

### Option 3: Multi-Mode Engine
This option introduces specialized engines or modes (e.g., Fast Path vs. Fully Configured).
- **Actions**:
  - When advanced features (such as redaction, filesystem safety checks, or backpressure) are not configured, the logger dynamically routes requests through an ultra-light write engine that aims to approach no measurable overhead in documented benchmarks.
  - When these features are enabled, the logger transparently shifts to the robust safety-first pipeline.
- **Pros**: aims to approach or match v2.3.0 baseline performance for simple raw text/bytes use cases.
- **Cons**: Increases testing matrix; duplicates logical paths; code complexity grows; fails to solve the regression for users who *do* need redaction or backpressure.

### Option 4: Partial Rebuild
A partial reconstruction of the writer pipeline, event buffer manager, and sequencing synchronization primitives.
- **Actions**:
  - Redesign the queue-to-writer bridge.
  - Implement a contiguous ring-buffer manager or a multi-buffer pooling system to replace slice-based appends.
  - Re-evaluate how channels and mutexes are combined in the synchronization layer.
- **Pros**: Offers the highest performance potential; removes historical design debt.
- **Cons**: High risk; extends the release timeline; requires deep regression testing (fuzzing, race, stress).

---

## Decision Criteria
The final decision will be based on the following priorities:
1. **Performance Recovery**: The chosen option must recover at least 50% of the hot-path latency regression and reduce 1KB payload memory usage (`B/op`) significantly.
2. **Security & Stability Guard**: No regression in JSON validation, symlink checks, path traversal prevention, or deadlock safety is permitted.
3. **Complexity & Maintainability**: Avoid unnecessary code duplication or cognitive overhead for maintainers.
4. **OTel Compatibility**: The design must accommodate optional OpenTelemetry integrations without measurable overhead in documented benchmarks on core logging when disabled.

---

## Trade-offs and Comparisons

| Dimension | Option 1: Incremental | Option 2: Writer-Owned | Option 3: Multi-Mode | Option 4: Rebuild |
|---|---|---|---|---|
| **Performance Gain** | Moderate | High | Maximum (unconfigured) | Maximum (all modes) |
| **Implementation Risk** | Very Low | Medium-High | Medium | High |
| **Code Complexity** | Low | High | High | Very High |
| **Testing Impact** | Minimal | Medium | High | Very High |

---

## Decision Pending
No decision has been finalized. Diagnostics planned in **Sprint 09.1** (specifically CPU/memory profiles and isolated micro-benchmarks of atomic operations and buffer growth) will determine which option is selected. 

The recommendation is to start with **Option 1 (Incremental)**. If profiling reveals that atomic coordination and buffer growth cannot be sufficiently optimized within the current structure, we will evaluate transitioning to **Option 2 (Writer-Owned)** or **Option 4 (Partial Rebuild)**.

---

## Impact

### Impact on Security
- Any changes to serialization or queueing must continue to pass the established security tests (preventing JSON injection, secret leakage, and path traversal).
- Buffer recycling must be thoroughly vetted to ensure memory is properly cleared (zeroed out) to prevent information leakage across log records.

### Impact on Compatibility
- The public API surface (methods, structures, and initialization signatures) must remain backward-compatible with `v2.x`.
- Internal refactorings must not expose raw mutexes or internal queues to user space.

### Impact on OpenTelemetry
- The integration of OTel must be designed to plug cleanly into the selected architecture. 
- If Option 2 (Writer-Owned) is chosen, the OTel adapter can hook into the writer's background loop, minimizing overhead on the producer.
