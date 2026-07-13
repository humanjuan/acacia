# ADR 0004: Structured JSON Maturity

* Status: Proposed
* Date: 2026-07-10
* Authors: Senior Go Architecture & Performance Engineering Agent

## Context

Acacia currently has strong flat structured logging support and legacy dynamic JSON support. However, modern structured logging users expect first-class support for nested objects, arrays, groups/namespaces, safe raw JSON and recursive redaction.

## Decision Drivers

- Maintain v2.x compatibility.
- Preserve fast flat builder path.
- Avoid reflection in recommended APIs.
- Keep `map[string]interface{}` as compatibility/legacy path.
- Introduce typed object/array builders.
- Define RawJSON security behavior.
- Define Any as explicit slow fallback.
- Preserve redaction across nested data.
- Preserve JSON validity under fuzzing.
- Avoid final performance claims until SSD validation.

## Options

### Option 1: Keep Flat Builder Only
In this option, we do not add nested object or array builders, keeping the builder flat. The legacy map path is the only way to log nested structures.
- *Pros*: Simplest code, no extra APIs.
- *Cons*: Users must use the slow legacy path for nested data, defeating the performance purpose of Phase 02/03.

### Option 2: Add Object/Array Builder
Add typed callbacks for objects and arrays that write directly to the buffer.
- *Pros*: High performance, zero reflection on recommended hot paths.
- *Cons*: Increases API surface and encoder complexity.

### Option 3: Add Full Dynamic Reflection-Based JSON
Automatically marshal any nested structure using reflection on every call.
- *Pros*: Simple API for users.
- *Cons*: Extremely high CPU and memory overhead (boxing/allocations), destroying performance goals.

### Option 4: Hybrid API
Provide a high-performance typed builder with callback APIs for objects and arrays, keep the legacy map-based path for compatibility, and add an explicit `Any` method documented as a slow fallback.
- *Pros*: Best tradeoff. High-performance paths are reflection-free, while convenience paths remain available and clearly isolated.
- *Cons*: Dual API paths require clear documentation.

## Proposed Direction

Option 4:
- flat builder remains fastest path;
- object/array builder provides multi-level JSON without reflection;
- Any/legacy map remain explicit slow compatibility paths;
- RawJSON is opt-in and validated;
- recursive redaction is mandatory for typed builders.

## Non-Goals

- No OpenTelemetry implementation in this phase.
- No SSD release validation in this phase.
- No breaking API changes.
- No dependency-heavy JSON framework.

## Acceptance Criteria
- Unit tests and race detector tests must pass cleanly.
- Reflection is not used in the recommended nested object/array builder hot paths.
- Recursive redaction must filter credentials inside objects and arrays.
- Fuzzing validates JSON validity and escaping under all inputs.
- Clearly documented performance tradeoffs between fast path and slow Any/legacy fallback.
