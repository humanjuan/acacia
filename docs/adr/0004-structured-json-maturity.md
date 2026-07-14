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

## Decision Update — Sprint 10.0 API Contract

During Sprint 10.0, the following API design decisions were formulated and recorded in the API Contract:
- **Namespace Scope**: `Namespace` uses a dedicated `*acacia.Namespace` callback scope instead of `*acacia.Event`. This scope does not expose `Msg()` or `Send()`, preventing premature event termination.
- **Array Nesting**: Nested arrays (arrays within arrays) are excluded from the initial Array Builder scope and postponed as a future extension. Only primitive arrays and arrays of objects are supported initially.
- **Panics in Callbacks**: Safe recovery is enforced. If a panic is caught during callback execution, a constant error field (`"_acacia_error": "object callback panic"` or `"array callback panic"`) is added. Raw panic details/messages are not logged by default to prevent credential leakage.
- **RawJSON**: Accepts any valid JSON value according to `json.Valid` (object, array, string, number, bool, null). It remains trusted input only, with no internal redaction, and invalid payloads degrade to safely escaped strings.
- **Zero Allocations**: Clarified that 0 allocs/op is a target for benchmarks on the flat fast path, not a general framework guarantee.
- **Duplicate Keys**: Handled as "last write wins" at the consumer layer. No deduplication logic will be run in the builder's fast path to avoid performance penalties.
- **Any Fallback**: Explicitly marked as a slow path that utilizes reflection/marshaling.
- **Recursive Redaction**: Mandatory Case-Insensitive filtering of credentials across all nested objects and arrays.

The status remains **Proposed** until the implementation phase starts and initial checks are completed.

## Decision Update — Sprint 10.2 Object Builder

During Sprint 10.2, the callback-based `Object` builder API was designed and implemented:
- **Direct Buffer Access**: The `Object` struct holds a pointer to the event's buffer (`*[]byte`), serializing fields directly into the destination space to avoid intermediate buffer allocations.
- **Rollback on Panic**: Before running the callback, the write offset in the buffer is recorded. If a panic occurs, the buffer is rolled back to that offset, and the safe constant indicator `"_acacia_error": "object callback panic"` is written.
- **Nil Callbacks**: Nil callbacks are safely intercepted, emitting an empty object `{}`.
- **Recursive Redaction**: Recursive sensitive-key check via `isSensitiveKey` normalization is enforced case-insensitively across nested levels.
- **Escape Safety**: Keys and values inside nested objects are escaped via `escapeJSONInto` with ASCII fast-path scanner.
- **Float64 Safety**: Reuses the Float64 safe non-finite float rules.

## Decision Update — Sprint 10.3 Array Builder

During Sprint 10.3, the callback-based `Array` builder API was designed and implemented:
- **Core Elements**: The `Array` struct supports typed primitive elements and nested `Object` elements without reflection.
- **Rollback and Panic Safety**:
  - Panic in the array callback rolls back the entire array field and appends `"_acacia_error": "array callback panic"`.
  - Panic inside a nested `Object` element within the array rolls back only that object element, appending `{"_acacia_error": "object callback panic"}` within the array, preserving all other elements.
- **Nil and Empty Callbacks**: Safely intercepted, emitting an empty array `[]`.
- **Zero-Value Safeness**: Instantiating a zero-value `Array` struct is a safe no-op.
- **Escaping and Redaction**: Values inside the array are properly escaped, and recursive redaction is fully enforced inside object elements within arrays.
- **Err and Float64**: `Err(nil)` is omitted, and non-finite `Float64` values (`NaN`, `+Inf`, `-Inf`) are stringified safely.
- **Deferred features**: Nested arrays, `RawJSON` in arrays, and `Any` inside arrays are deferred to keep the core complexity low.

## Decision Update — Sprint 10.4 Group and Namespace

During Sprint 10.4, Group and Namespace APIs were designed and implemented:
- **Group**: Implemented as a semantic alias for `Object`. It creates a nested JSON object structure by delegating `Event.Group` and `Object.Group` to `Event.Object` and `Object.Object` respectively. This inherits all benefits of `Object` including nil callbacks, rollback on panic, escaping, recursive redaction, and zero-value safety without duplicating the encoder logic.
- **Namespace**: Implemented as a dedicated, restricted scope `*acacia.Namespace` that writes flat, prefixed keys (`"prefix.key"`). It does not expose `Msg()` or `Send()`.
- **Zero-Value and Nil safety**: `Namespace` behaves as a safe no-op when nil or uninitialized. Nil and empty callbacks produce no output.
- **Rollback and Panic Safety**: Panic inside the `Namespace` callback rolls back all changes written within that namespace, restoring parent object state (`first` flag) and appending `"_acacia_error": "namespace callback panic"` inside the parent context.
- **Redaction and Escaping**: Key segments and compound keys are checked case-insensitively for sensitive patterns without allocation. Values are redacted with `[REDACTED]` if any portion of the prefix or key segment matches. Prefix and key are properly escaped using `escapeJSONInto`.
- **Err and Float64**: Non-finite Float64 values are mapped to string literals (`"NaN"`, `"+Inf"`, `"-Inf"`). `Err(nil)` is a no-op, and non-nil error values are appended under prefixed error keys.
- **Deferred features**: `RawJSON` is moved to active implementation (this sprint). `Any` remains deferred.

## Decision Update — Sprint 10.5 RawJSON
- **RawJSON Support**: Implemented `Event.RawJSON(key, raw)` and `Object.RawJSON(key, raw)` allowing pre-serialized JSON insertion directly as structured field values.
- **Syntactic Validation**: Standard library's `json.Valid` is used as the validation gate.
- **Log Injection and CR/LF Protection**: Pretty-printed or multiline valid JSON is compacted with `json.Compact` into a single line. If compaction fails or payload is syntactically invalid JSON, it is degraded safely to a JSON-escaped string literal.
- **Nil/Empty Safety**: Nil or empty `raw []byte` payload degrades to an empty string `""` safely without panicking.
- **Redaction Policy**: If the external key is sensitive, the entire payload is redacted to `"[REDACTED]"` at the root.
- **Trusted Input (No Internal Redaction)**: Valid, non-sensitive external keys do not trigger internal parsing or redaction of nested keys (contracted as *trusted input only*).
- **Deferred features**: `RawJSON` is moved to active implementation (previous sprint). `Any` is moved to active implementation (this sprint). OpenTelemetry integration remains deferred.

## Decision Update — Sprint 10.6 Any fallback
- **Any Support**: Implemented `Event.Any(key, val)` and `Object.Any(key, val)` for dynamic JSON serialization of arbitrary Go objects.
- **Slow-Path Fallback**: Documented as an explicit slow-path, utilizing `encoding/json` and isolated reflection.
- **Internal Redaction (Best-Effort)**: Valid nested maps, structs, and slices undergo recursive best-effort redaction. If a key matches a sensitive pattern, its value is replaced with `"[REDACTED]"`.
- **Robust Failure Fallback**: Non-serializable types (functions, channels, cyclic references, non-finite floats) do not panic; they degrade safely to `{"_acacia_error":"any marshal error"}`.
- **Diferimientos**: `Array.Any` and `Namespace.Any` remain deferred.

