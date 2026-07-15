# Structured JSON API Contract

## 1. Design Principles

To maintain consistency and correct behavior in Acacia Logger, the evolution toward a mature structured JSON API is guided by these principles:

- **v2.x Compatibility First:** The proposed API is fully backward compatible with the v2.x branch. The legacy API based on dynamic maps will keep working.
- **Explicit Hot Paths:** The hot path for flat JSON (`Str`, `Int`, etc.) remains the recommended high-performance route. It is designed for low allocation behavior, with benchmark results documented in Quality and Validation.
- **Nested Structures without Reflection:** We introduce a fluent API with callbacks to write nested objects and arrays sequentially to the buffer. This avoids the use of `map[string]interface{}` or reflection in the recommended paths.
- **Isolated Slow Paths:** The fallback for dynamic types (`Any`) and the legacy map API are kept for compatibility, but are documented and labeled as slow paths in benchmarks.
- **Safe RawJSON:** Passing pre-serialized JSON is validated to ensure structured syntax. It is documented as "trusted input only" (no internal redaction) to avoid the overhead of processing the string.
- **Mandatory Recursive Redaction:** Sensitive key sanitization propagates down into nested objects and collections.
- **Deferred SSD Validation:** Final performance validation of the release candidate is run on the internal SSD. Comparisons on external drives serve only for diagnostic telemetry.

---

## 2. Flat Builder API (Flat JSON)

This is the recommended path for most operational logs.

### Usage Example
```go
lg.InfoEvent().
    Str("service", "payments").
    Int("status", 200).
    Bool("cache_hit", true).
    Dur("latency", latency).
    Msg("request completed")
```

### Builder Methods
- `Str(key, val string) *Event`
- `Int(key string, val int) *Event`
- `Int64(key string, val int64) *Event`
- `Uint64(key string, val uint64) *Event`
- `Bool(key string, val bool) *Event`
- `Float64(key string, val float64) *Event`
- `Dur(key string, val time.Duration) *Event`
- `Time(key string, val time.Time) *Event`
- `Err(err error) *Event`
- `Secret(key, val string) *Event`
- `StrRedacted(key, val string) *Event`
- `Msg(msg string)`
- `Send()`

### Edge Cases
- **Empty Keys or Values:** Written normally in the JSON output (`"key": ""`).
- **Nil Errors:** If `Err(nil)` is called, the field is omitted from the log.
- **Duplicate Keys:** To maintain hot-path speed, we do not deduplicate keys in memory. Both keys are emitted to the JSON output, delegating duplicate handling to the log shipper or SIEM (where typically the last write wins).
- **Control Character and Unicode Escaping:** The encoder escapes control characters (`\x00` - `\x1f`), quotes (`"`), backslashes (`\`), and carriage returns/newlines (`\n`, `\r`) to prevent log injection.
- **Invalid UTF-8:** Strings with invalid UTF-8 sequences are serialized safely using substitution sequences to ensure the output remains valid JSON.
- **Non-Finite Floats (NaN and Inf):** Non-finite float values (`math.NaN()`, `math.Inf(1)`, and `math.Inf(-1)`) are serialized as valid JSON strings: `"NaN"`, `"+Inf"`, and `"-Inf"`.

---

## 3. Object Builder (Nested Objects)

This builder allows structured, typed nested objects using callbacks without reflection.

### Usage Example
```go
lg.InfoEvent().
    Object("user", func(u *acacia.Object) {
        u.Str("id", "u_123")
        u.Object("profile", func(p *acacia.Object) {
            p.Str("country", "CL")
            p.Str("tier", "premium")
        })
    }).
    Msg("checkout completed")
```

### Design Contract
- **Nil Callbacks:** A `nil` callback is treated as a safe no-op, writing an empty object (`{}`).
- **Panic Handling:** If a callback function panics, the logger recovers, discards the partially written object from the buffer to prevent invalid JSON, and appends a constant field `"_acacia_error": "object callback panic"`. The raw panic trace is omitted from the log to prevent credentials leaks.
- **Nesting Limits:** Nesting is supported. We recommend a limit of 5 levels to avoid call stack overflows.
- **Recursive Redaction:** Sensitive keys are redacted automatically inside nested objects.

---

## 4. Array Builder (Collections)

This builder allows structured, typed lists and arrays in the output stream.

### Usage Example
```go
lg.InfoEvent().
    Array("items", func(a *acacia.Array) {
        a.Object(func(i *acacia.Object) {
            i.Str("sku", "ABC")
            i.Int("qty", 2)
        })
        a.Str("manual-note")
        a.Int(42)
    }).
    Msg("cart evaluated")
```

### Design Contract
- **Supported Methods (Current core support):** The array builder supports typed additions (`Str`, `Int`, `Int64`, `Uint64`, `Bool`, `Float64`, `Time`, `Dur`, `Err`) and nested objects (`Object`).
- **Deferred Items:** Nested arrays, `RawJSON` in arrays, and `Any` inside arrays are deferred.
- **Empty or Nil Arrays:** Written safely as `[]`.
- **Recursive Redaction:** Automatically applied to sensitive fields inside objects within arrays.
- **Nil Callbacks or Panics:**
  - A `nil` array callback writes `[]`.
  - A panic in the array callback rolls back the array field and appends `"_acacia_error": "array callback panic"` to the parent element.
  - A panic inside an individual `Object` callback rolls back that item and writes `{"_acacia_error": "object callback panic"}` inside the array, keeping other elements intact.
- **Err and Float64:** `Err(nil)` is a no-op. `Err(error)` writes the message as a JSON string. Non-finite `Float64` values write valid JSON strings.

---

## 5. Group vs Namespace

We provide both options for different use cases:

### 5.1 Group (Nested JSON)
```go
lg.InfoEvent().Group("http", func(g *acacia.Object) {
    g.Str("method", "GET")
})
```
*Output:* `{"http":{"method":"GET"}}`
- **Semantics:** Group is an alias for `Object`. `Event.Group` delegates to `Event.Object` and `Object.Group` delegates to `Object.Object`.
- **Panic Handling:** If the callback panics, the group is rolled back and `"_acacia_error":"object callback panic"` is written to the parent element.
- **Nil or Empty Callbacks:** A `nil` or empty callback writes `{}` safely.

### 5.2 Namespace (Flat Prefixed Keys)
```go
lg.InfoEvent().Namespace("http", func(n *acacia.Namespace) {
    n.Str("method", "GET")
})
```
*Output:* `{"http.method":"GET"}`
- **Dedicated Scope:** The callback receives `*acacia.Namespace`, which does not expose `Msg()` or `Send()` to prevent premature line endings.
- **Zero-Value:** A zero-value instance of `acacia.Namespace` is a safe no-op.
- **Nil or Empty Callbacks:** If the callback is `nil` or does not write fields, the namespace writes nothing to the output.
- **Empty Prefix:**
  - If `prefix == ""`, the key is written as `key`.
  - If `key == ""` and `prefix != ""`, the key is written as `prefix + "."`.
  - If both are empty, an empty key `""` is written.
- **Escaping:** The prefix and the key are escaped using `escapeJSONInto`.
- **Panic Handling:**
  - In `Event.Namespace`, a panic rolls back to the initial state and writes `,"_acacia_error":"namespace callback panic"` at the root.
  - In `Object.Namespace`, a panic rolls back and writes `,"_acacia_error":"namespace callback panic"` inside the object, restoring the comma delimiter state.
- **Redaction:** Case-insensitive matching is run without dynamic compound key generation on:
  - The leaf key.
  - The prefix.
  - Any dot/hyphen/underscore delimited segment within key or prefix.
  - The final compound key.
- **Special Values:**
  - Non-finite `Float64` values write valid JSON strings.
  - `Err(nil)` is a no-op. `Err(err)` and `ErrKey(key, err)` write the error message as a JSON string under the prefixed key.

---

## 6. Safe RawJSON

Allows direct injection of pre-serialized JSON bytes.

### Usage Example
```go
lg.InfoEvent().
    RawJSON("payload", rawBytes).
    Msg("received payload")
```

### Design Contract
- **Supported Methods:** `Event.RawJSON(key, raw)` and `Object.RawJSON(key, raw)`. Versions for `Array` and `Namespace` are deferred.
- **Validation and Degradation:** Accepts valid JSON according to `json.Valid`. If the payload is invalid, it is safely serialized as an escaped JSON string.
- **Nil or Empty Payloads:** A `nil` or empty `[]byte{}` payload is serialized as an empty string `""`.
- **Line Injection and CR/LF Protection:** If valid JSON contains newlines (`\n` or `\r`), it is compacted using `json.Compact` to keep the output on a single line.
- **Redaction:**
  - **External:** If the top-level key matches a sensitive pattern, the entire payload is replaced with `"[REDACTED]"`.
  - **Internal (Trusted Input Only):** If the key is not sensitive, the payload is written directly without internal parsing or redaction, assuming it is pre-sanitized.
- **Deferred Items:** `Array.RawJSON` and `Namespace.RawJSON` are deferred.

---

## 7. Any / Interface Fallback

A dynamic fallback path.

### Usage Example
```go
lg.InfoEvent().
    Any("payload", structValue).
    Msg("debug payload")
```

### Design Contract
- **Performance:** Processed as a slow path using `encoding/json` and reflection. Not recommended for high-frequency logs.
- **Supported Methods:** `Event.Any(key, val)` and `Object.Any(key, val)`. Versions for `Array` and `Namespace` are deferred.
- **Redaction:**
  - **External:** If the top-level key is sensitive, the entire value is replaced with `"[REDACTED]"`.
  - **Internal (Best-Effort):** If the key is not sensitive, maps, structs, and slices are traversed recursively. Fields matching sensitive patterns are replaced with `"[REDACTED]"`. Perfect redaction is not guaranteed on highly complex or custom types.
- **Error Handling:** Un-serializable values (functions, channels, cyclic references) write a safe error object: `{"_acacia_error":"any marshal error"}`.

---

## 8. Mandatory Recursive Redaction

We audit recursive, case-insensitive redaction for these sensitive keys:
`password`, `passwd`, `pwd`, `token`, `access_token`, `refresh_token`, `authorization`, `authorization_header`, `cookie`, `set-cookie`, `api_key`, `apikey`, `secret`, `client_secret`, `private_key`, `bearer`, `jwt`, `session`, `session_id`, `csrf`, `xsrf`, `x-api-key`.

### Redaction Rules by Method:
- **Flat Builder / Event:** Checks the top-level key using static lookup.
- **Object Builder:** Checks recursive key names inside nested fields.
- **Array Builder:** Redacts nested fields within objects added to the array.
- **Group:** Inherits automatic recursive redaction from the Object Builder.
- **Namespace:** Checks prefix, key, individual segments, and final compound keys.
- **RawJSON:** Redacts the entire payload as `"[REDACTED]"` if the top-level key matches. Does not perform internal parsing.
- **Any Fallback:** Traverses maps, structs, and slices recursively to replace sensitive fields.
- **Legacy Map Path:** Simple reflective map support without the memory-efficient recursive features of the new builders.

---

## 9. Concurrency and Ownership

- **Concurrency:** Event builders (`*Event`, `*Object`, `*Array`) **are not thread-safe**. They must be used sequentially in a single goroutine.
- **Ownership:** Once `Msg()` or `Send()` is called, the event builder is released and must not be used again.
- **Callback Pointers:** Callback functions must not retain references to `*Object` or `*Array` outside the callback scope.

---

## 10. Current API Surface

Implemented:
- Flat typed fields on `Event`.
- `Object` on `Event` and `Object`.
- `Array` on `Event` and `Object`.
- `Group` on `Event` and `Object`.
- `Namespace` on `Event` and `Object`.
- `RawJSON` on `Event` and `Object`.
- `Any` on `Event` and `Object`.

Deferred:
- Nested arrays.
- `Array.RawJSON`.
- `Namespace.RawJSON`.
- `Array.Any`.
- `Namespace.Any`.
- OpenTelemetry adapter.
