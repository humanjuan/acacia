# Migration Strategy Guide

## Who Should Migrate
Projects that log using `StructuredJSON(true)` and pass `map[string]interface{}` payloads to the legacy Acacia formatting path should migrate to the typed structured builders to improve throughput and reduce memory allocations.

---

## Legacy Logging Patterns vs. Recommended Replacements

### 1. Flat Map Logging

**Legacy Pattern (Map):**
```go
lg.StructuredJSON(true)
lg.Info(map[string]interface{}{
    "service":    "payments",
    "status":     200,
    "cache_hit":  true,
})
```

**Recommended Replacement (Typed Builders):**
```go
lg.InfoEvent().
    Str("service", "payments").
    Int("status", 200).
    Bool("cache_hit", true).
    Msg("request completed")
```
*Benefits*: The typed builder bypasses reflection and dynamic interface conversions.

---

### 2. Nested Maps

**Legacy Pattern (Map):**
```go
lg.Info(map[string]interface{}{
    "user": map[string]interface{}{
        "id":      "u_123",
        "country": "CL",
    },
})
```

**Recommended Replacement (Object Builder):**
```go
lg.InfoEvent().
    Object("user", func(u *acacia.Object) {
        u.Str("id", "u_123").
        Str("country", "CL")
    }).
    Msg("user loaded")
```

---

### 3. Slices of Records

**Legacy Pattern (Slice):**
```go
lg.Info(map[string]interface{}{
    "items": []interface{}{
        map[string]interface{}{"sku": "ABC", "qty": 2},
        map[string]interface{}{"sku": "XYZ", "qty": 1},
    },
})
```

**Recommended Replacement (Array Builder):**
```go
lg.InfoEvent().
    Array("items", func(a *acacia.Array) {
        a.Object(func(o *acacia.Object) {
            o.Str("sku", "ABC").
            Int("qty", 2)
        })
        a.Object(func(o *acacia.Object) {
            o.Str("sku", "XYZ").
            Int("qty", 1)
        })
    }).
    Msg("cart evaluated")
```

---

### 4. Dynamic Payloads

**Legacy Pattern (Map):**
```go
// Dynamic map whose keys aren't known at compile-time
lg.Info(someDynamicMap)
```

**Recommended Replacement (Any / RawJSON):**
* **Case A (Pre-serialized JSON)**: If the payload is already JSON-encoded, use `RawJSON`:
  ```go
  lg.InfoEvent().
      RawJSON("payload", jsonBytes).
      Msg("raw response logged")
  ```
  *(Make sure the payload is safe and credentials are redacted, as RawJSON operates on a trusted input basis).*
* **Case B (Unknown Go Structure)**: If it is a generic Go struct or map, use `Any` as a slow path fallback:
  ```go
  lg.InfoEvent().
      Any("payload", someStruct).
      Msg("dynamic payload logged")
  ```

---

## Migration Checklist
Use the following checklist to guide the transition:

- [ ] Replace hot-path `map[string]interface{}` logs with typed fields.
- [ ] Replace nested maps with `Object`.
- [ ] Replace slices of objects with `Array(... Object(...))`.
- [ ] Keep `RawJSON` only for trusted pre-redacted JSON.
- [ ] Keep `Any` only for low-frequency dynamic payloads.
- [ ] Add tests for sensitive-key redaction.
- [ ] Run `go test ./...` and `go test -race ./...`.

---

## Common Mistakes

### 1. Reusing Event Builders
Acacia event builders (`*Event`, `*Object`, `*Array`) are pooled. Once `Msg()` or `Send()` is called, they are recycled. Do not retain pointers to them:
```go
// WRONG: Storing builder references
ev := lg.InfoEvent().Str("k", "v")
ev.Msg("first message")
ev.Str("k2", "v2") // Panics or causes data corruption!
```

### 2. Leaking Pointers inside Callbacks
Do not leak builder references outside the scope of callback functions:
```go
// WRONG: Storing inner object pointer
var userObj *acacia.Object
lg.InfoEvent().Object("user", func(o *acacia.Object) {
    userObj = o
})
userObj.Str("illegal", "leak") // Data race / heap violation!
```
Ensure all fields are written sequentially within the callback block.
