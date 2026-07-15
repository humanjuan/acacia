# Structured JSON Maturity

This document outlines the implemented structured JSON logging capabilities in Acacia.

---

## Implemented Capabilities

- **Flat Typed Fields:** Direct builder methods on event structures (`Str`, `Int`, `Int64`, `Uint64`, `Bool`, `Float64`, `Dur`, `Time`, `Err`) writing key-value pairs directly to the buffer, avoiding interface allocations.
- **Nested Objects (`Object`):** Nested JSON maps defined via callback scopes (`func(obj *Object)`), permitting arbitrary multi-level nested serialization without intermediate reflection or map instantiation.
- **Typed Arrays (`Array`):** Sequential array record lists defined via callbacks (`func(arr *Array)`), supporting nested values, sub-objects, and lists.
- **Group Namespaces (`Group`):** Prefix scopes producing nested JSON maps in the serialized log (`func(g *Group)`) to modularize fields under domain namespaces.
- **Namespace Scopes (`Namespace`):** Prefix flat key scopes producing flattened keys in the final JSON output (e.g. `auth.username`), avoiding dynamic string concatenation overhead.
- **Raw JSON Injection (`RawJSON`):** Inject pre-serialized trusted JSON byte slices directly into the stream, automatically validated for single-line compliance and bracket closure.
- **Dynamic Fallback (`Any`):** Reflection-based dynamic value formatter accepting `interface{}` values. Automatically handles maps, structs (by reading json tags), slices, cyclic references, and non-serializable fields. Redacts sensitive fields recursively on a best-effort basis.

---

## Sensitive Data Redaction
All structured JSON builders are integrated with the core redaction engine. Keys matching sensitive terms (e.g. `password`, `token`, `authorization_header`) are automatically redacted. Explicit sensitive values can be marked using `.Secret()` or `.StrRedacted()`.
Case-insensitive matching (including common formats like `authorizationheader` or `authorization_header`) is enforced recursively across all nested levels (Objects, Arrays, and Groups).

---

## Deferred Items
The following structured logging features are deferred for future candidate releases:
* **Custom JSON Marshaler Interface:** Support for custom caller-defined `MarshalJSON` triggers within builders.
* **Map Path Optimizations:** High-performance pre-compiling of map structures (the current legacy map path remains a fallback).

---

## Key References
* [Structured JSON API Contract](structured-json-api-contract.md): Technical API specifications and design rules.
* [Structured JSON Builder Guide](../guides/structured-json-builder.md): Manual and examples for developers.
* [Structured JSON Migration Guide](../guides/structured-json-migration.md): How to migrate existing map logs.
