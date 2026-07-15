# Redaction Policy Guide

## Overview
Acacia Logger enforces case-insensitive, segment-aware redaction of sensitive credentials in its structured builder. This guide details matching behavior, builder compliance, and safety boundaries.

---

## Sensitive Key Families
Acacia checks fields against a list of sensitive patterns. Key names are normalized case-insensitively, ignoring punctuation/separators like `-`, `_`, and `.`.

Normalized matching matches the following keywords:
* `password`, `passwd`, `pwd`
* `token`, `access_token`, `refresh_token`
* `authorization`, `authorization_header`
* `cookie`, `set-cookie`
* `api_key`, `apikey`, `client_secret`, `secret`, `private_key`
* `bearer`, `jwt`, `session`, `session_id`, `csrf`, `xsrf`, `x-api-key`

### Examples of normalizations:
* `Authorization-Header` $\rightarrow$ `authorizationheader` (Redacted)
* `set_cookie` $\rightarrow$ `setcookie` (Redacted)
* `access.token` $\rightarrow$ `accesstoken` (Redacted)

---

## Builder Behavior Matrix

| Path / Builder | External Key Redaction | Internal Redaction | Notes |
| :--- | :---: | :---: | :--- |
| **Flat** | Yes | N/A | High-speed fast path |
| **Object** | Yes | Yes | Typed recursive object fields |
| **Array** | Key name only | Yes for object elements | Elements parsed individually |
| **Group** | Yes | Yes | Delegates to Object |
| **Namespace** | Yes by prefix/key/segments | Yes | Flat prefixed keys |
| **RawJSON** | Yes (replaces entire payload) | No | Trusted input only |
| **Any Fallback** | Yes (replaces entire payload) | Best-effort | Slow path fallback |
| **Legacy map** | Compatibility behavior | Limited/variable | Depends on reflect-based core |

---

## Safety Boundaries

### RawJSON Boundary (Trusted Input Only)
* **Behavior**: If the external key is sensitive, the entire RawJSON payload is redacted to `"[REDACTED]"`.
* **Trusted Input**: If the external key is not sensitive, the internal bytes are injected directly without parsing or modifying nested elements. 
* **Warning**: Do not pass raw sensitive nested payloads through RawJSON if the external key is not flagged as sensitive.

### Any Boundary (Best-Effort Redaction)
* **Behavior**: If the external key is sensitive, the entire payload is redacted. If it is not sensitive, the object is serialized, decodified, and recursively traversed to replace nested sensitive keys with `"[REDACTED]"`.
* **Caveat**: No perfect redaction guarantee. Complex nested interfaces, un-exported struct fields, or custom JSON marshalers might not be fully redacted.

### Panic Sentinels
If a callback passed to `.Object()`, `.Group()`, or `.Namespace()` panics:
* The transaction is rolled back to prevent incomplete/malformed JSON output.
* The error is captured and logged safely as `{"_acacia_error":"object callback panic"}` (or `"namespace callback panic"`).
* The raw panic message and stack traces are suppressed from the final log event by design to mitigate credential leakage.

### Unsupported and Cycle Sentinels for Any
To prevent infinite recursion or heap failures, channels, functions, cyclic references, and non-finite floats are serialised to a safe sentinel `{"_acacia_error":"any marshal error"}` without exposing internal structure.

### CR/LF and UTF-8 Control
* **CR/LF Injection**: Inputs containing Carriage Returns (`\r`) or Line Feeds (`\n`) are escaped to `\r` and `\n` literals respectively, preventing log splitting or log injection attacks.
* **UTF-8 Safety**: Invalid UTF-8 bytes are properly escaped using JSON string encoding standard.

---

## What Not to Do
1. Do not use `Any` as a high-frequency hot path; it incurs significant reflection overhead.
2. Do not pass sensitive raw payloads inside `RawJSON` unless the outer key is sensitive, or you have pre-redacted the payload.
3. Do not rely on the legacy map formatting path for strict recursive typed redaction guarantees.
