# Phase 10 — Flat JSON Builder Hardening Report
*This is a historical engineering report. The filename is kept for audit continuity.*

## 1. Objective and Scope
The primary objective of Sprint 10.1 is to strengthen and harden the flat JSON path of the structured builder in Acacia Logger.
This hardening includes:
* Safe JSON encoding and escaping of control characters, Unicode, and invalid UTF-8.
* Implementation and verification of behavior regarding duplicate keys, empty values, and `Err(nil)`.
* Case-insensitive redaction of sensitive keys at the root of the object.
* Mitigation of log injection via strict escaping of carriage returns and line feeds (`\r\n`).
* Ensuring that no reflection or `encoding/json` dependencies are introduced into the hot path.
* Safe handling of non-finite `float64` values (`NaN`, `+Inf`, `-Inf`) by encoding them as valid JSON strings instead of unquoted numeric literals.
* Encoder optimization via a fast-path scan optimized for ASCII characters, with a safe, dynamic fallback to UTF-8 decoding only when non-ASCII bytes are detected.

---

## 2. Modified and Created Files
* **Modified**:
  * [acacia.go](../../acacia.go): Import of `unicode/utf8` and `math`, implementation of escaping with ASCII fast-path and safe UTF-8 fallback in `escapeJSONInto`, update of `Event` builder methods to escape both keys and values, and addition of the filter for non-finite floats.
* **Created**:
  * [test/phase10_flat_json_builder_test.go](../../test/phase10_flat_json_builder_test.go): Unit test suite covering the 12 key validation scenarios.
  * [test/phase10_flat_json_builder_fuzz_test.go](../../test/phase10_flat_json_builder_fuzz_test.go): Fuzz target to validate hostile UTF-8 sequences, CRLF, and redaction under infinite permutations.
  * [test/phase10_flat_json_builder_benchmark_test.go](../../test/phase10_flat_json_builder_benchmark_test.go): Diagnostic benchmark suite.

---

## 3. Implementation Details and v2.x Compatibility
* **Safe Escaping with ASCII Fast-Path**: A byte-scanning logic was implemented in `escapeJSONInto`. As long as the inspected characters are safe ASCII (`c < utf8.RuneSelf`), they are copied directly in continuous memory blocks. If a special character requiring escaping (`"`, `\`, control characters, newlines, etc.) is detected, the corresponding escape sequence is written and scanning resumes. Only if a byte greater than or equal to `utf8.RuneSelf` (non-ASCII) is detected, a safe fallback to `utf8.DecodeRuneInString` is performed, which decodes and replaces invalid sequences with the replacement rune `\uFFFD`.
* **Key Escaping**: All methods of the structured builder were extended to pass both the key and the value through the `escapeJSONInto` encoder, preventing syntax injection through user-controlled keys.
* **Non-Finite Floats**: The `Float64` method preserves finite numeric values as literal numbers in the structured output, but detects and degrades non-finite values (`NaN`, `+Inf`, `-Inf`) to safe JSON strings (`"NaN"`, `"+Inf"`, `"-Inf"`).
* **Duplicate Keys**: Following the optimal performance principle, no hot deduplication is performed. Both keys are written directly sequentially to the buffer ("last write wins" at the ingestion layer).
* **Err(nil)**: If a `nil` error is passed via `Err(nil)`, it is ignored and omitted from the final JSON to maintain compatibility with existing behavior.
* **Redaction**: The `isSensitiveKey` method normalizes and filters sensitive keys in a case-insensitive manner, replacing the original value with `[REDACTED]`. The `.Secret()` and `.StrRedacted()` methods redact the value in an explicitly safe manner.
* **Compatibility**: No destructive signature changes or breaking changes were introduced for v2.x. The compatibility API for the legacy map-based path (`StructuredJSON(true)` + `Info(map...)`) works exactly the same.

---

## 4. Evidence of Correctness and Concurrency Testing
The complete unit test suite was executed with the new tests included:

### General Unit Tests (`go test ./...`)
```text
?   	github.com/humanjuan/acacia/v2	[no test files]
?   	github.com/humanjuan/acacia/v2/examples/basic	[no test files]
?   	github.com/humanjuan/acacia/v2/internal/logging	[no test files]
ok  	github.com/humanjuan/acacia/v2/test	13.731s
```

### Focused Unit Tests (`go test ./test -run 'TestPhase10FlatJSON'`)
All behavioral correctness tests passed successfully:
```text
ok  	github.com/humanjuan/acacia/v2/test	0.813s
```
Suite tests executed:
* `TestPhase10FlatJSONEscapesControlCharacters`
* `TestPhase10FlatJSONEscapesQuotesAndBackslashes`
* `TestPhase10FlatJSONRejectsLogInjectionByEscapingCRLF`
* `TestPhase10FlatJSONInvalidUTF8StillValidJSON`
* `TestPhase10FlatJSONDuplicateKeysAreEmitted`
* `TestPhase10FlatJSONEmptyKeyAndEmptyValue`
* `TestPhase10FlatJSONNilErrorIsOmitted`
* `TestPhase10FlatJSONSensitiveKeysAreRedacted`
* `TestPhase10FlatJSONSecretAndStrRedacted`
* `TestPhase10FlatJSONLineIsValidJSON`
* `TestPhase10FlatJSONLegacyMapPathStillWorks`
* `TestPhase10FlatJSONFloat64NonFiniteStillValidJSON`

### Race Detector (`go test -race ./...`)
The race detector was run across the entire repository test suite, and no data races were reported under the executed suite:
```text
?   	github.com/humanjuan/acacia/v2	[no test files]
?   	github.com/humanjuan/acacia/v2/examples/basic	[no test files]
?   	github.com/humanjuan/acacia/v2/internal/logging	[no test files]
ok  	github.com/humanjuan/acacia/v2/test	25.937s
```

---

## 5. Fuzzing Validation
The fuzz target `FuzzPhase10FlatJSONStringEscaping` was executed continuously for 60 seconds, covering more than 180,000 iterations with random hostile UTF-8 sequences, keys, values, and newlines:
```text
fuzz: elapsed: 0s, gathering baseline coverage: 0/93 completed
fuzz: elapsed: 0s, gathering baseline coverage: 93/93 completed, now fuzzing with 12 workers
fuzz: elapsed: 3s, execs: 20836 (6945/sec), new interesting: 2 (total: 95)
...
fuzz: elapsed: 1m1s, execs: 180566 (0/sec), new interesting: 6 (total: 99)
PASS
ok  	github.com/humanjuan/acacia/v2/test	61.697s
```
**Result**: No panics, no corrupt JSON outputs, and no raw newlines within the serialized keys/values.

---

## 6. Summary of Diagnostic Benchmarks
The structured fast-path benchmarks were collected on the external development disk with 10 repetitions.

### Representative Observed Values
| Benchmark | Latency (ns/op) | Memory (B/op) | Allocations (allocs/op) |
| :--- | :---: | :---: | :---: |
| `BenchmarkPhase10FlatJSONBuilderSmall` | ~830 ns/op | ~33 B/op | 1 allocs/op |
| `BenchmarkPhase10FlatJSONBuilderTenFields` | ~1200 ns/op | ~27 B/op | 1 allocs/op |
| `BenchmarkPhase10FlatJSONBuilderEscapedString` | ~590 ns/op | ~29 B/op | 1 allocs/op |
| `BenchmarkPhase10FlatJSONBuilderRedactedField` | ~620 ns/op | ~84 B/op | 1 allocs/op |
| `BenchmarkPhase10FlatJSONBuilderDuplicateKeys` | ~670 ns/op | ~42 B/op | 1 allocs/op |
| `BenchmarkPhase10FlatJSONBuilderDisabledLevel` | ~15 ns/op | 0 B/op | 0 allocs/op |
| `BenchmarkPhase10FlatJSONBuilderASCIIString` | ~630 ns/op | ~40 B/op | 1 allocs/op |
| `BenchmarkPhase10FlatJSONBuilderInvalidUTF8String` | ~660 ns/op | ~64 B/op | 1 allocs/op |

### Performance and Allocation Analysis
* **Disabled Level**: Shows minimal overhead (~15 ns/op) and `0 allocs/op` (no heap allocations when the log level is disabled).
* **Builder Allocations (1 alloc/op)**: The likely cause is associated with sync.Pool/interface accounting; this should be confirmed with escape analysis if 0 allocs/op is targeted in the active path. No reflection or heavy marshaling of any kind exists in this hot path.

---

## 7. Sprint Confirmations
* **No SSD Hard Validation**: All benchmarks and diagnostic tests were executed on the external development disk. Final release validation on controlled internal SSD is deferred to the end of Phase 10.
* **Not Implemented**: Implementation of the `Object Builder`, `Array Builder`, `Group`, `Namespace`, `RawJSON`, `Any`, and `OpenTelemetry` support was omitted to comply strictly with the boundaries of this sprint.
* **Pending Risks**: Validate the CPU impact and resource usage of rune decoding on giant strings (>10KB).

---

*The complete raw results were saved in the private directory for internal auditing.*
