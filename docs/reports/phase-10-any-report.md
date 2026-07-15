# Phase 10 — Any Fallback Report
*This is a historical engineering report. The filename is kept for audit continuity.*

## 1. Objective and Scope
The primary objective of Sprint 10.6 is to design and incorporate support for dynamic serialization of arbitrary Go values under the structured API of Acacia Logger using an explicit slow path:
* **Event.Any**: Injects an `interface{}` value as a field at the event root.
* **Object.Any**: Injects an `interface{}` value inside sub-object builders.
* **Explicit Slow Path**: Isolated from the hot path by using the standard library `encoding/json` and internal reflection to resolve dynamic types. Its use is discouraged in high-frequency hot paths.
* **Full External Redaction**: If the name of the external key is sensitive, the entire payload is replaced with `"[REDACTED]"`.
* **Best-Effort Internal Redaction**: For non-sensitive keys, a decoding and recursive traversal is executed, replacing internal key values matching sensitive patterns with `"[REDACTED]"`.
* **Unsupported Types and Cycle Handling**: Cyclic references, functions, channels, and non-finite Float64 values (`NaN` or `Inf`) degrade safely to the sentinel `{"_acacia_error":"any marshal error"}` without causing runtime panics.
* **Nil Safety**: `nil` values are encoded standardly as `null`.

Support for `Array.Any`, `Namespace.Any`, or `OpenTelemetry` integrations is not included in this phase.

---

## 2. Modified and Created Files
* **Modified**:
  * `acacia.go`: Implementation of `Any` methods and helpers for redaction, validation, and slow serialization.
  * `docs/ACACIA_PROJECT_CONTEXT.md`: Update of the project state for Sprint 10.6.
  * `docs/PHASE_10_STRUCTURED_JSON_API_CONTRACT.md`: Refinement of the Any fallback contract behavior.
  * `docs/adr/0004-structured-json-maturity.md`: Inclusion of the technical decision record for Sprint 10.6.
  * `docs/reports/README.md`: Registration of the public report link.
* **Created**:
  * `test/phase10_any_test.go`: Suite of 23 correctness tests.
  * `test/phase10_any_fuzz_test.go`: Fuzz target to validate robustness, formatting, and escaping.
  * `test/phase10_any_benchmark_test.go`: Diagnostic benchmarks of the Any fallback.

---

## 3. Implementation Details and APIs

### Any API
Signatures were added to the main structures:
```go
func (e *Event) Any(key string, val interface{}) *Event
func (o *Object) Any(key string, val interface{}) *Object
```

### Slow Encoding Flow and Internal Redaction
1. If the value is `nil`, `null` is written directly.
2. The original value is serialized to bytes using `json.Marshal(val)`.
3. If serialization fails (due to cycles, functions, channels, or non-finite Float64 values), the sentinel `{"_acacia_error":"any marshal error"}` is safely written.
4. If successful, it is decoded into a generic structure (`interface{}`) via a `json.Decoder` with `UseNumber()` enabled to preserve numeric precision.
5. The decoded structure is recursively traversed, applying the `redactDecodedJSONValue` policy, replacing values of sensitive keys with `"[REDACTED]"`.
6. The resulting tree is re-serialized and injected using the safe RawJSON loader, applying newline compaction if necessary.

---

## 4. Correctness Testing Evidence

### Unit Tests (`go test ./...`)
Tests corresponding to Sprint 10.6 and earlier passed successfully:
```text
?   	github.com/humanjuan/acacia/v2	[no test files]
?   	github.com/humanjuan/acacia/v2/examples/basic	[no test files]
?   	github.com/humanjuan/acacia/v2/internal/logging	[no test files]
ok  	github.com/humanjuan/acacia/v2/test	16.253s
```

All 23 `Any` tests were individually approved:
* `TestPhase10AnyEventPrimitiveValuesValidJSON` - PASS
* `TestPhase10AnyEventNilValueEmitsNull` - PASS
* `TestPhase10AnyEventMapValueValidJSON` - PASS
* `TestPhase10AnyEventStructValueValidJSON` - PASS
* `TestPhase10AnyObjectValueValidJSON` - PASS
* `TestPhase10AnyObjectFirstFieldAndAfterExistingField` - PASS
* `TestPhase10AnySensitiveEventKeyRedactsEntirePayload` - PASS
* `TestPhase10AnySensitiveObjectKeyRedactsEntirePayload` - PASS
* `TestPhase10AnyBestEffortRedactsNestedMapKeys` - PASS
* `TestPhase10AnyBestEffortRedactsNestedStructJSONKeys` - PASS
* `TestPhase10AnyBestEffortRedactsSensitiveKeysInsideSlices` - PASS
* `TestPhase10AnyUnsupportedFunctionEmitsSafeError` - PASS
* `TestPhase10AnyUnsupportedChannelEmitsSafeError` - PASS
* `TestPhase10AnyCyclicMapEmitsSafeError` - PASS
* `TestPhase10AnyNonFiniteFloatEmitsSafeError` - PASS
* `TestPhase10AnyStringWithCRLFRemainsSingleLine` - PASS
* `TestPhase10AnyDoesNotExposeMsgOrSendOnArrayOrNamespace` - PASS
* `TestPhase10AnyFlatBuilderStillWorks` - PASS
* `TestPhase10AnyObjectBuilderStillWorks` - PASS
* `TestPhase10AnyArrayBuilderStillWorks` - PASS
* `TestPhase10AnyGroupNamespaceStillWork` - PASS
* `TestPhase10AnyRawJSONStillWorks` - PASS
* `TestPhase10AnyLegacyMapPathStillWorks`

### Race Detector (`go test -race ./...`)
The execution did not report any data races under the executed test suite.

---

## 5. Fuzzing Validation
The fuzz target `FuzzPhase10AnyValidityRedactionAndSingleLineSafety` was executed for 60 seconds, verifying stability and behavior without observing syntax corruptions or injected newlines under the executed suite:
```text
PASS
ok  	github.com/humanjuan/acacia/v2/test	62.655s
```

---

## 6. Summary of Diagnostic Benchmarks
Collected with 10 repetitions in the development environment on an external disk:

### Observed Performance Profile
| Test Case | Observed range (ns/op) | Memory Consumption (B/op) | Allocations (allocs/op) |
| :--- | :---: | :---: | :---: |
| **Any Primitive Value** | ~1880 - 6390 ns/op | ~2500 B/op | 10 allocs/op |
| **Any Simple Map** | ~3820 - 5070 ns/op | ~1630 B/op | 25 allocs/op |
| **Any Struct** | ~3650 - 4740 ns/op | ~1600 B/op | 23 allocs/op |
| **Any Object (Simple Map)** | ~3060 - 3360 ns/op | ~1500 B/op | 21 allocs/op |
| **Any Nested Map Redaction** | ~7240 - 10370 ns/op | ~2520 B/op | 45 allocs/op |
| **Any Slice of Objects** | ~4640 - 5980 ns/op | ~2180 B/op | 38 allocs/op |
| **Any Sensitive External Key** | ~470 - 670 ns/op | ~30 - 150 B/op | 1 alloc/op |
| **Any Unsupported Fallback** | ~800 - 1060 ns/op | ~57 - 99 B/op | 3 allocs/op |

*The allocations and latencies observed in the Any benchmarks confirm the slow-path behavior compared to typed structured builders, resulting from the encoding and decoding steps required for best-effort recursive redaction.*

---

## 7. Diagnostic Regression of Structured Builders
Cross-benchmark suites with 5 repetitions were run to verify that the changes did not introduce unexpected variations in already consolidated components:
* The functional suites for Flat, Object, Array, Group, Namespace, and RawJSON passed without issue.
* Benchmark results on the external development disk are recorded solely for diagnostic purposes and do not constitute absolute comparative performance conclusions.

---

## 8. Risks and Decisions
* **No SSD Hard Validation**: Metrics were collected on external development storage. The final characterization of release latencies and allocations requires testing on local internal SSD storage under controlled conditions.
* **Best-Effort Internal Redaction**: The recursive internal redaction process covers maps, structs serializable by json tags, and slices, but does not guarantee perfect redaction on complex types or serializers incompatible with `encoding/json`.
* **Exclusions**: `Array.Any` and `Namespace.Any` remain deferred.
