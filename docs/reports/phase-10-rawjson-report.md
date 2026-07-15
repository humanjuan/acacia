# Phase 10 — RawJSON Report

*This is a historical engineering report. The filename is kept for audit continuity.*

## 1. Objective and Scope
The primary objective of Sprint 10.5 is to design and incorporate support for direct insertion of pre-serialized JSON payloads as fields within the structured schema of Acacia Logger:
* **Event.RawJSON**: Method in the root event builder to securely attach valid or escaped JSON.
* **Object.RawJSON**: Method to inject structured JSON inside builder sub-objects.
* **Payload Compaction**: Mitigation of injections via compaction of line breaks (`\n` or `\r`) in valid JSON payloads.
* **Safe Handling of Invalid Payloads**: Automatic fallback that serializes corrupt or invalid payloads as an escaped JSON string.
* **Nil/Empty Safety**: `nil` or empty `[]byte{}` payloads are handled safely as empty strings (`""`), preventing runtime panics.
* **External Key Redaction**: If the key name is sensitive, the entire payload is replaced with `"[REDACTED]"`.
* **No Internal Redaction**: Attached valid payloads are not internally inspected for sensitive keys (contractually defined as **trusted input only**).

This phase does not include support for `Array.RawJSON`, `Namespace.RawJSON`, `Any`, or `OpenTelemetry` integrations.

---

## 2. Modified and Created Files
* **Modified**:
  * `acacia.go`: Imported `encoding/json`, added `RawJSON` methods and encoding/validation helpers.
  * `docs/ACACIA_PROJECT_CONTEXT.md`: Updated project status for Sprint 10.5.
  * `docs/PHASE_10_STRUCTURED_JSON_API_CONTRACT.md`: Specified RawJSON contract behavior.
  * `docs/adr/0004-structured-json-maturity.md`: Included technical decision log for Sprint 10.5.
  * `docs/reports/README.md`: Index of the new report.
* **Created**:
  * `test/phase10_rawjson_test.go`: Suite of 19 correctness tests.
  * `test/phase10_rawjson_fuzz_test.go`: Fuzz target to validate robustness, formatting, and escaping.
  * `test/phase10_rawjson_benchmark_test.go`: Diagnostic benchmarks for RawJSON.

---

## 3. Implementation Details and APIs

### RawJSON API
Added signatures to the main structs:
```go
func (e *Event) RawJSON(key string, raw []byte) *Event
func (o *Object) RawJSON(key string, raw []byte) *Object
```

### Secure Encoding Flow
1. The payload is validated using `json.Valid(raw)`.
2. If the payload is valid:
   - It checks if it contains line break control bytes (`\n` or `\r`).
   - If it does, `json.Compact` is executed on a temporary buffer to transform it into a single line.
   - If it is a flat payload, it is copied directly to the active log buffer using a controlled capacity reservation.
3. If the payload is invalid, it delegates to the safe formatter `appendEscapedRawJSONString`, which escapes special characters and wraps the value as a regular string.

---

## 4. Correctness Testing Evidence

### Unit Tests (`go test ./...`)
Tests corresponding to Sprint 10.5 and earlier passed successfully:
```text
?   	github.com/humanjuan/acacia/v2	[no test files]
?   	github.com/humanjuan/acacia/v2/examples/basic	[no test files]
?   	github.com/humanjuan/acacia/v2/internal/logging	[no test files]
ok  	github.com/humanjuan/acacia/v2/test	16.673s
```

All 19 specific tests for `RawJSON` were successfully passed:
* `TestPhase10RawJSONEventObjectPayloadValidJSON` - PASS
* `TestPhase10RawJSONEventArrayPayloadValidJSON` - PASS
* `TestPhase10RawJSONEventStringPayloadValidJSON` - PASS
* `TestPhase10RawJSONEventNumberBoolNullPayloadValidJSON` - PASS
* `TestPhase10RawJSONObjectPayloadValidJSON` - PASS
* `TestPhase10RawJSONObjectFirstFieldAndAfterExistingField` - PASS
* `TestPhase10RawJSONInvalidPayloadDegradesToEscapedString` - PASS
* `TestPhase10RawJSONNilAndEmptyPayloadDegradeToEmptyString` - PASS
* `TestPhase10RawJSONInvalidUTF8DegradesSafely` - PASS
* `TestPhase10RawJSONSensitiveEventKeyRedactsEntirePayload` - PASS
* `TestPhase10RawJSONSensitiveObjectKeyRedactsEntirePayload` - PASS
* `TestPhase10RawJSONTrustedInputDoesNotRedactNestedPayload` - PASS
* `TestPhase10RawJSONMultilineValidPayloadIsCompactedToSingleLine` - PASS
* `TestPhase10RawJSONEscapesKeys` - PASS
* `TestPhase10RawJSONFlatBuilderStillWorks` - PASS
* `TestPhase10RawJSONObjectBuilderStillWorks` - PASS
* `TestPhase10RawJSONArrayBuilderStillWorks` - PASS
* `TestPhase10RawJSONGroupNamespaceStillWork` - PASS
* `TestPhase10RawJSONLegacyMapPathStillWorks` - PASS

### Race Detector (`go test -race ./...`)
The execution did not report any data races under the executed test suite.

---

## 5. Fuzzing Validation
The fuzz target `FuzzPhase10RawJSONValidityAndSingleLineSafety` was executed successfully for 60 seconds, checking structural stability against altered payloads without observing any syntax corruption or injected line breaks under the executed suite:
```text
PASS
ok  	github.com/humanjuan/acacia/v2/test	62.542s
```

---

## 6. Diagnostic Benchmark Summary
Collected with 10 repetitions in the development environment on an external disk:

### Observed Performance Profile
| Caso de Prueba | Rango observado (ns/op) | Consumo de Memoria (B/op) | Alocaciones (allocs/op) |
| :--- | :---: | :---: | :---: |
| **Fast Path Válido Plano (Objeto/Array/Scalar)** | ~610 - 990 ns/op | ~25 - 47 B/op | 1 alloc/op |
| **Object Interno RawJSON** | ~670 - 1030 ns/op | ~42 - 53 B/op | 2 allocs/op |
| **Fallback Inválido** | ~640 - 760 ns/op | ~50 - 90 B/op | 2 allocs/op |
| **Redacción por Key Externa** | ~510 - 710 ns/op | ~29 - 93 B/op | 1 alloc/op |
| **Compacción Multiline** | ~900 - 1410 ns/op | ~89 - 92 B/op | 2 allocs/op |

*The allocations observed in the flat fast path correspond to the internal buffer instantiation or to closures and Go escape analysis; the exact cause should be verified with dedicated profiling if optimization is required in a production environment.*

---

## 7. Diagnostic Regression of Structured Builders
Cross-benchmark suites were executed with 5 repetitions to verify that the changes did not introduce unexpected variations in the already consolidated components:
* The Flat, Object, Array, Group, and Namespace suites passed without functional issues.
* Benchmark results on the external development disk are recorded solely for diagnostic purposes and do not constitute absolute comparative performance conclusions.

---

## 8. Risks and Decisions
* **No SSD Hard Validation**: Metrics were collected on external development storage. Final characterization of release latencies and allocations requires testing on local internal SSD storage under controlled conditions.
* **Trusted Input Only**: The contract assumes that payloads are pre-sanitized for internal redaction. If recursive inspection and modification of nested keys in the payload are required, the client should use the standard `Object` interface or standard builders.
* **Exclusions**: `Array.RawJSON`, `Namespace.RawJSON`, and the `Any` fallback remain deferred.
