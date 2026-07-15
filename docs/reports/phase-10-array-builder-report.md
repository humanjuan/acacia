# Phase 10 — Array Builder Report
*This is a historical engineering report. The filename is kept for audit continuity.*

## 1. Objective and Scope
The primary objective of Sprint 10.3 is to incorporate typed support for JSON collections (arrays) in Acacia Logger, reusing the low-level encoder, the reflection-free callback API, and the Phase 10 security guidelines.
This sprint covers:
* Reflection-free callback-based API: `lg.InfoEvent().Array("items", func(a *acacia.Array) { ... })`.
* Typed primitive methods: `Str`, `Int`, `Int64`, `Uint64`, `Bool`, `Float64`, `Time`, `Dur`, `Err`.
* Object elements inside the array: `a.Object(func(o *acacia.Object) { ... })`.
* Safe handling of `nil` and empty callbacks, emitting `[]` instead of omitting or panicking.
* Safe rollback in case of panic during callback execution.
* Safe initialization (no-op) for zero-value `Array` structs.
* Case-insensitive recursive redaction on sensitive fields inside array `Object` elements.
* Complete string escaping and safe formatting for non-finite `float64` values (`NaN`, `+Inf`, `-Inf`).
* Verification under unit test suite, race detector, fuzzing, and diagnostic benchmarks.

Support for `Array.Any`, `Namespace.Any`, or `OpenTelemetry` integrations is not included in this phase.

---

## 2. Modified and Created Files
* **Modified**:
  * acacia.go: Addition of the `Array` structure, its typed methods, and `Array` methods in `Event` and `Object`.
  * docs/ACACIA_PROJECT_CONTEXT.md: Update of Next Action and recommended commit message.
  * docs/PHASE_10_STRUCTURED_JSON_API_CONTRACT.md: Update of the Array Builder contract.
  * docs/adr/0004-structured-json-maturity.md: Inclusion of the Decision Update section for the Array Builder.
  * docs/reports/README.md: Indexing of the report.
* **Created**:
  * test/phase10_array_builder_test.go: Unit test suite covering correctness of primitive and object arrays, panics, nil callbacks, and recursive redaction.
  * test/phase10_array_builder_fuzz_test.go: Fuzzing to validate array JSON syntactic correctness.
  * test/phase10_array_builder_benchmark_test.go: Diagnostic benchmark suite.

---

## 3. Implementation Details and APIs

### Public API of Event and Object
```go
func (e *Event) Array(key string, fn func(*Array)) *Event
func (o *Object) Array(key string, fn func(*Array)) *Object
```

### Array Builder Methods
```go
func (a *Array) Str(val string) *Array
func (a *Array) Int(val int) *Array
func (a *Array) Int64(val int64) *Array
func (a *Array) Uint64(val uint64) *Array
func (a *Array) Bool(val bool) *Array
func (a *Array) Float64(val float64) *Array
func (a *Array) Dur(val time.Duration) *Array
func (a *Array) Time(val time.Time) *Array
func (a *Array) Err(err error) *Array
func (a *Array) Object(fn func(*Object)) *Array
```

### Rollback Semantics upon Panic
* **Panic in Event.Array / Object.Array**: Records the offset before writing the field. In case of panic, it truncates the buffer to that offset, restores the parent's `first` state (in objects), and appends `"_acacia_error": "array callback panic"`.
* **Panic in Array.Object**: If the callback function for an individual object inside the array panics, only that object is discarded and `{"_acacia_error":"object callback panic"}` is inserted in its place inside the array. This ensures the validity of the final JSON without discarding the other array elements.

---

## 4. Correctness Testing Evidence

The complete unit test suites were executed successfully:

### General Unit Tests (`go test ./...`)
```text
?   	github.com/humanjuan/acacia/v2	[no test files]
?   	github.com/humanjuan/acacia/v2/examples/basic	[no test files]
?   	github.com/humanjuan/acacia/v2/internal/logging	[no test files]
ok  	github.com/humanjuan/acacia/v2/test	22.109s
```

### Focused Unit Tests (`go test ./test -run 'TestPhase10ArrayBuilder'`)
The 17 correctness unit tests passed clean:
```text
ok  	github.com/humanjuan/acacia/v2/test	1.800s
```
Executed tests:
* `TestPhase10ArrayBuilderPrimitiveArrayValidJSON`
* `TestPhase10ArrayBuilderEmptyCallbackEmitsEmptyArray`
* `TestPhase10ArrayBuilderNilCallbackEmitsEmptyArray`
* `TestPhase10ArrayBuilderArrayInsideObjectValidJSON`
* `TestPhase10ArrayBuilderObjectElementsValidJSON`
* `TestPhase10ArrayBuilderEscapesStringValues`
* `TestPhase10ArrayBuilderFloat64NonFiniteStillValidJSON`
* `TestPhase10ArrayBuilderErrNilIsNoOp`
* `TestPhase10ArrayBuilderErrValueIsString`
* `TestPhase10ArrayBuilderObjectElementRedactsSensitiveKeys`
* `TestPhase10ArrayBuilderEventArrayCallbackPanicDoesNotCorruptJSON`
* `TestPhase10ArrayBuilderObjectArrayCallbackPanicAsFirstFieldDoesNotCorruptParent`
* `TestPhase10ArrayBuilderObjectArrayCallbackPanicAfterExistingFieldKeepsValidJSON`
* `TestPhase10ArrayBuilderObjectElementPanicDoesNotLeakRawPanic`
* `TestPhase10ArrayBuilderZeroValueIsNoOp`
* `TestPhase10ArrayBuilderObjectBuilderStillWorks`
* `TestPhase10ArrayBuilderLegacyMapPathStillWorks`

### Race Detector (`go test -race ./...`)
No race condition was reported under concurrency:
```text
ok  	github.com/humanjuan/acacia/v2/test	41.056s
```

---

## 5. Fuzzing Validation
The fuzz target `FuzzPhase10ArrayBuilderEscapingAndObjectRedaction` was executed for 60 seconds with 12 workers, completing more than 89,000 executions:
No failures, leaks of raw panic messages, or invalid JSON were observed under the executed suite.
```text
fuzz: elapsed: 0s, gathering baseline coverage: 0/83 completed
fuzz: elapsed: 0s, gathering baseline coverage: 83/83 completed, now fuzzing with 12 workers
fuzz: elapsed: 3s, execs: 29378 (9792/sec), new interesting: 13 (total: 96)
...
fuzz: elapsed: 1m1s, execs: 89571 (0/sec), new interesting: 27 (total: 110)
PASS
ok  	github.com/humanjuan/acacia/v2/test	61.569s
```

---

## 6. Summary of Diagnostic Benchmarks
The Array builder benchmarks were collected with 10 repetitions on the external development disk.

### Observed Performance Ranges
| Benchmark | Latency (ns/op) | Memory (B/op) | Allocations (allocs/op) |
| :--- | :---: | :---: | :---: |
| `BenchmarkPhase10ArrayBuilderPrimitiveSmall` | ~690 - 980 ns/op | ~45 - 152 B/op | 2 allocs/op |
| `BenchmarkPhase10ArrayBuilderPrimitiveTenItems` | ~670 - 880 ns/op | ~63 - 190 B/op | 2 allocs/op |
| `BenchmarkPhase10ArrayBuilderObjectElements` | ~960 - 1390 ns/op | ~82 - 203 B/op | 4 allocs/op |
| `BenchmarkPhase10ArrayBuilderNestedInObject` | ~790 - 1090 ns/op | ~67 - 173 B/op | 3 allocs/op |
| `BenchmarkPhase10ArrayBuilderEscapedString` | ~680 - 1220 ns/op | ~53 - 151 B/op | 2 allocs/op |
| `BenchmarkPhase10ArrayBuilderObjectRedaction` | ~710 - 1290 ns/op | ~71 - 219 B/op | 3 allocs/op |
| `BenchmarkPhase10ArrayBuilderFloat64` | ~720 - 1080 ns/op | ~43 - 131 B/op | 2 allocs/op |

* **Allocations**: The observed allocations are likely associated with closures/callbacks and escape analysis; the cause should be confirmed with specific analysis if optimization is pursued.

---

## 7. Diagnostic Regression for Flat/Object
Functional and performance regression testing was run for Flat JSON and the Object Builder:
* The functional suites `TestPhase10FlatJSON` and `TestPhase10ObjectBuilder` passed cleanly.
* Diagnostic benchmarks on the external disk showed stable execution ranges (maintaining `0 allocs/op` for loggers with disabled levels). Latency results reflected typical development variability on external disks.

---

## 8. Risks and Decisions
* **No SSD Hard Validation**: Benchmarks were run on the external development disk. Validation on controlled internal SSD is deferred to the final phase.
* **Excluded Features**: Nested arrays (arrays within arrays), `RawJSON` and `Any` in arrays, as well as `Group`, `Namespace`, and `OpenTelemetry` were omitted to preserve the specific scope of this sprint.
