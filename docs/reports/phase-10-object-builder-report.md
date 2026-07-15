# Phase 10 — Object Builder Report
*This is a historical engineering report. The filename is kept for audit continuity.*

## 1. Objective and Scope
The primary objective of Sprint 10.2 is to incorporate typed support for nested JSON objects using callbacks in Acacia Logger, maintaining the high-performance, reflection-free identity in the recommended paths, and preserving compatibility with v2.x.
This sprint includes:
* Reflection-free callback-based API: `lg.InfoEvent().Object("key", func(o *acacia.Object) { ... })`.
* Safe handling of `nil` callbacks emitting an empty object (`{}`).
* Rollback of partial buffer on callback panics, preventing corrupt JSON outputs and emitting a constant descriptive field `"_acacia_error": "object callback panic"`.
* Case-insensitive recursive redaction of sensitive fields at any nesting level.
* Complete character escaping in nested keys and values.
* Treatment of non-finite `float64` values (`NaN`, `+Inf`, `-Inf`) as valid JSON strings inside objects.
* Correct handling of `Err(nil)` (omitted field) and `ErrKey`.
* Complete unit testing, fuzzing, and diagnostic benchmarks.

---

## 2. Modified and Created Files
* **Modified**:
  * [acacia.go](../../acacia.go): Implementation of the `Object` structure and its typed methods, and addition of the `Object` method in the `Event` structure.
  * [docs/reports/README.md](README.md): Indexing of the new sprint report.
  * [docs/ACACIA_PROJECT_CONTEXT.md](../ACACIA_PROJECT_CONTEXT.md): Update of Next Action and recommended commit message.
  * [docs/adr/0004-structured-json-maturity.md](../adr/0004-structured-json-maturity.md): Inclusion of the Decision Update section for the Object Builder.
* **Created**:
  * [test/phase10_object_builder_test.go](../../test/phase10_object_builder_test.go): Unit test suite verifying nesting correctness, panics, nil-callbacks, escaping, and recursive redaction.
  * [test/phase10_object_builder_fuzz_test.go](../../test/phase10_object_builder_fuzz_test.go): Fuzz target to validate JSON syntactic integrity under random and permuted inputs.
  * [test/phase10_object_builder_benchmark_test.go](../../test/phase10_object_builder_benchmark_test.go): Diagnostic benchmark suite.

---

## 3. Implementation Details and v2.x Compatibility
* **Scoped Design**: The `Object` structure operates directly on the event buffer using a slice pointer (`*[]byte`). This avoids additional allocations of intermediate buffers during object serialization.
* **Rollback on Panic**: Before executing the callback function, the write offset in the buffer is recorded. If the callback execution panics, the buffer is truncated (rolled back) to that offset, and the safe constant field `"_acacia_error": "object callback panic"` is appended at the parent's level, ensuring the output JSON is not left truncated or corrupt.
* **Recursive Redaction**: In all `Object` methods, `isSensitiveKey` is applied before serializing, ensuring that sensitive keys are redacted recursively at any depth.
* **Safe Float64**: The semantics implemented in the previous sprint are inherited, serializing normal floats as literal JSON numbers, and non-finite ones as safe strings.
* **Compatibility**: All public signatures of v2.x are preserved. The legacy path based on dynamic maps continues to function exactly as before.

---

## 4. Evidence of Correctness and Concurrency Testing
The complete unit test suite was executed with the new tests included:

### General Unit Tests (`go test ./...`)
```text
?   	github.com/humanjuan/acacia/v2	[no test files]
?   	github.com/humanjuan/acacia/v2/examples/basic	[no test files]
?   	github.com/humanjuan/acacia/v2/internal/logging	[no test files]
ok  	github.com/humanjuan/acacia/v2/test	19.618s
```

### Focused Unit Tests (`go test ./test -run 'TestPhase10ObjectBuilder'`)
All behavior correctness tests for Object passed successfully:
```text
ok  	github.com/humanjuan/acacia/v2/test	1.479s
```
Object suite tests executed:
* `TestPhase10ObjectBuilderNestedObjectValidJSON`
* `TestPhase10ObjectBuilderEmptyCallbackEmitsEmptyObject`
* `TestPhase10ObjectBuilderNilCallbackEmitsEmptyObject`
* `TestPhase10ObjectBuilderEscapesKeysAndValues`
* `TestPhase10ObjectBuilderDuplicateKeysAreEmitted`
* `TestPhase10ObjectBuilderRecursiveSensitiveKeysAreRedacted`
* `TestPhase10ObjectBuilderErrNilIsOmitted`
* `TestPhase10ObjectBuilderErrKeyEscapedAndRedacted`
* `TestPhase10ObjectBuilderFloat64NonFiniteStillValidJSON`
* `TestPhase10ObjectBuilderCallbackPanicDoesNotCorruptJSON`
* `TestPhase10ObjectBuilderDeepNestingFiveLevels`
* `TestPhase10ObjectBuilderLegacyMapPathStillWorks`
* `TestPhase10ObjectBuilderZeroValueIsNoOp`
* `TestPhase10ObjectBuilderNestedCallbackPanicAsFirstFieldDoesNotCorruptParent`
* `TestPhase10ObjectBuilderNestedCallbackPanicAfterExistingFieldKeepsValidJSON`

### Race Detector (`go test -race ./...`)
No data races were reported under the executed suite:
```text
ok  	github.com/humanjuan/acacia/v2/test	55.912s
```

---

## 5. Fuzzing Validation
The fuzz target `FuzzPhase10ObjectBuilderEscapingAndRedaction` was executed for 60 seconds, completing more than 223,000 iterations without failures or unrecovered panics:
```text
fuzz: elapsed: 0s, gathering baseline coverage: 0/89 completed
fuzz: elapsed: 0s, gathering baseline coverage: 89/89 completed, now fuzzing with 12 workers
fuzz: elapsed: 3s, execs: 7351 (2449/sec), new interesting: 3 (total: 92)
...
fuzz: elapsed: 1m1s, execs: 223011 (0/sec), new interesting: 9 (total: 98)
PASS
ok  	github.com/humanjuan/acacia/v2/test	61.834s
```
**Result**: No corruptions in the JSON outputs and no raw newlines within the serialized keys or values.

---

## 6. Summary of Diagnostic Benchmarks
The Object builder benchmarks were collected on the external development disk with 10 repetitions.

### Observed Ranges (Diagnostic Benchmarks)
| Benchmark | Latency (ns/op) | Memory (B/op) | Allocations (allocs/op) |
| :--- | :---: | :---: | :---: |
| `BenchmarkPhase10ObjectBuilderSmall` | ~880 - 1360 ns/op | ~42 - 161 B/op | 2 allocs/op |
| `BenchmarkPhase10ObjectBuilderNested` | ~1020 - 1360 ns/op | ~60 - 201 B/op | 3 allocs/op |
| `BenchmarkPhase10ObjectBuilderFiveLevels` | ~1230 - 2860 ns/op | ~110 - 285 B/op | 6 allocs/op |
| `BenchmarkPhase10ObjectBuilderEscapedString` | ~950 - 2650 ns/op | ~44 - 110 B/op | 2 allocs/op |
| `BenchmarkPhase10ObjectBuilderRedactedField` | ~850 - 1620 ns/op | ~49 - 134 B/op | 2 allocs/op |
| `BenchmarkPhase10ObjectBuilderFloat64` | ~1140 - 1900 ns/op | ~44 - 124 B/op | 2 allocs/op |

### Performance and Allocation Analysis
* **Allocations**: The diagnostic benchmarks observe an increase in allocs/op per nesting level; the probable cause should be confirmed with escape analysis if optimization is pursued.
* **Append-Only Write**: The design preserves direct writing to the final event buffer, avoiding the allocation and copying of secondary buffers.

---

## 7. Diagnostic Regression for Flat JSON
Flat JSON regression was executed. The functional suite passed; benchmarks on the external disk showed variability and are thus not used as a final performance conclusion.

---

## 8. Sprint Confirmations
* **No SSD Hard Validation**: All benchmarks and diagnostic tests were executed on the external development disk. Release validation on controlled SSD is deferred to the end of Phase 10.
* **Not Implemented**: Implementation of the `Array Builder`, `Group`, `Namespace`, `RawJSON`, `Any`, and `OpenTelemetry` support was explicitly omitted to comply strictly with the boundaries of this sprint.

---

*The complete raw results were saved in the private directory for internal auditing.*
