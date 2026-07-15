# Phase 10 — Group and Namespace Report
*This is a historical engineering report. The filename is kept for audit continuity.*

## 1. Objective and Scope
The primary objective of Sprint 10.4 is to design and incorporate support for ergonomic APIs and structured grouping in Acacia Logger:
* **Group (Real Nested Structure)**: Groups fields using the existing `Object` type. It is compatible with the Object Builder policies of reflection-free callbacks, rollback on panics, and recursive redaction.
* **Namespace (Flat Keys with Prefix)**: Flattens fields with a delimiter (`"prefix.key"`). It defines a dedicated callback scope `*acacia.Namespace` free of event finalization methods (`Msg` or `Send`), preventing premature line closures.
* **Nil/Empty Callback Handling**: Calls with empty or `nil` callbacks are handled as safe no-ops that do not emit corrupt data.
* **Rollback on Panic**: A panic in namespace/group callbacks executes a full rollback of the event buffer or parent object to avoid JSON syntax corruption, and emits a safe `"_acacia_error"` indicator without leaking the raw panic message.
* **Security and Redaction**: Case-insensitive security redaction is performed on the prefix, the leaf key, individual segments separated by `.`, `_`, or `-`, or the normalized compound key.
* **Escaping and Encoding**: Both the prefix and final key are escaped with `escapeJSONInto` to mitigate log injection attacks.

Support for `RawJSON`, `Any`, or `OpenTelemetry` is not included in this phase.

---

## 2. Modified and Created Files
* **Modified**:
  * acacia.go: Implementation of the `Namespace` struct and its associated methods, addition of `Group` and `Namespace` methods to `Event` and `Object`, and compound key redaction helpers.
  * docs/ACACIA_PROJECT_CONTEXT.md: Update of the current phase status, next actions, and recommended commit.
  * docs/PHASE_10_STRUCTURED_JSON_API_CONTRACT.md: Documentation and refinement of the Group vs Namespace duality.
  * docs/adr/0004-structured-json-maturity.md: Inclusion of the technical decisions record for Sprint 10.4.
  * docs/reports/README.md: Registration of the link to the public report.
* **Created**:
  * test/phase10_group_namespace_test.go: Unit tests validating flow correctness, panics, segment redaction, escaping, and regressions.
  * test/phase10_group_namespace_fuzz_test.go: Fuzzing target `FuzzPhase10NamespaceEscapingAndRedaction` to test robustness against injection and panics.
  * test/phase10_group_namespace_benchmark_test.go: Diagnostic benchmark suite for Group and Namespace.

---

## 3. Implementation Details and APIs

### Group API
Implemented as a semantic alias of `Object` to cleanly reuse Acacia's Object Builder:
```go
func (e *Event) Group(key string, fn func(*Object)) *Event
func (o *Object) Group(key string, fn func(*Object)) *Object
```

### Namespace API
A dedicated `Namespace` structure is implemented with chained methods that do not expose sending mechanisms:
```go
type Namespace struct {
    buf     *[]byte
    prefix  string
    isEvent bool
    first   *bool
}

func (e *Event) Namespace(prefix string, fn func(*Namespace)) *Event
func (o *Object) Namespace(prefix string, fn func(*Namespace)) *Object

func (n *Namespace) Str(key, val string) *Namespace
func (n *Namespace) Int(key string, val int) *Namespace
func (n *Namespace) Int64(key string, val int64) *Namespace
func (n *Namespace) Uint64(key string, val uint64) *Namespace
func (n *Namespace) Bool(key string, val bool) *Namespace
func (n *Namespace) Float64(key string, val float64) *Namespace
func (n *Namespace) Dur(key string, val time.Duration) *Namespace
func (n *Namespace) Time(key string, val time.Time) *Namespace
func (n *Namespace) Err(err error) *Namespace
func (n *Namespace) ErrKey(key string, err error) *Namespace
func (n *Namespace) Secret(key, val string) *Namespace
func (n *Namespace) StrRedacted(key, val string) *Namespace
```

### Conservative Redaction Without Dynamic Compound Key Concatenation
To avoid string allocations during key composition when checking for redaction, two helpers were added:
1. `isSensitiveCompoundKey(prefix, key string) bool`: consecutively normalizes (simulating joining with `.`) and checks forbidden keys on a fixed stack buffer.
2. `hasSensitiveSegment(s string) bool`: iterates through the string looking for segments delimited by `.`, `_`, or `-` and validates them individually.

---

## 4. Correctness Testing Evidence
All test suites were executed successfully:

### General Unit Tests (`go test ./...`)
```text
?   	github.com/humanjuan/acacia/v2	[no test files]
?   	github.com/humanjuan/acacia/v2/examples/basic	[no test files]
?   	github.com/humanjuan/acacia/v2/internal/logging	[no test files]
ok  	github.com/humanjuan/acacia/v2/test	18.285s
```

### Focused Unit Tests (`go test -v ./test -run 'TestPhase10(Group|Namespace)'`)
All 26 tests of the Group and Namespace suite were approved:
* `TestPhase10GroupEventEmitsNestedObject` - PASS
* `TestPhase10GroupObjectEmitsNestedObject` - PASS
* `TestPhase10GroupNilCallbackEmitsEmptyObject` - PASS
* `TestPhase10GroupCallbackPanicDoesNotLeakRawPanic` - PASS
* `TestPhase10GroupObjectBuilderStillWorks` - PASS
* `TestPhase10NamespaceEventEmitsFlatPrefixedKeys` - PASS
* `TestPhase10NamespaceObjectEmitsFlatPrefixedKeys` - PASS
* `TestPhase10NamespaceNilCallbackIsNoOp` - PASS
* `TestPhase10NamespaceEmptyCallbackIsNoOp` - PASS
* `TestPhase10NamespaceEscapesPrefixAndKeys` - PASS
* `TestPhase10NamespacePrefixEmptyUsesRawKey` - PASS
* `TestPhase10NamespaceZeroValueIsNoOp` - PASS
* `TestPhase10NamespaceSensitiveLeafKeyIsRedacted` - PASS
* `TestPhase10NamespaceSensitivePrefixIsRedacted` - PASS
* `TestPhase10NamespaceSensitiveCompoundKeyIsRedacted` - PASS
* `TestPhase10NamespaceSecretAndStrRedacted` - PASS
* `TestPhase10NamespaceErrNilIsNoOp` - PASS
* `TestPhase10NamespaceErrKeyEmitsPrefixedError` - PASS
* `TestPhase10NamespaceFloat64NonFiniteStillValidJSON` - PASS
* `TestPhase10NamespaceEventCallbackPanicDoesNotCorruptJSON` - PASS
* `TestPhase10NamespaceObjectCallbackPanicAsFirstFieldDoesNotCorruptParent` - PASS
* `TestPhase10NamespaceObjectCallbackPanicAfterExistingFieldKeepsValidJSON` - PASS
* `TestPhase10NamespaceFlatBuilderStillWorks` - PASS
* `TestPhase10NamespaceObjectBuilderStillWorks` - PASS
* `TestPhase10NamespaceArrayBuilderStillWorks` - PASS
* `TestPhase10NamespaceLegacyMapPathStillWorks`

### Race Detector (`go test -race ./...`)
Execution under the race detector reported no data races under the executed suite.
```text
ok  	github.com/humanjuan/acacia/v2/test	40.961s
```

---

## 5. Fuzzing Validation
The fuzz target `FuzzPhase10NamespaceEscapingAndRedaction` was successfully executed for 60 seconds with 12 workers, completing iterations without unexpected panics and serving as diagnostic evidence to verify behavior against control character injections (CR/LF):
```text
PASS
ok  	github.com/humanjuan/acacia/v2/test	62.554s
```

---

## 6. Summary of Diagnostic Benchmarks
Collected with 10 repetitions on the external development disk:

### Observed Performance Ranges
| Benchmark | Latency (ns/op) | Memory (B/op) | Allocations (allocs/op) |
| :--- | :---: | :---: | :---: |
| `BenchmarkPhase10GroupEventSmall` | ~730 - 1330 ns/op | ~42 - 61 B/op | 2 allocs/op |
| `BenchmarkPhase10GroupObjectNested` | ~680 - 980 ns/op | ~57 - 88 B/op | 3 allocs/op |
| `BenchmarkPhase10NamespaceEventSmall` | ~850 - 930 ns/op | ~73 - 92 B/op | 2 allocs/op |
| `BenchmarkPhase10NamespaceEventTenFields` | ~2150 - 3620 ns/op | ~74 - 106 B/op | 2 allocs/op |
| `BenchmarkPhase10NamespaceObjectSmall` | ~760 - 1120 ns/op | ~90 - 187 B/op | 3 allocs/op |
| `BenchmarkPhase10NamespaceEscapedKeyValue` | ~760 - 1970 ns/op | ~73 - 91 B/op | 2 allocs/op |
| `BenchmarkPhase10NamespaceRedactedField` | ~600 - 1300 ns/op | ~77 - 116 B/op | 2 allocs/op |
| `BenchmarkPhase10NamespaceFloat64` | ~1010 - 1370 ns/op | ~74 - 96 B/op | 2 allocs/op |

*The observed allocations are likely associated with closures/callbacks and escape analysis; the cause should be confirmed with specific analysis if optimization is pursued.*

---

## 7. Diagnostic Regression for Flat/Object/Array
Functional and performance regression tests were run to check the behavior of the remaining structured builders:
* All unit and integration tests for Flat JSON, the Object Builder, and the Array Builder passed successfully.
* Diagnostic regression for Flat/Object/Array was executed. The functional suites passed; the benchmarks on the external disk are maintained as diagnostic evidence and are not used as a final performance conclusion.

---

## 8. Risks and Decisions
* **No SSD Hard Validation**: Benchmark data comes from the diagnostic development environment on an external disk. Rigorous final release validation will be performed on an internal SSD.
* **Mandatory Exclusions**: Excluded from the scope: `RawJSON` in arrays/namespaces, dynamic `Any`, and `OpenTelemetry` configurations.
