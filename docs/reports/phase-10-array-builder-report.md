# Phase 10 — Array Builder Report

## 1. Objetivo y Alcance
El objetivo principal del Sprint 10.3 es incorporar soporte tipado para colecciones (arrays) JSON en Acacia Logger, reutilizando el encoder de bajo nivel, la API de callbacks sin reflexión y las directrices de seguridad de la Fase 10.
Este sprint cubre:
* API basada en callback sin reflexión: `lg.InfoEvent().Array("items", func(a *acacia.Array) { ... })`.
* Métodos tipados primitivos: `Str`, `Int`, `Int64`, `Uint64`, `Bool`, `Float64`, `Time`, `Dur`, `Err`.
* Elementos Object dentro del array: `a.Object(func(o *acacia.Object) { ... })`.
* Manejo seguro de callbacks `nil` y vacíos, emitiendo `[]` en lugar de omitir o panicar.
* Rollback seguro en caso de pánico durante la ejecución de los callbacks.
* Inicialización segura (no-op) para structs `Array` zero-value.
* Redacción recursiva case-insensitive en campos sensibles dentro de elementos `Object` de arrays.
* Escape completo de strings y formato seguro para `float64` no finitos (`NaN`, `+Inf`, `-Inf`).
* Verificación bajo suite de pruebas unitarias, race detector, fuzzing y benchmarks diagnósticos.

---

## 2. Archivos Modificados y Creados
* **Modificado**:
  * acacia.go: Adición de la estructura `Array`, sus métodos tipados, y métodos `Array` en `Event` y `Object`.
  * docs/ACACIA_PROJECT_CONTEXT.md: Actualización de Next Action y recommended commit message.
  * docs/PHASE_10_STRUCTURED_JSON_API_CONTRACT.md: Actualización del contrato de Array Builder.
  * docs/adr/0004-structured-json-maturity.md: Inclusión de la sección Decision Update para el Array Builder.
  * docs/reports/README.md: Indexación del reporte.
* **Creado**:
  * test/phase10_array_builder_test.go: Suite de pruebas unitarias cubriendo correctitud de arrays primitivos y de objetos, pánicos, nil callbacks y redacción recursiva.
  * test/phase10_array_builder_fuzz_test.go: Fuzzing para validar la correctitud sintáctica JSON del array.
  * test/phase10_array_builder_benchmark_test.go: Suite de benchmarks diagnósticos.

---

## 3. Detalles de Implementación y APIs

### API Pública de Event y Object
```go
func (e *Event) Array(key string, fn func(*Array)) *Event
func (o *Object) Array(key string, fn func(*Array)) *Object
```

### Métodos del Array Builder
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

### Semántica de Rollback ante Panic
* **Panic en Event.Array / Object.Array**: Registra el offset antes de escribir el campo. En caso de pánico, trunca el búfer a ese offset, restaura el estado `first` del padre (en objetos) y añade `"_acacia_error": "array callback panic"`.
* **Panic en Array.Object**: Si la función del callback para un objeto individual dentro del array paniquea, se descarta únicamente ese objeto y se inserta en su lugar `{"_acacia_error":"object callback panic"}` dentro del array. Esto asegura la validez del JSON final sin descartar los demás elementos del array.

---

## 4. Evidencia de Pruebas de Correctitud

Se ejecutaron las suites de pruebas unitarias completas con éxito:

### Pruebas Unitarias Generales (`go test ./...`)
```text
?   	github.com/humanjuan/acacia/v2	[no test files]
?   	github.com/humanjuan/acacia/v2/examples/basic	[no test files]
?   	github.com/humanjuan/acacia/v2/internal/logging	[no test files]
ok  	github.com/humanjuan/acacia/v2/test	22.109s
```

### Pruebas Unitarias Enfocadas (`go test ./test -run 'TestPhase10ArrayBuilder'`)
Las 17 pruebas unitarias de correctitud pasaron limpias:
```text
ok  	github.com/humanjuan/acacia/v2/test	1.800s
```
Pruebas ejecutadas:
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
No se reportó ninguna condición de carrera bajo concurrencia:
```text
ok  	github.com/humanjuan/acacia/v2/test	41.056s
```

---

## 5. Validación bajo Fuzzing
El fuzz target `FuzzPhase10ArrayBuilderEscapingAndObjectRedaction` se ejecutó durante 60 segundos con 12 workers, completando más de 89,000 ejecuciones:
No se observaron fallos, fugas de mensajes crudos de panic ni JSON inválido bajo la suite ejecutada.
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

## 6. Benchmarks Diagnósticos Resumidos
Los benchmarks del Array builder se recopilaron con 10 repeticiones en el disco externo de desarrollo.

### Rangos de Rendimiento Observados
| Benchmark | Latencia (ns/op) | Memoria (B/op) | Alocaciones (allocs/op) |
| :--- | :---: | :---: | :---: |
| `BenchmarkPhase10ArrayBuilderPrimitiveSmall` | ~690 - 980 ns/op | ~45 - 152 B/op | 2 allocs/op |
| `BenchmarkPhase10ArrayBuilderPrimitiveTenItems` | ~670 - 880 ns/op | ~63 - 190 B/op | 2 allocs/op |
| `BenchmarkPhase10ArrayBuilderObjectElements` | ~960 - 1390 ns/op | ~82 - 203 B/op | 4 allocs/op |
| `BenchmarkPhase10ArrayBuilderNestedInObject` | ~790 - 1090 ns/op | ~67 - 173 B/op | 3 allocs/op |
| `BenchmarkPhase10ArrayBuilderEscapedString` | ~680 - 1220 ns/op | ~53 - 151 B/op | 2 allocs/op |
| `BenchmarkPhase10ArrayBuilderObjectRedaction` | ~710 - 1290 ns/op | ~71 - 219 B/op | 3 allocs/op |
| `BenchmarkPhase10ArrayBuilderFloat64` | ~720 - 1080 ns/op | ~43 - 131 B/op | 2 allocs/op |

* **Alocaciones**: Las alocaciones observadas probablemente están asociadas a closures/callbacks y escape analysis; la causa debe confirmarse con análisis específico si se decide optimizar este punto.

---

## 7. Regresión Diagnóstica Flat/Object
Se ejecutó regresión diagnóstica funcional y de rendimiento para Flat JSON y Object Builder:
* Las suites funcionales `TestPhase10FlatJSON` y `TestPhase10ObjectBuilder` pasaron de forma limpia.
* Los benchmarks diagnósticos en el disco externo mostraron rangos estables de ejecución (manteniendo `0 allocs/op` para loggers con niveles desactivados). Los resultados de latencia reflejaron la variabilidad ordinaria de desarrollo en disco externo.

---

## 8. Riesgos y Decisiones
* **No SSD Hard Validation**: Los benchmarks se han ejecutado en el disco externo de desarrollo. La validación en SSD interno controlado está diferida para la fase final.
* **Características Excluidas**: Se omitió la implementación de arrays aninados (arrays dentro de arrays), `RawJSON` y `Any` en arrays, así como `Group`, `Namespace` y `OpenTelemetry` para preservar el alcance específico de este sprint.
