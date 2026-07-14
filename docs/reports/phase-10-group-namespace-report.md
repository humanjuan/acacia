# Phase 10 — Group and Namespace Report

## 1. Objetivo y Alcance
El objetivo principal del Sprint 10.4 es diseñar e incorporar soporte para APIs de ergonomía y agrupamiento estructurado en Acacia Logger:
* **Group (Estructura Anidada Real)**: Agrupa campos utilizando el tipo `Object` existente. Es compatible con las políticas del Object Builder de callbacks sin reflexión, rollback ante pánicos y redacción recursiva.
* **Namespace (Claves Planas con Prefijo)**: Aplana los campos con un delimitador (`"prefix.key"`). Define un scope de callbacks dedicado `*acacia.Namespace` libre de métodos de finalización de evento (`Msg` o `Send`), lo que previene cierres prematuros de línea.
* **Manejo de Callback Nil/Vacío**: Las llamadas a callbacks vacíos o `nil` se manejan como no-ops seguros que no emiten datos corruptos.
* **Rollback ante Panic**: El pánico en callbacks de namespace/group ejecuta un rollback completo del búfer del evento o del objeto padre para evitar corrupciones de sintaxis JSON, y emite un indicador seguro `"_acacia_error"` sin fugar el mensaje crudo del pánico.
* **Seguridad y Redacción**: Se realiza redacción de seguridad case-insensitive sobre el prefijo, la clave de la hoja, los segmentos individuales separados por `.`, `_`, o `-`, o la clave compuesta normalizada.
* **Escapado y Codificación**: Tanto el prefijo como la clave final se escapan con `escapeJSONInto` para mitigar ataques de log injection.

No se incluye en esta fase el soporte para `RawJSON`, `Any` o `OpenTelemetry`.

---

## 2. Archivos Modificados y Creados
* **Modificado**:
  * acacia.go: Implementación del struct `Namespace` y sus métodos asociados, adición de los métodos `Group` y `Namespace` a `Event` e `Object`, y helpers de redacción de clave compuesta.
  * docs/ACACIA_PROJECT_CONTEXT.md: Actualización del estado actual de la fase, next actions y commit recomendado.
  * docs/PHASE_10_STRUCTURED_JSON_API_CONTRACT.md: Documentación y refinamiento de la dualidad de Group vs Namespace.
  * docs/adr/0004-structured-json-maturity.md: Inclusión del registro de decisiones técnicas del Sprint 10.4.
  * docs/reports/README.md: Registro del enlace al reporte público.
* **Creado**:
  * test/phase10_group_namespace_test.go: Pruebas unitarias que validan correctitud de flujos, pánicos, redacción en segmentos, escapado y regresiones.
  * test/phase10_group_namespace_fuzz_test.go: Fuzzing target `FuzzPhase10NamespaceEscapingAndRedaction` para probar robustez contra inyección y pánicos.
  * test/phase10_group_namespace_benchmark_test.go: Suite de benchmarks diagnósticos para Group y Namespace.

---

## 3. Detalles de Implementación y APIs

### API de Group
Implementado como un alias semántico de `Object` para reutilizar de forma limpia el Object Builder de Acacia:
```go
func (e *Event) Group(key string, fn func(*Object)) *Event
func (o *Object) Group(key string, fn func(*Object)) *Object
```

### API de Namespace
Se implementa una estructura dedicada `Namespace` con métodos encadenados que no exponen mecanismos de envío:
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

### Redacción conservadora sin concatenación dinámica de clave compuesta
Para evitar la alocación de strings durante la composición de la clave al verificar la redacción, se agregaron dos helpers:
1. `isSensitiveCompoundKey(prefix, key string) bool`: normaliza de forma consecutiva (simulando la unión con `.`) y verifica las claves prohibidas sobre un búfer de pila fijo.
2. `hasSensitiveSegment(s string) bool`: itera el string buscando segmentos delimitados por `.`, `_` o `-` y los valida individualmente.

---

## 4. Evidencia de Pruebas de Correctitud
Se ejecutaron con éxito todas las suites de pruebas:

### Pruebas Unitarias Generales (`go test ./...`)
```text
?   	github.com/humanjuan/acacia/v2	[no test files]
?   	github.com/humanjuan/acacia/v2/examples/basic	[no test files]
?   	github.com/humanjuan/acacia/v2/internal/logging	[no test files]
ok  	github.com/humanjuan/acacia/v2/test	18.285s
```

### Pruebas Unitarias Enfocadas (`go test -v ./test -run 'TestPhase10(Group|Namespace)'`)
Se aprobaron las 26 pruebas de la suite de Group y Namespace:
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
La ejecución bajo race detector no reportó data races bajo la suite ejecutada.
```text
ok  	github.com/humanjuan/acacia/v2/test	40.961s
```

---

## 5. Validación bajo Fuzzing
El fuzz target `FuzzPhase10NamespaceEscapingAndRedaction` se ejecutó durante 60 segundos con 12 workers de forma exitosa, logrando completar las iteraciones sin pánicos imprevistos y sirviendo como evidencia diagnóstica para verificar el comportamiento ante inyecciones de control (CR/LF):
```text
PASS
ok  	github.com/humanjuan/acacia/v2/test	62.554s
```

---

## 6. Benchmarks Diagnósticos Resumidos
Recopilados con 10 repeticiones en el disco de desarrollo externo:

### Rangos de Rendimiento Observados
| Benchmark | Latencia (ns/op) | Memoria (B/op) | Alocaciones (allocs/op) |
| :--- | :---: | :---: | :---: |
| `BenchmarkPhase10GroupEventSmall` | ~730 - 1330 ns/op | ~42 - 61 B/op | 2 allocs/op |
| `BenchmarkPhase10GroupObjectNested` | ~680 - 980 ns/op | ~57 - 88 B/op | 3 allocs/op |
| `BenchmarkPhase10NamespaceEventSmall` | ~850 - 930 ns/op | ~73 - 92 B/op | 2 allocs/op |
| `BenchmarkPhase10NamespaceEventTenFields` | ~2150 - 3620 ns/op | ~74 - 106 B/op | 2 allocs/op |
| `BenchmarkPhase10NamespaceObjectSmall` | ~760 - 1120 ns/op | ~90 - 187 B/op | 3 allocs/op |
| `BenchmarkPhase10NamespaceEscapedKeyValue` | ~760 - 1970 ns/op | ~73 - 91 B/op | 2 allocs/op |
| `BenchmarkPhase10NamespaceRedactedField` | ~600 - 1300 ns/op | ~77 - 116 B/op | 2 allocs/op |
| `BenchmarkPhase10NamespaceFloat64` | ~1010 - 1370 ns/op | ~74 - 96 B/op | 2 allocs/op |

*Las alocaciones observadas probablemente están asociadas a closures/callbacks y escape analysis; la causa debe confirmarse con análisis específico si se decide optimizar este punto.*

---

## 7. Regresión Diagnóstica Flat/Object/Array
Se ejecutaron pruebas de regresión funcionales y de rendimiento para comprobar el comportamiento del resto de builders estructurados:
* Todas las pruebas unitarias e integrales para Flat JSON, Object Builder y Array Builder pasaron de manera correcta.
* Se ejecutó regresión diagnóstica de Flat/Object/Array. Las suites funcionales pasaron; los benchmarks en disco externo se mantienen como evidencia diagnóstica y no se usan como conclusión final de rendimiento.

---

## 8. Riesgos y Decisiones
* **No SSD Hard Validation**: Los datos de benchmark provienen del ambiente diagnóstico de desarrollo en disco externo. La validación rigurosa final de release se realizará sobre SSD interno.
* **Exclusiones Mandatorias**: Quedan excluidos del alcance: `RawJSON` en arrays/namespaces, `Any` dinámico y configuraciones de `OpenTelemetry`.
