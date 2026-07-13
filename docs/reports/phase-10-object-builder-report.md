# Phase 10 — Object Builder Report

## 1. Objetivo y Alcance
El objetivo principal del Sprint 10.2 es incorporar soporte tipado para objetos JSON anizados mediante callbacks en Acacia Logger, manteniendo la identidad de alto rendimiento, libre de reflexión en los caminos recomendados y preservando la compatibilidad con v2.x.
Este sprint incluye:
* API basada en callback sin reflexión: `lg.InfoEvent().Object("key", func(o *acacia.Object) { ... })`.
* Manejo seguro de callbacks `nil` emitiendo un objeto vacío (`{}`).
* Rollback del búfer parcial ante pánicos en el callback, evitando salidas JSON corruptas y emitiendo un campo descriptivo constante `"_acacia_error": "object callback panic"`.
* Redacción recursiva case-insensitive de campos sensibles en cualquier nivel de anidación.
* Escapado completo de caracteres en claves y valores anizados.
* Tratamiento de `float64` no finitos (`NaN`, `+Inf`, `-Inf`) como strings JSON válidos dentro de objetos.
* Manejo correcto de `Err(nil)` (campo omitido) y `ErrKey`.
* Pruebas unitarias completas, fuzzing y benchmarks diagnósticos.

---

## 2. Archivos Modificados y Creados
* **Modificado**:
  * [acacia.go](../../acacia.go): Implementación de la estructura `Object` y sus métodos tipados, y adición del método `Object` en la estructura `Event`.
  * [docs/reports/README.md](README.md): Indexación del nuevo reporte de sprint.
  * [docs/ACACIA_PROJECT_CONTEXT.md](../ACACIA_PROJECT_CONTEXT.md): Actualización de Next Action y recommended commit message.
  * [docs/adr/0004-structured-json-maturity.md](../adr/0004-structured-json-maturity.md): Inclusión de la sección Decision Update para el Object Builder.
* **Creado**:
  * [test/phase10_object_builder_test.go](../../test/phase10_object_builder_test.go): Suite de pruebas unitarias verificando la correctitud de anidación, panics, nil-callbacks, escaping y redacción recursiva.
  * [test/phase10_object_builder_fuzz_test.go](../../test/phase10_object_builder_fuzz_test.go): Fuzz target para validar la integridad sintáctica de JSON ante inputs aleatorios y permutados.
  * [test/phase10_object_builder_benchmark_test.go](../../test/phase10_object_builder_benchmark_test.go): Suite de benchmarks diagnósticos.

---

## 3. Detalles de Implementación y Compatibilidad v2.x
* **Diseño Scoped**: La estructura `Object` actúa directamente sobre el búfer del evento mediante un puntero a slice (`*[]byte`). Esto evita alocaciones adicionales de búferes intermedios durante la serialización del objeto.
* **Rollback ante Panic**: Antes de ejecutar la función del callback, se registra el offset de escritura en el búfer. Si la ejecución del callback paniquea, el búfer se trunca (rollback) a ese offset y se añade a nivel del padre el campo seguro constante `"_acacia_error": "object callback panic"`, asegurando que el JSON de salida no quede truncado o corrupto.
* **Redacción Recursiva**: En todos los métodos de `Object` se aplica `isSensitiveKey` antes de serializar, asegurando que las claves sensibles se redacten de forma recursiva a cualquier profundidad.
* **Float64 Seguro**: Se hereda la semántica implementada en el sprint anterior, serializando floats normales como números JSON literales, y no finitos como strings seguros.
* **Compatibilidad**: Se conservan todas las firmas públicas de v2.x. El path legacy basado en mapas dinámicos sigue funcionando exactamente como antes.

---

## 4. Evidencia de Pruebas de Correctitud y Concurrencia
Se ejecutó la suite de pruebas unitarias completa con los nuevos tests incluidos:

### Pruebas Unitarias Generales (`go test ./...`)
```text
?   	github.com/humanjuan/acacia/v2	[no test files]
?   	github.com/humanjuan/acacia/v2/examples/basic	[no test files]
?   	github.com/humanjuan/acacia/v2/internal/logging	[no test files]
ok  	github.com/humanjuan/acacia/v2/test	19.618s
```

### Pruebas Unitarias Enfocadas (`go test ./test -run 'TestPhase10ObjectBuilder'`)
Todas las pruebas de correctitud de comportamiento de Object pasaron con éxito:
```text
ok  	github.com/humanjuan/acacia/v2/test	1.479s
```
Pruebas de la suite de Object ejecutadas:
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
No se reportaron data races bajo la suite ejecutada:
```text
ok  	github.com/humanjuan/acacia/v2/test	55.912s
```

---

## 5. Validación bajo Fuzzing
El fuzz target `FuzzPhase10ObjectBuilderEscapingAndRedaction` fue ejecutado durante 60 segundos, completando más de 223,000 iteraciones sin fallos ni pánicos no recuperados:
```text
fuzz: elapsed: 0s, gathering baseline coverage: 0/89 completed
fuzz: elapsed: 0s, gathering baseline coverage: 89/89 completed, now fuzzing with 12 workers
fuzz: elapsed: 3s, execs: 7351 (2449/sec), new interesting: 3 (total: 92)
...
fuzz: elapsed: 1m1s, execs: 223011 (0/sec), new interesting: 9 (total: 98)
PASS
ok  	github.com/humanjuan/acacia/v2/test	61.834s
```
**Resultado**: Sin corrupciones en las salidas JSON ni saltos de línea crudos dentro de las llaves o valores serializados.

---

## 6. Benchmarks Diagnósticos Resumidos
Los benchmarks del Object builder fueron recolectados en el disco externo de desarrollo con 10 repeticiones.

### Rangos Observados (Benchmarks Diagnósticos)
| Benchmark | Latencia (ns/op) | Memoria (B/op) | Alocaciones (allocs/op) |
| :--- | :---: | :---: | :---: |
| `BenchmarkPhase10ObjectBuilderSmall` | ~880 - 1360 ns/op | ~42 - 161 B/op | 2 allocs/op |
| `BenchmarkPhase10ObjectBuilderNested` | ~1020 - 1360 ns/op | ~60 - 201 B/op | 3 allocs/op |
| `BenchmarkPhase10ObjectBuilderFiveLevels` | ~1230 - 2860 ns/op | ~110 - 285 B/op | 6 allocs/op |
| `BenchmarkPhase10ObjectBuilderEscapedString` | ~950 - 2650 ns/op | ~44 - 110 B/op | 2 allocs/op |
| `BenchmarkPhase10ObjectBuilderRedactedField` | ~850 - 1620 ns/op | ~49 - 134 B/op | 2 allocs/op |
| `BenchmarkPhase10ObjectBuilderFloat64` | ~1140 - 1900 ns/op | ~44 - 124 B/op | 2 allocs/op |

### Análisis de Alocaciones y Rendimiento
* **Alocaciones**: los benchmarks diagnósticos observan incremento de allocs/op por nivel de anidación; la causa probable debe confirmarse con escape analysis si se decide optimizar este punto.
* **Escritura Append-Only**: El diseño conserva la escritura directa en el búfer final del evento, evitando la asignación y copia de búferes secundarios.

---

## 7. Regresión Diagnóstica Flat JSON
Se ejecutó regresión diagnóstica de Flat JSON. La suite funcional pasó; los benchmarks en disco externo mostraron variabilidad, por lo que no se usan como conclusión final de rendimiento.

---

## 8. Confirmaciones del Sprint
* **No SSD Hard Validation**: Todos los benchmarks y pruebas diagnósticas se han ejecutado en el disco externo de desarrollo. La validación de release en SSD controlado está diferida para el final de la Fase 10.
* **No Implementado**: Se omitió explícitamente la implementación de `Array Builder`, `Group`, `Namespace`, `RawJSON`, `Any` y soporte de `OpenTelemetry` para cumplir estrictamente con los límites de este sprint.

---

*Los resultados completos en bruto se guardaron en el directorio privado para auditoría interna.*
