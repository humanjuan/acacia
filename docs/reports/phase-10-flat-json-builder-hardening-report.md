# Phase 10 — Flat JSON Builder Hardening Report

## 1. Objetivo y Alcance
El objetivo principal del Sprint 10.1 es robustecer y hardening el camino JSON plano del builder estructurado en Acacia Logger. 
Este hardening incluye:
* Codificación y escapado seguro de JSON de caracteres de control, Unicode y UTF-8 inválido.
* Implementación y verificación del comportamiento frente a claves duplicadas, valores vacíos y `Err(nil)`.
* Redacción case-insensitive de claves sensibles en la raíz del objeto.
* Mitigación de inyecciones de logs (log injection) mediante el escapado estricto de retornos de carro y saltos de línea (`\r\n`).
* Asegurar que no se introducen dependencias de reflexión o de `encoding/json` en el hot path.
* Manejo seguro de valores `float64` no finitos (`NaN`, `+Inf`, `-Inf`) codificándolos como strings JSON válidos en lugar de literales numéricos sin comillas.
* Optimización del codificador mediante un escaneo fast-path optimizado para caracteres ASCII con fallback dinámico y seguro hacia decodificación UTF-8 únicamente cuando se detectan bytes no ASCII.

---

## 2. Archivos Modificados y Creados
* **Modificado**:
  * [acacia.go](../../acacia.go): Importación de `unicode/utf8` y `math`, implementación de escapado con fast-path ASCII y fallback UTF-8 seguro en `escapeJSONInto`, actualización de los métodos del builder de `Event` para escapar tanto claves como valores, y adición del filtro para floats no finitos.
* **Creado**:
  * [test/phase10_flat_json_builder_test.go](../../test/phase10_flat_json_builder_test.go): Suite de pruebas unitarias cubriendo los 12 escenarios clave de validación.
  * [test/phase10_flat_json_builder_fuzz_test.go](../../test/phase10_flat_json_builder_fuzz_test.go): Fuzz target para validar secuencias hostiles de UTF-8, CRLF y redacción bajo permutaciones infinitas.
  * [test/phase10_flat_json_builder_benchmark_test.go](../../test/phase10_flat_json_builder_benchmark_test.go): Suite de benchmarks diagnósticos.

---

## 3. Detalles de Implementación y Compatibilidad v2.x
* **Escapado Seguro con Fast-Path ASCII**: Se implementó una lógica de escaneo por bytes en `escapeJSONInto`. Mientras los caracteres inspeccionados sean ASCII seguro (`c < utf8.RuneSelf`), se copian en bloques continuos de memoria de manera directa. Si se detecta un carácter especial que requiere escapado (`"`, `\`, caracteres de control, saltos de línea, etc.), se escribe la secuencia de escape correspondiente y se reanuda el escaneo. Únicamente si se detecta un byte mayor o igual a `utf8.RuneSelf` (no ASCII), se realiza un fallback seguro hacia `utf8.DecodeRuneInString`, el cual decodifica y reemplaza secuencias inválidas por la runa de reemplazo `\uFFFD`.
* **Escapado de Claves**: Se extendieron todos los métodos del builder estructurado para pasar tanto la clave como el valor a través del codificador `escapeJSONInto`, previniendo inyecciones de sintaxis a través de claves controladas por el usuario.
* **Floats No Finitos**: El método `Float64` conserva los valores numéricos finitos como números literales en la salida estructurada, pero detecta y degrada valores no finitos (`NaN`, `+Inf`, `-Inf`) a cadenas JSON seguras (`"NaN"`, `"+Inf"`, `"-Inf"`).
* **Claves Duplicadas**: Siguiendo el principio de rendimiento óptimo, no se realiza deduplicación en caliente. Ambas claves se escriben directamente de forma secuencial en el búfer ("last write wins" en ingesta).
* **Err(nil)**: Si se pasa un error `nil` mediante `Err(nil)`, este es ignorado y omitido del JSON final para mantener compatibilidad con el comportamiento existente.
* **Redacción**: El método `isSensitiveKey` normaliza y filtra las claves sensibles de forma case-insensitive, reemplazando el valor original por `[REDACTED]`. Los métodos `.Secret()` y `.StrRedacted()` redactan el valor de forma explícitamente segura.
* **Compatibilidad**: No se introdujeron cambios de firma destructivos ni breaking changes para v2.x. La API de compatibilidad para el path legacy basado en mapas (`StructuredJSON(true)` + `Info(map...)`) funciona exactamente igual.

---

## 4. Evidencia de Pruebas de Correctitud y Concurrencia
Se ejecutó la suite de pruebas unitarias completa con los nuevos tests incluidos:

### Pruebas Unitarias Generales (`go test ./...`)
```text
?   	github.com/humanjuan/acacia/v2	[no test files]
?   	github.com/humanjuan/acacia/v2/examples/basic	[no test files]
?   	github.com/humanjuan/acacia/v2/internal/logging	[no test files]
ok  	github.com/humanjuan/acacia/v2/test	13.731s
```

### Pruebas Unitarias Enfocadas (`go test ./test -run 'TestPhase10FlatJSON'`)
Todas las pruebas de correctitud de comportamiento pasaron con éxito:
```text
ok  	github.com/humanjuan/acacia/v2/test	0.813s
```
Pruebas de la suite ejecutadas:
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
El detector de carreras se ejecutó sobre toda la suite de pruebas del repositorio, y no se reportaron data races bajo la suite ejecutada:
```text
?   	github.com/humanjuan/acacia/v2	[no test files]
?   	github.com/humanjuan/acacia/v2/examples/basic	[no test files]
?   	github.com/humanjuan/acacia/v2/internal/logging	[no test files]
ok  	github.com/humanjuan/acacia/v2/test	25.937s
```

---

## 5. Validación bajo Fuzzing
El fuzz target `FuzzPhase10FlatJSONStringEscaping` fue ejecutado de manera continua durante 60 segundos, cubriendo más de 180,000 iteraciones con claves, valores, saltos de línea y secuencias UTF-8 hostiles aleatorias:
```text
fuzz: elapsed: 0s, gathering baseline coverage: 0/93 completed
fuzz: elapsed: 0s, gathering baseline coverage: 93/93 completed, now fuzzing with 12 workers
fuzz: elapsed: 3s, execs: 20836 (6945/sec), new interesting: 2 (total: 95)
...
fuzz: elapsed: 1m1s, execs: 180566 (0/sec), new interesting: 6 (total: 99)
PASS
ok  	github.com/humanjuan/acacia/v2/test	61.697s
```
**Resultado**: Sin pánicos, sin salidas JSON corruptas y sin saltos de línea crudos dentro de las llaves/valores serializados.

---

## 6. Benchmarks Diagnósticos Resumidos
Los benchmarks del fast path estructurado fueron recolectados en el disco externo de desarrollo con 10 repeticiones.

### Valores Representativos Observados
| Benchmark | Latencia (ns/op) | Memoria (B/op) | Alocaciones (allocs/op) |
| :--- | :---: | :---: | :---: |
| `BenchmarkPhase10FlatJSONBuilderSmall` | ~830 ns/op | ~33 B/op | 1 allocs/op |
| `BenchmarkPhase10FlatJSONBuilderTenFields` | ~1200 ns/op | ~27 B/op | 1 allocs/op |
| `BenchmarkPhase10FlatJSONBuilderEscapedString` | ~590 ns/op | ~29 B/op | 1 allocs/op |
| `BenchmarkPhase10FlatJSONBuilderRedactedField` | ~620 ns/op | ~84 B/op | 1 allocs/op |
| `BenchmarkPhase10FlatJSONBuilderDuplicateKeys` | ~670 ns/op | ~42 B/op | 1 allocs/op |
| `BenchmarkPhase10FlatJSONBuilderDisabledLevel` | ~15 ns/op | 0 B/op | 0 allocs/op |
| `BenchmarkPhase10FlatJSONBuilderASCIIString` | ~630 ns/op | ~40 B/op | 1 allocs/op |
| `BenchmarkPhase10FlatJSONBuilderInvalidUTF8String` | ~660 ns/op | ~64 B/op | 1 allocs/op |

### Análisis de Alocaciones y Rendimiento
* **Disabled Level**: Muestra un overhead mínimo (~15 ns/op) y `0 allocs/op` (sin alocaciones en el heap cuando el nivel del log está desactivado).
* **Builder Alocaciones (1 alloc/op)**: La causa probable está asociada a sync.Pool/interface accounting; debe confirmarse con escape analysis si se persigue 0 allocs/op en el path activo. No existe ningún tipo de reflexión ni marshalings pesados en este camino caliente.

---

## 7. Confirmaciones del Sprint
* **No SSD Hard Validation**: Todos los benchmarks y pruebas diagnósticas se han ejecutado en el disco externo de desarrollo. La validación final de release en SSD interno controlado está diferida para el final de la Fase 10.
* **No Implementado**: Se omitió la implementación de `Object Builder`, `Array Builder`, `Group`, `Namespace`, `RawJSON`, `Any` y soporte de `OpenTelemetry` para cumplir estrictamente con los límites de este sprint.
* **Riesgos Pendientes**: Validar el impacto de la decodificación rune en cadenas gigantes (>10KB) y su consumo de CPU.

---

*Los resultados completos en bruto se guardaron en el directorio privado para auditoría interna.*
