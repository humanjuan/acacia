# Phase 10 — Any Fallback Report

## 1. Objetivo y Alcance
El objetivo principal del Sprint 10.6 es diseñar e incorporar soporte para la serialización dinámica de valores Go arbitrarios bajo la API estructurada de Acacia Logger utilizando un camino lento explícito:
* **Event.Any**: Inyecta un valor de tipo `interface{}` como campo en la raíz del evento.
* **Object.Any**: Inyecta un valor de tipo `interface{}` dentro de constructores de sub-objetos.
* **Camino Lento Explícito**: Aislado del hot-path mediante el uso de la biblioteca estándar `encoding/json` y reflexión interna para resolver tipos dinámicos. Su uso se desaconseja en hot paths de alta frecuencia.
* **Redacción Externa Completa**: Si el nombre de la clave externa es sensible, el payload completo se reemplaza por `"[REDACTED]"`.
* **Redacción Interna Best-Effort**: Para claves no sensibles, se ejecuta una decodificación y recorrido recursivo que reemplaza por `"[REDACTED]"` los valores de claves internas que coincidan con patrones sensibles.
* **Manejo de Ciclos y Tipos No Soportados**: Las referencias cíclicas, funciones, canales y valores Float64 no finitos (`NaN` o `Inf`) se degradan de forma segura al sentinel `{"_acacia_error":"any marshal error"}` sin provocar pánicos de ejecución.
* **Nil Safety**: Los valores `nil` se codifican de forma estándar como `null`.

No se incluye en esta fase el soporte para `Array.Any`, `Namespace.Any` o integraciones de `OpenTelemetry`.

---

## 2. Archivos Modificados y Creados
* **Modificado**:
  * `acacia.go`: Implementación de los métodos `Any` y helpers de redacción, validación y serialización lenta.
  * `docs/ACACIA_PROJECT_CONTEXT.md`: Actualización del estado del proyecto para Sprint 10.6.
  * `docs/PHASE_10_STRUCTURED_JSON_API_CONTRACT.md`: Refinamiento del comportamiento del contrato de Any fallback.
  * `docs/adr/0004-structured-json-maturity.md`: Inclusión del registro de decisiones técnicas del Sprint 10.6.
  * `docs/reports/README.md`: Registro del enlace al reporte público.
* **Creado**:
  * `test/phase10_any_test.go`: Suite de 23 pruebas de correctitud.
  * `test/phase10_any_fuzz_test.go`: Fuzz target para validar robustez, formato y escaping.
  * `test/phase10_any_benchmark_test.go`: Benchmarks diagnósticos del Any fallback.

---

## 3. Detalles de Implementación y APIs

### API de Any
Se agregaron las firmas a las estructuras principales:
```go
func (e *Event) Any(key string, val interface{}) *Event
func (o *Object) Any(key string, val interface{}) *Object
```

### Flujo de Codificación Lento y Redacción Interna
1. Si el valor es `nil`, se escribe directamente `null`.
2. Se serializa el valor original a bytes con `json.Marshal(val)`.
3. Si la serialización falla (debido a ciclos, funciones, canales o Float64 no finitos), se escribe de forma segura el sentinel `{"_acacia_error":"any marshal error"}`.
4. Si tiene éxito, se decodifica hacia una estructura genérica (`interface{}`) a través de un `json.Decoder` con `UseNumber()` habilitado para preservar la precisión numérica.
5. Se recorre recursivamente la estructura decodificada aplicando la política `redactDecodedJSONValue`, reemplazando valores de claves sensibles por `"[REDACTED]"`.
6. Se vuelve a serializar el árbol resultante y se inyecta utilizando el cargador de RawJSON seguro, aplicando compacción de saltos de línea si es necesario.

---

## 4. Evidencia de Pruebas de Correctitud

### Pruebas Unitarias (`go test ./...`)
Las pruebas correspondientes al Sprint 10.6 y anteriores pasaron de manera correcta:
```text
?   	github.com/humanjuan/acacia/v2	[no test files]
?   	github.com/humanjuan/acacia/v2/examples/basic	[no test files]
?   	github.com/humanjuan/acacia/v2/internal/logging	[no test files]
ok  	github.com/humanjuan/acacia/v2/test	16.253s
```

Se aprobaron de forma individual las 23 pruebas de `Any`:
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
* `TestPhase10AnyLegacyMapPathStillWorks` - PASS

### Race Detector (`go test -race ./...`)
La ejecución no reportó data races bajo la suite de pruebas ejecutada.

---

## 5. Validación bajo Fuzzing
El fuzz target `FuzzPhase10AnyValidityRedactionAndSingleLineSafety` se ejecutó durante 60 segundos, comprobando la estabilidad y comportamiento sin observar corrupciones de sintaxis ni saltos de línea inyectados bajo la suite ejecutada:
```text
PASS
ok  	github.com/humanjuan/acacia/v2/test	62.655s
```

---

## 6. Benchmarks Diagnósticos Resumidos
Recopilados con 10 repeticiones en el ambiente de desarrollo en disco externo:

### Perfil de Rendimiento Observado
| Caso de Prueba | Rango observado (ns/op) | Consumo de Memoria (B/op) | Alocaciones (allocs/op) |
| :--- | :---: | :---: | :---: |
| **Any Valor Primitivo** | ~1880 - 6390 ns/op | ~2500 B/op | 10 allocs/op |
| **Any Map Simple** | ~3820 - 5070 ns/op | ~1630 B/op | 25 allocs/op |
| **Any Struct** | ~3650 - 4740 ns/op | ~1600 B/op | 23 allocs/op |
| **Any Object (Map Simple)** | ~3060 - 3360 ns/op | ~1500 B/op | 21 allocs/op |
| **Any Redacción Mapa Anidado** | ~7240 - 10370 ns/op | ~2520 B/op | 45 allocs/op |
| **Any Slice de Objetos** | ~4640 - 5980 ns/op | ~2180 B/op | 38 allocs/op |
| **Any Key Externa Sensible** | ~470 - 670 ns/op | ~30 - 150 B/op | 1 alloc/op |
| **Any Fallback de No Soportado** | ~800 - 1060 ns/op | ~57 - 99 B/op | 3 allocs/op |

*Las alocaciones y latencias observadas en los benchmarks de Any confirman el comportamiento de camino lento en comparación con los constructores estructurados tipados, producto de los pasos de codificación y decodificación requeridos para la redacción recursiva best-effort.*

---

## 7. Regresión Diagnóstica de Builders Estructurados
Se ejecutaron suites de benchmark cruzados con 5 repeticiones para verificar que los cambios no introdujeron variaciones imprevistas en los componentes ya consolidados:
* Las suites funcionales de Flat, Object, Array, Group, Namespace y RawJSON pasaron sin inconvenientes.
* Los resultados de benchmarks en disco de desarrollo externo se registran únicamente con fines diagnósticos y no constituyen conclusiones comparativas absolutas de rendimiento.

---

## 8. Riesgos y Decisiones
* **No SSD Hard Validation**: Las métricas se recolectaron en almacenamiento externo de desarrollo. La caracterización final de latencias y alocaciones de release requiere pruebas sobre almacenamiento local SSD interno bajo condiciones controladas.
* **Redacción Interna Best-effort**: El proceso recursivo de redacción interna cubre mapas, structs serializables por json tags y slices, pero no promete redacción perfecta en tipos o serializadores complejos no compatibles con `encoding/json`.
* **Exclusiones**: Quedan diferidos `Array.Any` y `Namespace.Any`.
