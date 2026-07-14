# Phase 10 — RawJSON Report

## 1. Objetivo y Alcance
El objetivo principal del Sprint 10.5 es diseñar e incorporar soporte para la inserción de payloads JSON pre-serializados de forma directa como campos dentro del esquema estructurado en Acacia Logger:
* **Event.RawJSON**: Método en el constructor de eventos raíz para adjuntar JSON válidos o escapados de forma segura.
* **Object.RawJSON**: Método para inyectar JSON estructurados dentro de sub-objetos del constructor.
* **Compacción de Payloads**: Mitigación de inyecciones mediante compresión de saltos de línea (`\n` o `\r`) en payloads JSON válidos.
* **Comportamiento Seguro ante Payload Inválido**: Fallback automático que serializa el payload corrupto o no-válido como un string JSON escapado.
* **Nil/Empty Safety**: Los payloads `nil` o `[]byte{}` vacíos se manejan de forma segura como strings vacíos (`""`), evitando pánicos de ejecución.
* **Redacción por Key Externa**: Si el nombre de la clave es sensible, el payload completo se reemplaza por `"[REDACTED]"`.
* **Sin Redacción Interna**: Los payloads válidos adjuntados no se inspeccionan internamente en búsqueda de claves sensibles (contractualmente definido como **trusted input only**).

No se incluye en esta fase el soporte para `Array.RawJSON`, `Namespace.RawJSON`, `Any` o integraciones de `OpenTelemetry`.

---

## 2. Archivos Modificados y Creados
* **Modificado**:
  * `acacia.go`: Importación de `encoding/json`, adición de los métodos `RawJSON` y helpers de codificación/validación.
  * `docs/ACACIA_PROJECT_CONTEXT.md`: Actualización del estado del proyecto para Sprint 10.5.
  * `docs/PHASE_10_STRUCTURED_JSON_API_CONTRACT.md`: Especificación del comportamiento del contrato de RawJSON.
  * `docs/adr/0004-structured-json-maturity.md`: Inclusión del registro de decisiones técnicas del Sprint 10.5.
  * `docs/reports/README.md`: Índice del nuevo reporte.
* **Creado**:
  * `test/phase10_rawjson_test.go`: Suite de 19 pruebas de correctitud.
  * `test/phase10_rawjson_fuzz_test.go`: Fuzz target para validar robustez, formato y escaping.
  * `test/phase10_rawjson_benchmark_test.go`: Benchmarks diagnósticos de RawJSON.

---

## 3. Detalles de Implementación y APIs

### API de RawJSON
Se añadieron las firmas a las estructuras principales:
```go
func (e *Event) RawJSON(key string, raw []byte) *Event
func (o *Object) RawJSON(key string, raw []byte) *Object
```

### Flujo de Codificación Seguro
1. Se valida el payload mediante `json.Valid(raw)`.
2. Si el payload es válido:
   - Se revisa si contiene bytes de control de salto de línea (`\n` o `\r`).
   - De contenerlos, se ejecuta `json.Compact` en un búfer temporal para transformarlo a una sola línea.
   - De ser un payload plano, se copia directamente al buffer activo del log mediante una reserva de capacidad controlada.
3. Si el payload no es válido, se delega al formateador seguro `appendEscapedRawJSONString` que escapa los caracteres especiales y encierra el valor como un string regular.

---

## 4. Evidencia de Pruebas de Correctitud

### Pruebas Unitarias (`go test ./...`)
Las pruebas correspondientes al Sprint 10.5 y anteriores pasaron de manera correcta:
```text
?   	github.com/humanjuan/acacia/v2	[no test files]
?   	github.com/humanjuan/acacia/v2/examples/basic	[no test files]
?   	github.com/humanjuan/acacia/v2/internal/logging	[no test files]
ok  	github.com/humanjuan/acacia/v2/test	16.673s
```

Se aprobaron de forma individual las 19 pruebas específicas para `RawJSON`:
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
La ejecución no reportó data races bajo la suite de pruebas ejecutada.

---

## 5. Validación bajo Fuzzing
El fuzz target `FuzzPhase10RawJSONValidityAndSingleLineSafety` se ejecutó de forma correcta durante 60 segundos, comprobando la estabilidad estructural frente a payloads alterados y sin observar corrupciones de sintaxis ni saltos de línea inyectados bajo la suite ejecutada:
```text
PASS
ok  	github.com/humanjuan/acacia/v2/test	62.542s
```

---

## 6. Benchmarks Diagnósticos Resumidos
Recopilados con 10 repeticiones en el ambiente de desarrollo en disco externo:

### Perfil de Rendimiento Observado
| Caso de Prueba | Rango observado (ns/op) | Consumo de Memoria (B/op) | Alocaciones (allocs/op) |
| :--- | :---: | :---: | :---: |
| **Fast Path Válido Plano (Objeto/Array/Scalar)** | ~610 - 990 ns/op | ~25 - 47 B/op | 1 alloc/op |
| **Object Interno RawJSON** | ~670 - 1030 ns/op | ~42 - 53 B/op | 2 allocs/op |
| **Fallback Inválido** | ~640 - 760 ns/op | ~50 - 90 B/op | 2 allocs/op |
| **Redacción por Key Externa** | ~510 - 710 ns/op | ~29 - 93 B/op | 1 alloc/op |
| **Compacción Multiline** | ~900 - 1410 ns/op | ~89 - 92 B/op | 2 allocs/op |

*Las alocaciones observadas en el fast path plano corresponden a la instanciación interna del búfer o a closures y análisis de escape de Go; la causa exacta debe verificarse con perfiles dedicados si se requiere optimización en un entorno productivo.*

---

## 7. Regresión Diagnóstica de Builders Estructurados
Se ejecutaron suites de benchmark cruzados con 5 repeticiones para verificar que los cambios no introdujeron variaciones imprevistas en los componentes ya consolidados:
* Las suites de Flat, Object, Array, Group y Namespace pasaron sin inconvenientes funcionales.
* Los resultados de benchmarks en disco de desarrollo externo se registran únicamente con fines diagnósticos y no constituyen conclusiones comparativas absolutas de rendimiento.

---

## 8. Riesgos y Decisiones
* **No SSD Hard Validation**: Las métricas se recolectaron en almacenamiento externo de desarrollo. La caracterización final de latencias y alocaciones de release requiere pruebas sobre almacenamiento local SSD interno bajo condiciones controladas.
* **Trusted Input Only**: El contrato asume que los payloads son previamente sanitizados para redacción interna. Si se requiere inspección recursiva y modificación de llaves anidadas en el payload, el cliente debe utilizar la interfaz normal `Object` o los builders estándar.
* **Exclusiones**: Quedan diferidos `Array.RawJSON`, `Namespace.RawJSON` y el fallback `Any`.
