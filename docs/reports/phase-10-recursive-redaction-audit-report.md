# Phase 10 — Recursive Redaction Audit Report

## 1. Objetivo y Alcance
El objetivo principal del Sprint 10.7 es realizar una auditoría, hardening y documentación exhaustiva de las políticas y consistencia de redacción en todos los builders estructurados de Acacia Logger:
* **Validación de Familias Sensibles**: Mapeo completo de las 21 familias de claves prohibidas y sus variantes con normalización case-insensitive y remoción de separadores (`-`, `_`, `.`).
* **Verificación Cross-Builder**: Confirmar que Flat, Object, Array, Group y Namespace apliquen redacción de forma consistente.
* **Fronteras y Límites**:
  * **RawJSON**: Confirmar que redacta únicamente la clave externa ( trusted input only).
  * **Any**: Verificar que realiza redacción externa y redacción recursiva interna best-effort a través de `encoding/json`.
  * **Legacy Map**: Documentar las características de compatibilidad v2.x sin extender promesas de optimización o redacción exacta recursiva.
* **Seguridad y Robustez**: Validar el control de fugas ante fallas de codificación, ciclos, caracteres UTF-8 no válidos, inyecciones de saltos de línea (CR/LF) y pánicos en callbacks.

No se introducen nuevas APIs públicas, ni soporte para `OpenTelemetry`, ni validaciones comparativas de rendimiento absoluto sobre SSD locales.

---

## 2. Archivos Modificados y Creados
* **Modificado**:
  * `acacia.go`: Hardening de la concordancia para la clave `authorization_header` / `authorizationheader` en selectores globales.
  * `docs/ACACIA_PROJECT_CONTEXT.md`: Actualización del estado y próxima acción del Sprint 10.7.
  * `docs/PHASE_10_STRUCTURED_JSON_API_CONTRACT.md`: Incorporación de la matriz de redacción estructurada.
  * `docs/adr/0004-structured-json-maturity.md`: Registro de la decisión de diseño de auditoría.
  * `docs/reports/README.md`: Adición del enlace al reporte público.
* **Creado**:
  * `test/phase10_recursive_redaction_audit_test.go`: Suite con 24 escenarios de correctitud.
  * `test/phase10_recursive_redaction_audit_fuzz_test.go`: Fuzz target para evaluar el comportamiento con llaves, valores y prefijos arbitrarios.
  * `test/phase10_recursive_redaction_audit_benchmark_test.go`: Benchmarks de caracterización del impacto de redacción.

---

## 3. Matriz de Redacción por Builder

| Builder / Flujo | Criterio de Redacción | Redacción Interna | Comportamiento del matcher de redacción |
| :--- | :--- | :---: | :---: |
| **Flat Builder** | Clave externa sensible en `isSensitiveKey` | N/A | Sin concatenación dinámica intencional de clave compuesta; el evento completo puede presentar allocs/op según benchmark |
| **Object Builder** | Clave recursiva a nivel de campos tipados | Sí (campos hijos) | Sin concatenación dinámica intencional de clave compuesta; el evento completo puede presentar allocs/op según benchmark |
| **Array Builder** | Claves internas de objetos agregados al arreglo | Sí (en callback) | Sin concatenación dinámica intencional de clave compuesta; el evento completo puede presentar allocs/op según benchmark |
| **Group Builder** | Reutiliza la lógica recursiva del Object Builder | Sí (campos hijos) | Sin concatenación dinámica intencional de clave compuesta; el evento completo puede presentar allocs/op según benchmark |
| **Namespace** | Claves compuestas, prefijos o segmentos sensibles | Sí | Sin concatenación dinámica intencional de clave compuesta; el evento completo puede presentar allocs/op según benchmark |
| **RawJSON** | Únicamente clave externa (reemplazo completo) | No (Trusted Input) | Sin concatenación dinámica intencional de clave compuesta; el evento completo puede presentar allocs/op según benchmark |
| **Any Fallback** | Clave externa sensible e interna best-effort | Sí (recursivo) | Camino lento; benchmarks diagnósticos observan ~20-45 allocs/op según caso |
| **Legacy Map** | Basado en el recorrido clásico de Acacia | Parcial (reflectivo) | Variable |

---

## 4. Familias de Claves Sensibles y Normalización
Se auditaron y validaron las siguientes familias de claves (case-insensitive y omitiendo separadores `-`, `_`, `.`):
* `password`, `passwd`, `pwd`
* `token`, `access_token`, `refresh_token`
* `authorization`, `authorization_header`
* `cookie`, `set-cookie`
* `api_key`, `apikey`, `client_secret`, `secret`, `private_key`
* `bearer`, `jwt`, `session`, `session_id`, `csrf`, `xsrf`, `x-api-key`

### Hardening de `authorization_header`
Se detectó que la variante `authorization_header` normalizada como `authorizationheader` no coincidía exactamente con el selector `"authorization"`. Se endurecieron los selectores globales en `acacia.go` añadiendo explícitamente el caso `"authorizationheader"`, cubriendo esa variante bajo la suite auditada.

---

## 5. Resultados de Pruebas y Robustez

### Pruebas Unitarias y Race Detector
Todas las suites pasaron correctamente sin observar fugas de los valores secretos usados por la suite ejecutada ni conflictos concurrentes:
```text
ok  	github.com/humanjuan/acacia/v2/test	17.822s
```
* **Panic Sentinels**: Pánicos provocados dentro de los callbacks del constructor (`Object`, `Namespace`) son capturados por Acacia. Limpian el buffer y emiten de forma limpia la etiqueta `"_acacia_error":"object callback panic"` (o `"namespace callback panic"`), sin observar filtración de los mensajes crudos de panic bajo los tests ejecutados al archivo de salida.
* **Control de Saltos de Línea (CR/LF)**: Los casos ejecutados validan que las entradas auditadas no generan saltos físicos de línea en el evento final, mitigando riesgos de inyección de logs.

### Validación bajo Fuzzing
El fuzz target `FuzzPhase10RecursiveRedactionAuditSensitiveKeys` se ejecutó durante 62 segundos, validando permutaciones aleatorias de llaves y builders sin observar corrupciones de sintaxis ni pánicos inesperados:
```text
PASS
ok  	github.com/humanjuan/acacia/v2/test	62.650s
```

---

## 6. Benchmarks Diagnósticos Resumidos
Caracterización de rendimiento recolectada con 10 repeticiones en entorno de disco externo:

### Rendimiento de Redacción en Builders Estructurados
| Benchmark | Rango observado (ns/op) | Alocaciones (allocs/op) |
| :--- | :---: | :---: |
| **Flat Builder (Str Redactado)** | ~500 - 700 ns/op | 1 alloc/op |
| **Object Builder (Redactado)** | ~630 - 810 ns/op | 2 allocs/op |
| **Array Builder (Elemento Redactado)** | ~600 - 820 ns/op | 3 allocs/op |
| **Namespace (Redactado)** | ~520 - 660 ns/op | 2 allocs/op |
| **RawJSON (Externa Redactada)** | ~460 - 640 ns/op | 1 alloc/op |
| **Any Fallback (Mapa Redactado)** | ~6100 - 9450 ns/op | 45 allocs/op |

*Nota: Los resultados de benchmarks diagnósticos confirman el coste de Any debido al flujo de decodificación y re-marshaling recursivo, mientras que los builders estructurados directos operan con alocaciones sumamente optimizadas.*

---

## 7. Regresión Diagnóstica de Builders
Se comparó el comportamiento con los builders consolidados (sprints 10.1 a 10.6). Las suites funcionales pasaron; los benchmarks en disco externo quedan como evidencia diagnóstica, no como conclusión final de rendimiento en los flujos previos.

---

## 8. Riesgos y Decisiones
* **No SSD Hard Validation**: Caracterizaciones realizadas sobre almacenamiento externo de desarrollo. Las métricas finales de liberación deben validarse en hardware de producción local SSD.
* **Sin Nuevas APIs**: Se respetó la regla de no modificar firmas de las APIs ni añadir métodos públicos.
* **RawJSON Trusted Input**: Se documenta formalmente la frontera de diseño: los payloads procesados mediante RawJSON no se inspeccionan internamente por motivos de rendimiento.
