# Phase 04 — Redaction and Sensitive Data Security Report

* **Date:** 2026-07-07
* **Branch:** `dev`
* **Commit:** `da9803e perf: implement append-only JSON encoder with benchmarks and fuzzing`
* **Go version:** `go1.23.2 darwin/amd64`
* **OS/Arch:** `darwin/amd64`
* **Scope:** Diseño e integración de un sistema de redacción automática y explícita para datos sensibles en el hot path estructurado de Acacia.

---

## Objective
Evitar fugas accidentales de secretos en logs estructurados (p. ej., tokens, contraseñas, cookies) mediante un mecanismo de redacción case-insensitive y normalizada de claves (sin allocations adicionales sobre el builder), sin hacer uso de reflexión ni expresiones regulares en el hot path.

---

## Scope Decision
* **Protegido:** El hot path de la nueva API Fluent/Builder estructurada tipada (`Event`). Todos los campos que utilicen el constructor tipado (como `Str`, `Int`, `Bool`, `Time`, `Dur`, y `ErrKey`) se benefician de la redacción automática y explícita de forma directa.
* **Compatibilidad/Deuda Legacy:** El wrapper heredado en `internal/logging/wrapper.go` que emplea `redactRecursive` reflectivo se mantiene intacto para no romper a los clientes legacy. Su alto costo de CPU/memoria queda registrado bajo deuda técnica a extinguir cuando se remueva por completo el path legacy.

---

## Redaction Design
* **Redacción Explícita:** Se mantienen los métodos `Secret(key, val)` y `StrRedacted(key, val)` que sustituyen directamente el valor por `"[REDACTED]"` a nivel de bytes, sin allocations ni escaneos.
* **Redacción Automática por Clave:** Cada llamada de campo tipado en `Event` intercepta la clave mediante una función interna ultrarrápida `isSensitiveKey(key)`. Si se detecta una clave sensible, se escribe inmediatamente `[REDACTED]` y se aborta la serialización del valor original.
* **Normalización en pila sin allocations (key check zero-allocation):** La función `isSensitiveKey` normaliza la clave de forma case-insensitive y descarta caracteres separadores (`-`, `_`, `.`) en una variable local de pila (array de 64 bytes), evitando asignaciones de Heap.

---

## Sensitive Key Policy
Se protegen las siguientes palabras clave sensibles en cualquier combinación de mayúsculas/minúsculas y separadores:
* `authorization`, `cookie`, `set-cookie`, `token`, `access_token`, `refresh_token`, `password`, `passwd`, `pwd`, `api_key`, `private_key`, `client_secret`, `secret`, `bearer`, `jwt`, `session`, `session_id`, `csrf`, `xsrf`, `x-api-key`.

---

## Public API Impact
* **Ningún cambio incompatible.** La API de v2.x no sufre modificaciones de firmas ni de comportamiento roto. Se incorporó `ErrKey(key, err)` en `Event` para soportar el mapeo de errores bajo claves sensibles personalizadas.

---

## Files Changed
* **acacia.go**: Adición del normalizador case-insensitive `isSensitiveKey`, el helper de formateo `appendRedactedField`, y la integración del filtro automático en todos los métodos de campo estructurado de `Event`.
* **test/phase04_redaction_test.go**: Suite de pruebas de conformidad unitaria.
* **test/phase04_redaction_benchmark_test.go**: Suite de benchmarks de redacción.
* **test/phase04_redaction_fuzz_test.go**: Suite de fuzzing ofensivo de inyección y detección de bypasses.

---

## Tests Executed

| Command | Result | Notes |
|---|---:|---|
| `go test ./...` | **PASS** | Suite de pruebas Acacia en verde. |
| `go test -race ./...` | **PASS** | Sin condiciones de carrera. |

---

## Fuzzing Summary

| Target | Result | Notes |
|---|---:|---|
| `FuzzRedactionSensitiveKeysNoLeak` | **PASS** | Valida que ninguna variante de clave sensible fugue su valor. |
| `FuzzRedactionKeyVariantsNoBypass` | **PASS** | Valida que normalizaciones hostiles no burlen el redactor. |
| `FuzzRedactionMixedTypesNoLeak` | **PASS** | Valida que tipos no-string (Int, Bool, etc.) se redacten de forma segura. |
| `FuzzRedactionNonSensitivePreserved` | **PASS** | Valida que los datos regulares no sensibles no sufran alteraciones. |

---

## Benchmark Summary

### GOMAXPROCS=1
| Benchmark | ns/op | B/op | allocs/op | Compared To | Notes |
|---|---:|---:|---:|---|---|
| `BenchmarkRedactionDisabledLevel` | 16.3 ns | 0 B | 0 | `Fase 03` | Cero allocations |
| `BenchmarkRedactionExplicitSecret` | 634.3 ns | 282 B | 1 | `Fase 03` | Sin costo adicional |
| `BenchmarkRedactionAutoSensitiveStr` | 647.5 ns | 219 B | 1 | `Fase 03` | Detección automática en 1 alloc |
| `BenchmarkRedactionAutoSensitiveMixed` | 736.5 ns | 284 B | 1 | `Fase 03` | Redacta 4 claves en 1 alloc |
| `BenchmarkRedactionNonSensitiveStr` | 641.0 ns | 231 B | 1 | `Fase 03` | Path normal sin penalización |
| `BenchmarkRedactionKeyCheckSensitive` | 18.0 ns | 0 B | 0 | - | Sin alocaciones |
| `BenchmarkRedactionKeyCheckNonSensitive`| 19.2 ns | 0 B | 0 | - | Sin alocaciones |
| `BenchmarkRedactionManyFields` | 768.9 ns | 234 B | 1 | - | Reutilización de búferes |

### GOMAXPROCS=2
| Benchmark | ns/op | B/op | allocs/op | Compared To | Notes |
|---|---:|---:|---:|---|---|
| `BenchmarkRedactionDisabledLevel` | 21.4 ns | 0 B | 0 | `Fase 03` | Descarte rápido |
| `BenchmarkRedactionExplicitSecret` | 727.1 ns | 246 B | 1 | `Fase 03` | Sin impacto de memoria |
| `BenchmarkRedactionAutoSensitiveStr` | 791.1 ns | 245 B | 1 | `Fase 03` | 1 alocación única |
| `BenchmarkRedactionAutoSensitiveMixed` | 806.6 ns | 225 B | 1 | `Fase 03` | 1 alocación única |
| `BenchmarkRedactionNonSensitiveStr` | 993.6 ns | 233 B | 1 | `Fase 03` | 1 alocación única |

---

## Legacy Redaction Status
* La función `redactRecursive` del wrapper legacy (que consume **22 alocaciones** y **980 B/op** debido al uso extensivo de reflexión y mapeo dinámico) permanece en **`internal/logging/wrapper.go`** para sostener retrocompatibilidad, pero su uso no es recomendado en el hot path de alto rendimiento.

---

## pprof Summary
El perfilador de asignaciones para la redacción automatizada muestra que:
* La llamada `isSensitiveKey` no registra uso de memoria en Heap (**0 B/op**).
* Todo el Heap asignado corresponde al puntero genérico de `sync.Pool` devuelto en `NewEvent`.
* En la comparación directa de redacción mixta:
  * **Legacy (`BenchmarkVector2Redact`):** Asigna 1.96 GB de Heap acumulado en perfiles.
  * **New (`BenchmarkRedactionAutoSensitiveMixed`):** Asigna 0.67 GB de Heap acumulado (65% menos memoria asignada y 22x menos alocaciones netas).

---

## Hot Path Restrictions Check
El camino de redacción estructurada tipada:
* **¿Utiliza reflect?** **NO**.
* **¿Utiliza regexp?** **NO**.
* **¿Utiliza strings.ToLower con allocation?** **NO**.
* **¿Utiliza fmt.Sprint/fmt.Sprintf?** **NO**.
* **¿Utiliza encoding/json.Marshal?** **NO**.
* **¿Utiliza map[string]interface{}?** **NO**.

---

## Security Findings
* **Ninguno.** No se detectaron vulnerabilidades ni fugas (*bypasses*) tras la ejecución completa del motor de fuzzing.

---

## Acceptance Criteria Status

| Criterion | Status | Evidence |
|---|---|---|
| Redacción inline sin allocations adicionales sobre el builder | **CUMPLIDO** | `isSensitiveKey` retorna 0 allocs/op; el flujo completo del builder mantiene 1 alloc/op por `sync.Pool`. |
| Denylist insensible a case/separadores sin Heap | **CUMPLIDO** | `isSensitiveKey` retorna 0 allocs/op. |
| Redacción automática para tipos no-string | **CUMPLIDO** | Validado en `TestRedactionMixedTypes`. |
| Fuzzing ofensivo completo en verde | **CUMPLIDO** | Fuzzing de bypass y no fuga completado. |

---

## Risks / Regressions
* **Diferencia de comportamiento:** El builder nuevo aplica redacción automática sobre las claves sensibles registradas de forma inmediata, mientras que la API legacy no implementa denylist automática por defecto (solo redacción recursiva opcional en mapas del wrapper). Esto se documenta y acepta como restricción de diseño de convivencia en v2.

---

## Private Evidence References
* `.private/reports/phase-04-private-evidence.md`
* `.private/bench/phase04_redaction_bench_default.txt`
* `.private/bench/phase04_redaction_bench_gmp1.txt`
* `.private/bench/phase04_redaction_bench_gmp2.txt`
* `.private/pprof/phase04_redaction_cpu.pprof`
* `.private/pprof/phase04_redaction_mem.pprof`

---

## Next Phase Recommendation
Se recomienda avanzar a la **Fase 05 — Backpressure, cola y memoria** para rediseñar el canal de eventos asíncronos y resolver la contención por lock, permitiendo maximizar el rendimiento multitarea de Acacia.
