# Phase 02 — Typed Structured API Report

* **Date:** 2026-07-07
* **Branch:** `dev`
* **Commit:** `13b5b5b fix: remediate JSON control character escaping vulnerability and document Phase 01 baseline`
* **Go version:** `go1.23.2 darwin/amd64`
* **OS/Arch:** `darwin/amd64`
* **Scope:** Diseño e implementación de la nueva Fluent API estructurada tipada (Builder).

---

## Objective
Diseñar e implementar una API Fluent/Builder estructurada tipada para Acacia que permita escribir logs estructurados de alto rendimiento sin pasar por conversiones a interfaces (`interface{}`), mapeo dinámico (`map[string]interface{}`), reflexión, ni marshaling dinámico de JSON en el hot path. Todo esto conviviendo sin romper la retrocompatibilidad con la API clásica de v2.x.

---

## API Compatibility Decision
Dado que en Go no se permite la sobrecarga de métodos, no era posible redefinir `Info(data interface{}, args ...interface{})` para retornar un builder. Se diseñó una API tipada para campos estructurados.
Se optó por introducir métodos de evento explícitos:
* `Event(level string) *Event` (Genérico)
* `InfoEvent() *Event`
* `WarnEvent() *Event`
* `ErrorEvent() *Event`
* `DebugEvent() *Event`
* `CriticalEvent() *Event`

Esta nomenclatura es limpia, compatible y sigue el estándar de la industria (p. ej., zerolog/zap), permitiendo a los usuarios migrar paulatinamente sus paths críticos a la nueva API tipada mientras mantienen el resto de la aplicación con la API legacy tradicional.

Nota de diseño: El método genérico `Event(level string)` recibe el nivel como string para conservar compatibilidad con la representación actual de niveles de Acacia, pero queda establecido como un punto evaluable para transicionar a una API más estricta (tipada) en v3 o una fase posterior.

---

## Public API Added
Se incorporaron los siguientes métodos públicos en `Log`:
* `func (l *Log) Event(level string) *Event`
* `func (l *Log) InfoEvent() *Event`
* `func (l *Log) WarnEvent() *Event`
* `func (l *Log) ErrorEvent() *Event`
* `func (l *Log) DebugEvent() *Event`
* `func (l *Log) CriticalEvent() *Event`

Y la estructura `Event` con sus métodos builder encadenables:
* `func (e *Event) Str(key, val string) *Event`
* `func (e *Event) Int(key string, val int) *Event`
* `func (e *Event) Int64(key string, val int64) *Event`
* `func (e *Event) Uint64(key string, val uint64) *Event`
* `func (e *Event) Bool(key string, val bool) *Event`
* `func (e *Event) Float64(key string, val float64) *Event`
* `func (e *Event) Err(err error) *Event`
* `func (e *Event) Dur(key string, val time.Duration) *Event`
* `func (e *Event) Time(key string, val time.Time) *Event`
* `func (e *Event) Secret(key, val string) *Event` (Redacción inicial Fase 02)
* `func (e *Event) StrRedacted(key, val string) *Event`
* `func (e *Event) Msg(msg string)` (Finaliza y envía el log)
* `func (e *Event) Send()` (Finaliza y envía el log sin mensaje principal)

---

## Implementation Summary
* **Búferes Reutilizables:** `Event` recupera búferes de bytes `[]byte` desde el pool interno de Acacia (`getJSONBufCap`) y escribe el payload de JSON de manera incremental mediante métodos rápidos (`strconv.AppendInt`, `escapeJSONInto` sin allocations).
* **Pool de Estructuras:** Las estructuras `Event` se obtienen y regresan a un `sync.Pool` (`eventPool`), reduciendo la alocación de control a un máximo de **1 alloc/op** debido al escape de la interfaz del pool.
* **Evaluación Temprana:** Si el nivel de log está deshabilitado, `NewEvent` retorna inmediatamente `nil`. Las llamadas encadenadas sobre un puntero `nil` se descartan sin costo, resultando en **0 allocs/op** y cero cómputo.
* **Integración:** El evento serializado se encola como un mensaje de tipo `kind: 2` (preformatted JSON), reutilizando el canal asíncrono y el backend de escritura en disco existente.

---

## Files Changed
* **acacia.go**: Estructura `Event` y métodos de inicialización y escritura.
* **test/phase02_event_builder_test.go**: Suite de pruebas de conformidad, control de escapes, convivencia y rotación.
* **test/phase02_event_builder_benchmark_test.go**: Suite de benchmarks.
* **test/phase02_event_builder_fuzz_test.go**: Fuzzing de inyección de líneas, JSON válido y no fuga de secretos.

---

## Tests Executed

| Command | Result | Notes |
|---|---:|---|
| `go test ./...` | **PASS** | Suite completa en verde. |
| `go test -race ./...` | **PASS** | Sin condiciones de carrera detectadas. |

---

## Benchmark Summary

### Escenario con GOMAXPROCS=1
| Benchmark | GOMAXPROCS | ns/op | B/op | allocs/op | Compared To | Notes |
|---|---:|---:|---:|---:|---|---|
| `BenchmarkEventBuilderDisabledLevel` | 1 | 18.9 ns | 0 B | 0 | `Benchmark_structured` | Descarte de nivel desactivado |
| `BenchmarkEventBuilderSmall` | 1 | 709.1 ns | 297 B | 1 | `BenchmarkVector1JSON` | 13x menos allocations |
| `BenchmarkEventBuilderStringFields` | 1 | 685.6 ns | 248 B | 1 | `BenchmarkVector1JSON` | 13x menos allocations |
| `BenchmarkEventBuilderMixedFields` | 1 | 758.8 ns | 269 B | 1 | `BenchmarkVector1JSON` | 13x menos allocations |
| `BenchmarkEventBuilder1KB` | 1 | 14371.0 ns | 35247 B | 10 | `BenchmarkVector1JSON` | Regresión de latencia/memoria |
| `BenchmarkVector1JSON` (Legacy Map) | 1 | 3541.0 ns | 750 B | 13 | Baseline anterior | 13 allocations por boxing |

### Escenario con GOMAXPROCS=2 (Bajo Paralelismo)
| Benchmark | GOMAXPROCS | ns/op | B/op | allocs/op | Compared To | Notes |
|---|---:|---:|---:|---:|---|---|
| `BenchmarkEventBuilderDisabledLevel` | 2 | 21.4 ns | 0 B | 0 | `Benchmark_structured` | Descarte ultra rápido |
| `BenchmarkEventBuilderSmall` | 2 | 636.6 ns | 209 B | 1 | `BenchmarkVector1JSON` | 13x menos allocations |
| `BenchmarkEventBuilderStringFields` | 2 | 937.0 ns | 155 B | 1 | `BenchmarkVector1JSON` | 13x menos allocations |
| `BenchmarkEventBuilderMixedFields` | 2 | 1298.0 ns | 172 B | 1 | `BenchmarkVector1JSON` | 13x menos allocations |
| `BenchmarkEventBuilder1KB` | 2 | 16588.0 ns | 31817 B | 10 | `BenchmarkVector1JSON` | Regresión de latencia/memoria |
| `BenchmarkVector1JSON` (Legacy Map) | 2 | 4713.0 ns | 648 B | 13 | Baseline anterior | 13 allocations por boxing |

---

## Fuzzing Summary

| Target | Result | Notes |
|---|---:|---|
| `FuzzEventBuilderJSONValid` | **PASS** | Genera líneas JSON conformes a RFC 8259. |
| `FuzzEventBuilderNoLineInjection` | **PASS** | Evita inyección de líneas físicas por salto de línea. |
| `FuzzEventBuilderSecretNoLeak` | **PASS** | Valores secretos son correctamente enmascarados como `[REDACTED]`. |

---

## Hot Path Restrictions Check
El camino crítico de la nueva API Fluent estructurada:
* **¿Utiliza reflect?** **NO**.
* **¿Utiliza fmt.Sprint/Sprintf?** **NO**.
* **¿Utiliza encoding/json.Marshal?** **NO**.
* **¿Utiliza map[string]interface{}?** **NO**.

---

## Acceptance Criteria Status

| Criterion | Status | Evidence |
|---|---|---|
| API Fluent compatible con v2 implementada | **CUMPLIDO** | Métodos `InfoEvent` y builder agregados en `acacia.go`. |
| Disabled level en 0 allocs/op | **CUMPLIDO** | `BenchmarkEventBuilderDisabledLevel` retorna 0 allocs/op. |
| Builder con máximo 1 alloc/op en casos estándar | **CUMPLIDO** | `BenchmarkEventBuilderSmall` reporta 1 alloc/op. |
| Suite de fuzzing robusta integrada | **CUMPLIDO** | Nuevos fuzz targets creados y pasados en verde. |

---

## Risks / Regressions
* **Regresión en Logs Grandes (1KB):** La nueva API estructurada tipada presenta una **regresión de latencia y de consumo de memoria** en mensajes grandes (como en `BenchmarkEventBuilder1KB`), debido a redimensionamientos dinámicos del búfer mediante operaciones `append` sucesivas de Go (10 allocations y 35 KB/op). El builder es idóneo para el caso común `small/mixed`, pero este comportamiento ineficiente en payloads masivos queda registrado como deuda técnica para ser corregido en la **Fase 03** (Encoder append-only) y la **Fase 05** (Pools de buffers estáticos).

---

## Private Evidence References
* `.private/bench/phase02_builder_bench_gmp1.txt`
* `.private/bench/phase02_builder_bench_gmp2.txt`
* `.private/reports/phase-02-private-evidence.md`

---

## Next Phase Recommendation
Se recomienda avanzar a la **Fase 03 — Encoder JSON append-only** para optimizar y unificar los pools de búferes, lo que permitirá mitigar el crecimiento dinámico de los búferes y consolidar un codificador centralizado de alto rendimiento para escenarios masivos.
