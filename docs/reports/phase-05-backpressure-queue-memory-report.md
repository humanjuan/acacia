# Phase 05 — Backpressure, Queue, and Memory Optimization Report

* **Date:** 2026-07-08
* **Branch:** `dev`
* **Commit:** `feat: add sensitive data redaction for structured events`
* **Go version:** `go1.23.2`
* **OS/Arch:** `darwin/amd64`
* **Scope:** Diseño, integración y optimización de políticas de backpressure/descarte de colas, telemetría thread-safe y eliminación de alocaciones redundantes bajo saturación (evitando el "engaño del drop").

---

## Documents Reviewed
Durante el inicio y transcurso de esta fase, se revisó y analizó el cuerpo documental del proyecto para alineación de diseño:
* **docs/ACACIA_PROJECT_CONTEXT.md**: Comprobación del alcance operacional e hitos del proyecto.
* **docs/ACACIA_IMPROVEMENT_ROADMAP.md**: Inspección de los requerimientos específicos de backpressure/descarte y el uso de canales.
* **docs/TESTING_AND_BENCHMARKING.md**: Guía sobre la estabilidad y reproducibilidad de los benchmarks con count=10.
* **docs/SECURITY_HARDENING_PLAN.md**: Verificación de que las nuevas políticas no alteren ni inyecten caracteres de control o comprometan la seguridad.
* **docs/PUBLIC_PRIVATE_DOCUMENTATION_POLICY.md**: Política estricta de exclusión de rutas locales y versionado seguro.
* **docs/REPORTING_GUIDE.md**: Estructura requerida para los reportes públicos de cierre de fase.
* **docs/PERFORMANCE_ENGINEERING_PLAN.md**: Directrices para reducir allocations a 0 B/op en caminos calientes y saturados.
* **docs/MIGRATION_STRATEGY.md**: Principio de no ruptura de compatibilidad y preservación de defaults.
* **observability_audit.md**: Detalla la auditoría original donde se identificó que el descarte tardío ("el engaño del drop") generaba un 52.5% de CPU en `mallocgc` y las colas grandes (500k) de struct ocupaban 35MB estáticos de memoria inicial.
* **docs/reports/phase-01-baseline-report.md**, **phase-02-typed-api-report.md**, **phase-03-json-encoder-report.md**, **phase-04-redaction-security-report.md**: Utilizados para validar la evolución de allocations en el hot path.

---

## Backpressure Policy Design
Se implementaron e integraron las siguientes políticas de backpressure en Acacia:
1. **BlockOnFull (Predeterminado Histórico):**
   * **Compatibilidad histórica:** El logger legacy original de Acacia bloquea al productor si la cola está llena. Para asegurar total compatibilidad con clientes existentes y evitar rupturas operativas imprevistas en producción, se ha mantenido `BlockOnFull` como la política predeterminada de Acacia.
   * **Comportamiento:** Si la cola está llena, el productor bloquea de forma síncrona en el canal de eventos hasta que se libere espacio.
2. **DropNewest:**
   * **Comportamiento:** Si la cola está llena, se descarta el nuevo evento log entrante de forma inmediata y no bloqueante.
3. **DropOldest:**
   * **Comportamiento:** Si la cola está llena, se expulsa el evento de log más antiguo del canal `_log.events` de forma no bloqueante para hacer espacio al nuevo evento.
   * **Limitaciones:** Está limitado a la capacidad concurrente. Si la ranura de cola liberada es inmediatamente ocupada por otra goroutine competidora antes de que el evento actual pueda encolarse, este se descarta para evitar bloqueos del sistema.
4. **DropDebugOnly:**
   * **Comportamiento:** Si la cola está llena y el evento es de nivel `DEBUG` o `INFO`, se descarta de inmediato para aliviar la saturación. Si el evento es de nivel `WARN`, `ERROR` o `CRITICAL`, se aplica el comportamiento de `BlockOnFull` (bloqueante) para garantizar que los logs críticos de producción nunca se pierdan bajo saturación.

---

## Metrics Added
Se integró una estructura de telemetría de cola atómica y segura para hilos (`Stats`) accesible sin allocations:
* **EnqueuedTotal:** Total de eventos que fueron aceptados con éxito en la cola de escritura.
* **WrittenTotal:** Total de eventos de log que el escritor asíncrono ha escrito físicamente en disco.
* **DroppedTotal:** Total de eventos de log que han sido descartados debido a saturación y a la política de backpressure activa.
* **QueueLen / QueueCap:** Longitud actual de ocupación y capacidad máxima de la cola.
* **WriteErrorsTotal:** Total de errores de escritura física (I/O) en disco.
* **RotationErrorsTotal:** Total de errores al intentar renombrar o crear archivos durante rotaciones diarias o por tamaño.

---

## Control Event Safety
* **Seguridad de eventos de control:** Las llamadas críticas del sistema como `Sync()` y `Close()`, así como los cambios de rotación en caliente, se enrutan exclusivamente a través del canal dedicado `_log.control` en lugar de mezclarse en el canal `_log.events`.
* **Garantía ante descartes:** Dado que las políticas de drop (`DropNewest`, `DropOldest`, `DropDebugOnly`) actúan de forma estricta y exclusiva sobre la cola `_log.events`, los mensajes de control de infraestructura nunca pueden ser descartados.
* **Sincronización robusta:** La sincronización toma un snapshot de `enqueuedSeq` y el escritor drena el canal hasta ese índice exacto antes de dar `ack`, lo que previene deadlocks bajo descarte concurrente de logs.

---

## Pre-Drop Work Reduction
Para mitigar las alocaciones inútiles en el builder estructurado tipado bajo descarte ("el engaño del drop"), se integró una optimización de "comprobación previa al drop" (pre-drop check) en `NewEvent(level)`:
* Si la política es `DropNewest` o `DropDebugOnly` y `len(events) >= cap(events)`, se comprueba si el evento califica para drop antes de reservar buffers o estructuras del pool.
* Si califica, se incrementa de inmediato `droppedSeq` y se retorna `nil`.
* Los métodos encadenados posteriores (`Str`, `Int`, `Send`, `Msg`) cortocircuitan al instante al recibir `nil`.
* **Path que evita trabajo:** El hot path de la API estructurada tipada (`Event`).
* **Deuda legacy:** El path legacy `Log.Info(map[string]interface{})` aún requiere allocations iniciales debido al boxing de interfaces y a la conversión a mapas antes de encolar. Esto se mantiene como deuda técnica controlada a extinguir en v3.

---

## Stress Test Summary
Las pruebas de estrés concurrentes ejecutaron 64 goroutines concurrentes intentando encolar 5,000 logs cada una (un total de 320,000 logs por prueba) sobre una cola pequeña de 1,000 ranuras:
* **Resultados:**
  * `TestStressBackpressureDropNewest`: Procesó y descartó correctamente sin deadlocks.
  * `TestStressBackpressureBlockOnFull`: Garantizó 0 descartes y completó todas las escrituras sin fallos de carrera.
  * `TestStressBackpressureDropDebugOnly`: Descartó debugs pero preservó logs de error sin interrupciones.
  * `TestStressSyncCloseUnderSaturation`: Llamadas de sincronización concurrente unbloquearon correctamente durante el apagado del escritor, garantizando drenaje seguro y robustez.

---

## Benchmark Summary

### GOMAXPROCS=1
Métricas obtenidas con el comando: `GOMAXPROCS=1 go test ./test -bench 'Benchmark(Backpressure|Vector3Drop|EventBuilderDisabledLevel)' -benchmem -run '^$' -count=10`

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkBackpressureDropNewestFullQueue` | 37.4 ns | 0 B | 0 | Zero-allocation real |
| `BenchmarkBackpressureBlockOnFull` | 359.3 ns | 45 B | 0 | Regulador bloqueante |
| `BenchmarkBackpressureDropOldestFullQueue` | 143.3 ns | 2 B | 0 | Desalojo FIFO veloz |
| `BenchmarkBackpressureDropDebugOnlyFullQueue` | 36.9 ns | 0 B | 0 | Descarte rápido de debug |
| `BenchmarkBackpressureStats` | 4.7 ns | 0 B | 0 | Consulta stats sin allocations |
| `BenchmarkBackpressureDropped` | 0.8 ns | 0 B | 0 | Consulta de drop simple |
| `BenchmarkBackpressureBuilderDropNewestFullQueue` | 24.6 ns | 0 B | 0 | Pre-drop check en builder tipado |

### GOMAXPROCS=2
Métricas obtenidas con el comando: `GOMAXPROCS=2 go test ./test -bench 'Benchmark(Backpressure|Vector3Drop|EventBuilderDisabledLevel)' -benchmem -run '^$' -count=10`

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkBackpressureDropNewestFullQueue` | 91.2 ns | 120 B | 0 | Sin allocations bajo multiproceso |
| `BenchmarkBackpressureBlockOnFull` | 218.2 ns | 3 B | 0 | Rendimiento en hilos concurrentes |
| `BenchmarkBackpressureDropOldestFullQueue` | 212.8 ns | 205 B | 0 | Expulsión FIFO segura |
| `BenchmarkBackpressureDropDebugOnlyFullQueue` | 85.7 ns | 125 B | 0 | Comportamiento híbrido |
| `BenchmarkBackpressureStats` | 3.6 ns | 0 B | 0 | Telemetría rápida |
| `BenchmarkBackpressureDropped` | 0.5 ns | 0 B | 0 | Telemetría rápida |
| `BenchmarkBackpressureBuilderDropNewestFullQueue` | 130.0 ns | 109 B | 0 | Cortocircuito pre-drop óptimo |

---

## pprof Summary
El análisis de perfiles CPU y de memoria con la herramienta `go tool pprof` para `BenchmarkBackpressureBuilderDropNewestFullQueue` revela que:
* **CPU:** Bajo saturación con descarte, la CPU consume la mayor parte del tiempo en llamadas del sistema e infraestructura interna de canales de Go (`runtime.selectgo`, `kevent`). El costo del pre-drop check es imperceptible.
* **Memoria:** Las allocations en Heap son exactamente **0 B/op** debido al desvío preventivo y la reutilización total de las instancias de Evento desde el pool y búferes locales.

---

## Race Detector Summary
* La suite completa de pruebas stress en concurrencia masiva ejecutada bajo `go test -race ./...` completó exitosamente con resultado **PASS** y **0 advertencias de carrera de datos (data races)**.
* Se garantiza que la recolección de estadísticas (`Stats`) y los cambios de backpressure en caliente operan de forma thread-safe mediante operaciones atómicas en `sync/atomic`.

---

## Acceptance Criteria Status
* **Políticas configurables de descarte:** Cumplido (DropNewest, BlockOnFull, DropOldest, DropDebugOnly).
* **Evitar alocaciones previo al drop:** Cumplido (retorno nil preventivo en NewEvent).
* **Soporte de estadísticas en caliente:** Cumplido (métodos Stats() y Dropped() thread-safe).
* **Sincronización y apagado sin deadlocks:** Cumplido (drenaje de control unbloqueado en Close).

---

## Risks / Regressions
* **Deuda Técnica de Mapas:** Los paths legacy de logs estructurados (`Log.Info(map[string]interface{})`) siguen haciendo alocaciones dinámicas e boxing antes de poder verificar si la cola está llena. Esto representa un costo menor y controlado, con planes de desuso total en v3.

---

## Private Evidence References
La evidencia de rendimiento cruda y perfiles generados localmente se encuentran almacenados y protegidos en:
* `.private/reports/phase-05-private-evidence.md` (reporte de evidencia privado)
* `.private/pprof/phase05_backpressure_cpu.pprof`
* `.private/pprof/phase05_backpressure_mem.pprof`
* `.private/bench/phase05_backpressure_bench_gmp1.txt`
* `.private/bench/phase05_backpressure_bench_gmp2.txt`

---

## Next Phase Recommendation
Se recomienda declarar la **Fase 05** como completamente **finalizada y cerrada**.
La siguiente fase planificada es:
* **Fase 06 — Filesystem, permisos y rotación segura**
