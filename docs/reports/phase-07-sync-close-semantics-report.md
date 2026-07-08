# Phase 07 — Sync and Close Formal Semantics Report

Date: 2026-07-08
Branch: `dev`
Commit: `a8843fb`
Go version: `go1.23.2`
OS/Arch: `darwin/amd64`
Scope: Formalización de la semántica de sincronización (`Sync()`) y cierre (`Close()`) del logger asíncrono Acacia.

---

## Objective
El objetivo de la Fase 07 es definir, estructurar e implementar una semántica formal de sincronización determinista (`Sync()`) y apagado seguro (`Close()`) para Acacia, eliminando deadlocks, previniendo excepciones por inyección de logs post-cierre y garantizando la entrega de todos los eventos de log que han sido aceptados antes de la invocación de estas barreras.

---

## Documents Reviewed
* **docs/ACACIA_PROJECT_CONTEXT.md**
* **docs/ACACIA_IMPROVEMENT_ROADMAP.md**
* **docs/TESTING_AND_BENCHMARKING.md**
* **docs/SECURITY_HARDENING_PLAN.md**
* **docs/PUBLIC_PRIVATE_DOCUMENTATION_POLICY.md**
* **docs/REPORTING_GUIDE.md**
* **docs/PERFORMANCE_ENGINEERING_PLAN.md**
* **docs/MIGRATION_STRATEGY.md**
* **docs/reports/phase-06-filesystem-permissions-rotation-report.md**

---

## Scope Decision
* **Dentro de Alcance:**
  * Máquina de estados atómica (`stateOpen`, `stateClosing`, `stateClosed`).
  * No cerrar físicamente el canal `events` para evitar panics de escritura concurrentes.
  * Canal de control dedicado (`control`) y protocolo estructurado de cierre (`isClose: true`).
  * Bloqueos en productores liberados de forma determinista mediante el cierre del canal `done`.
  * Drenaje completo asíncrono y fsync del archivo antes del apagado.
  * Suite de pruebas unitarias, de pérdida cero, de estrés y de carrera concurrentes para Sync/Close.
* **Fuera de Alcance:**
  * Rediseño de políticas generales de rotación de archivos.
  * Rediseño de la infraestructura interna de asincronía y pooling de buffers JSON.

---

## Formal Sync Semantics
* `Sync()` actúa como una barrera estricta que captura el snapshot de la secuencia encolada (`enqueuedSeq`).
* Espera hasta que el escritor asíncrono procese y escriba todos los eventos hasta dicha secuencia inclusive, ejecutando un `Sync()` (fsync) físico en el descriptor.
* Si el logger se encuentra cerrado o en proceso de cierre, `Sync()` retorna de inmediato como un no-op seguro para evitar bloqueos.
* Es concurrente y reentrante (múltiples goroutines pueden llamarlo simultáneamente).

---

## Formal Close Semantics
* `Close()` es completamente idempotente, controlado a través de una instancia `sync.Once`.
* Transiciona de forma atómica el estado del logger a `stateClosing`, impidiendo la aceptación de nuevos logs.
* Cierra el canal `done`, liberando instantáneamente a todos los productores que estuviesen bloqueados bajo la política `BlockOnFull`.
* Envía un evento de control especial al escritor asíncrono para notificarle que debe iniciar el apagado ordenado.
* El escritor procesa los elementos remanentes encolados en `events` hasta vaciar la cola por completo.
* Finalmente, se realiza un `flush()` del búfer interno, se ejecuta el fsync en el descriptor del archivo y se cierra el descriptor físico.
* Al finalizar el drenaje de hilos en `wg.Wait()`, el estado se establece en `stateClosed`.

---

## Post-Close Behavior
* **`Sync()` después de `Close()`:** No-op instantáneo (retorna de inmediato sin bloquear).
* **`Close()` después de `Close()`:** No-op instantáneo idempotente protegido por `sync.Once`.
* **Escrituras de Logs (`Info`, `Warn`, `Error`, `Debug`, `Write`, `InfoBytes`, `Event().Send()`) después de `Close()`:**
  * No bloquean en ningún caso.
  * No lanzan excepciones de tipo `panic: send on closed channel`.
  * Incrementan el contador atómico de descartes (`DroppedTotal`) y limpian inmediatamente los buffers reutilizables del pool.

---

## Control Event Protocol
* Las peticiones de control se componen a través de la estructura `controlReq` con canales de confirmación (`ack`).
* El escritor es la única entidad responsable de leer `control` y realizar `close(ack)`.
* Durante la fase de cierre (`isClose: true`), el escritor realiza una limpieza final drenando de forma exhaustiva los canales de control pendientes para garantizar que no existan hilos clientes bloqueados.

---

## Zero-Loss Guarantee
* **Attempted Event:** Un evento enviado por el cliente que intenta ser procesado.
* **Accepted Event:** Evento que supera la validación de estado y es insertado exitosamente en el canal asíncrono `events`.
* **Dropped Event:** Evento descartado debido a saturación de políticas o estado de apagado.
* **Written Event:** Evento transferido con éxito al almacenamiento persistente.
* **Garantía:** Todo evento **Accepted** previo al inicio de `Sync()` o `Close()` tiene entrega garantizada (cero pérdidas) en el archivo físico.

---

## Public API Impact
* No hay cambios ni roturas de firmas públicas de métodos existentes de Acacia.
* Los métodos `Sync()` y `Close()` mantienen sus firmas de v2.x.
* El método `Status()` pasa a reportar el estado exacto en base al ciclo de vida del logger (`stateOpen`).

---

## Files Changed
* `acacia.go`
* `test/phase07_sync_close_test.go`
* `test/phase07_sync_close_stress_test.go`
* `test/phase07_zero_loss_test.go`
* `test/phase07_sync_close_benchmark_test.go`

---

## Tests Executed

| Command | Result | Notes |
|---|---:|---|
| `go test ./...` | **PASS** | Ejecución de suite unitaria completa |
| `go test -race ./...` | **PASS** | Validación del detector de carreras en todo el repo |
| `go test ./test -run 'Test.*ZeroLoss' -count=1` | **PASS** | Pruebas de pérdida cero en Sync y Close |
| `go test ./test -run TestStressSyncClose -count=1` | **PASS** | Stress concurrent de Sync y Close |

---

## Zero-Loss Test Summary

| Test | Result | Notes |
|---|---:|---|
| `TestZeroLossBeforeSync` | **PASS** | N logs escritos previo a Sync aparecen íntegros |
| `TestZeroLossBeforeClose` | **PASS** | N logs escritos previo a Close aparecen íntegros |
| `TestZeroLossConcurrentAcceptedBeforeClose` | **PASS** | Logrado en concurrencia masiva sin pérdidas |
| `TestDropPoliciesNoPromise` | **PASS** | DroppedTotal + EnqueuedTotal igual a intentos |

---

## Stress / Race Summary

| Test | Result | Notes |
|---|---:|---|
| `TestStressSyncCloseConcurrentProducers` | **PASS** | 32 productores concurrentes escribiendo durante Sync/Close |
| `TestStressSyncCloseBlockOnFull` | **PASS** | 64 escritores bloqueados liberados de inmediato sin deadlock |
| `TestStressSyncCloseDropNewest` | **PASS** | Estabilidad asíncrona bajo descartes e intervalos de Sync |
| `TestStressSyncCloseDropOldest` | **PASS** | Estabilidad asíncrona bajo desalojo de cabecera en cola |
| `TestStressSyncCloseDropDebugOnly` | **PASS** | Pruebas de prioridad de eventos bajo cierre asíncrono |
| `TestStressSyncCloseRotation` | **PASS** | Escritores pesados rotando y cerrando concurrentemente |
| `TestStressRepeatedClose` | **PASS** | 50 goroutines llamando Close simultáneamente sin pánicos |

---

## Fuzzing Regression Summary

| Target | Result | Notes |
|---|---:|---|
| `FuzzStructuredJSONValidLine` | **PASS** | Estabilidad en parseo estructurado legacy |
| `FuzzEventBuilderJSONValid` | **PASS** | Estabilidad en el generador tipado JSON |
| `FuzzRedactionSensitiveKeysNoLeak` | **PASS** | Redacción automática de secretos sin regresión |
| `FuzzValidateLogFilenameNoTraversal` | **PASS** | Bloqueo persistente de path traversal |

---

## Benchmark Summary

### GOMAXPROCS=1
| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkSyncEmpty` | 1180 | 112 | 1 | Barrera básica |
| `BenchmarkSyncWithBufferedEvents` | 7406 | 128 | 2 | Sincronización intermedia |
| `BenchmarkCloseEmpty` | 22,000,000 | 808 | 10 | I/O completa del disco |
| `BenchmarkCloseWithBufferedEvents` | 20,000,000 | 824 | 11 | I/O asíncrona completa |
| `BenchmarkWriteAfterClose` | 17.1 | 0 | 0 | Cortocircuito atómico instantáneo |
| `BenchmarkSyncAfterClose` | 0.66 | 0 | 0 | Retorno inmediato optimizado |
| `BenchmarkConcurrentSync` | 1192 | 112 | 1 | Sincronía paralela |
| `BenchmarkCloseUnderBlockOnFull` | 23,000,000 | 808 | 10 | Liberación concurrente y cierre |

### GOMAXPROCS=2
| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkSyncEmpty` | 1104 | 112 | 1 | Barrera básica |
| `BenchmarkSyncWithBufferedEvents` | 7787 | 128 | 2 | Sincronización intermedia |
| `BenchmarkCloseEmpty` | 21,000,000 | 858 | 10 | I/O completa del disco |
| `BenchmarkCloseWithBufferedEvents` | 21,000,000 | 786 | 10 | I/O asíncrona completa |
| `BenchmarkWriteAfterClose` | 18.9 | 0 | 0 | Cortocircuito atómico instantáneo |
| `BenchmarkSyncAfterClose` | 0.62 | 0 | 0 | Retorno inmediato optimizado |
| `BenchmarkConcurrentSync` | 1344 | 112 | 1 | Sincronía paralela |
| `BenchmarkCloseUnderBlockOnFull` | 20,000,000 | 807 | 10 | Liberación concurrente y cierre |

---

## Security / Reliability Findings
* **SEC-07-001 (Deadlock en BlockOnFull durante Close):** Remediado.
* **SEC-07-002 (Panic por escritura post-cierre):** Remediado.
* **SEC-07-003 (Pérdida de logs aceptados en Close):** Remediado.

---

## Acceptance Criteria Status

| Criterion | Status | Evidence |
|---|---|---|
| Cierre idempotente y concurrente | **Completo** | `TestConcurrentClose`, `TestStressRepeatedClose` |
| Bloqueo en productores mitigado | **Completo** | `TestBlockOnFullClose`, `TestStressSyncCloseBlockOnFull` |
| Sin panics post-cierre | **Completo** | `TestWriteAfterClose`, `BenchmarkWriteAfterClose` |
| Drenaje total y cero pérdidas | **Completo** | `TestZeroLossBeforeClose`, `TestZeroLossConcurrentAcceptedBeforeClose` |
| Sync concurrente y de barrera | **Completo** | `TestConcurrentSync`, `TestZeroLossBeforeSync` |

---

## Risks / Regressions
* El drenaje síncrono del buffer físico puede ralentizar las llamadas de `Close()` en sistemas bajo alta latencia I/O de disco (por ejemplo, redes compartidas o almacenamiento en la nube), lo cual es el comportamiento correcto esperado para evitar pérdidas de información.

---

## Private Evidence References
* `.private/reports/phase-07-private-evidence.md`
* `.private/bench/phase07_go_test.txt`
* `.private/bench/phase07_go_test_race.txt`
* `.private/bench/phase07_stress_sync_close.txt`
* `.private/bench/phase07_stress_sync_close_gmp1.txt`
* `.private/bench/phase07_stress_sync_close_race.txt`
* `.private/bench/phase07_zero_loss_final.txt`
* `.private/bench/phase07_final_bench_gmp1.txt`
