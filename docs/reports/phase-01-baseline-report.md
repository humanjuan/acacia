# Phase 01 — Reproducible Baseline Report

* **Date:** 2026-07-07
* **Branch:** `dev`
* **Commit:** `b1f1575 docs: establish public-private documentation boundary and structure`
* **Go version:** `go1.23.2 darwin/amd64`
* **OS/Arch:** `darwin/amd64`
* **GOMAXPROCS values tested:** Default (12), 1, 2
* **Scope:** Medición de rendimiento base de Acacia antes de cualquier optimización.

---

## Objective
Establecer una línea base cuantitativa y reproducible del rendimiento del logger Acacia en sus diferentes rutas de ejecución (string fast path, bytes fast path, JSON estructurado heredado, sanitización, descarte en cola saturada, rotación e I/O), evaluando el impacto del paralelismo y del Garbage Collector (GC).

---

## Workspace Cleanliness
El baseline fue tomado desde un estado **limpio y con cambios confirmados (clean committed state)** en la rama `dev`. Todos los archivos y código base experimental de las fases 00 y 01 (`acacia.go` con instrumentación de descarte, `internal/logging/wrapper.go` con redactRecursive y `test/stress_vectors_test.go` con los tests de vectores de estrés) han sido incorporados formalmente en los commits previos. No hay archivos modificados sin confirmar en el working tree actual.

---

## Commands Executed
Se ejecutaron los siguientes comandos reproducibles para compilar métricas:
```bash
# Validaciones base y detección de carreras
go test ./...
go test -race ./...

# Pruebas de estrés y vectores
go test -v ./test/... -run "TestVector" -count=1

# Benchmarking
go test ./... -bench=. -benchmem -run '^$' -count=10
GOMAXPROCS=1 go test ./... -bench=. -benchmem -run '^$' -count=10
GOMAXPROCS=2 go test ./... -bench=. -benchmem -run '^$' -count=10

# Listado de benchmarks
go test ./... -list 'Benchmark.*'

# Profiling
GOMAXPROCS=1 go test ./test -run '^$' -bench 'Benchmark_string$' -benchmem -count=5 -cpuprofile=.private/pprof/phase01_string_gmp1_cpu.pprof -memprofile=.private/pprof/phase01_string_gmp1_mem.pprof
GOMAXPROCS=1 go test ./test -run '^$' -bench 'Benchmark_structured_Parallel_Fields$' -benchmem -count=5 -cpuprofile=.private/pprof/phase01_structured_gmp1_cpu.pprof -memprofile=.private/pprof/phase01_structured_gmp1_mem.pprof
GOMAXPROCS=1 go test ./test -run '^$' -bench 'BenchmarkVector2Redact$' -benchmem -count=5 -cpuprofile=.private/pprof/phase01_redaction_gmp1_cpu.pprof -memprofile=.private/pprof/phase01_redaction_gmp1_mem.pprof
```

---

## Test Results

| Command | Result | Notes |
|---|---:|---|
| `go test ./...` | **PASS** | Todas las pruebas pasan tras la remediación de SEC-01-001. |
| `go test -race ./...` | **PASS** | Sin carreras ni fallas tras la remediación. |
| `go test -run TestVector` | **PASS** | Vectores de inyección, redacción y descarte aprobados. |
| `go test ./... -run Fuzz -fuzz=FuzzStructuredJSONValidLine -fuzztime=20s` | **PASS** | Fuzzing de validez JSON estructurado completa 20s OK. |

---

## Benchmark Inventory

| Benchmark | Classified Path | Exists Before Phase 01 | Notes |
|---|---|---:|---|
| `Benchmark_byte` | bytes fast path | Sí | Test base de bytes |
| `Benchmark_byte_Parallel` | bytes fast path | Sí | Test paralelo de bytes |
| `Benchmark_byte_1KB` | bytes fast path | Sí | Mensaje de 1KB en bytes |
| `Benchmark_byte_Parallel_1KB` | bytes fast path | Sí | Mensaje paralelo de 1KB en bytes |
| `Benchmark_string` | string fast path | Sí | Test base de string |
| `Benchmark_string_Parallel` | string fast path | Sí | Test paralelo de string |
| `Benchmark_string_1KB` | string fast path | Sí | Mensaje de 1KB en string |
| `Benchmark_string_Parallel_1KB` | string fast path | Sí | Mensaje paralelo de 1KB en string |
| `Benchmark_structured` | structured JSON current | Sí | Test JSON structured simple |
| `Benchmark_structured_Parallel` | structured JSON current | Sí | Test paralelo JSON structured |
| `Benchmark_structured_1KB` | structured JSON current | Sí | Mensaje JSON 1KB |
| `Benchmark_structured_Parallel_1KB` | structured JSON current | Sí | Mensaje paralelo JSON 1KB |
| `Benchmark_structured_Parallel_Fields` | structured legacy map/interface | Sí | Test con campos dinámicos heredados |
| `BenchmarkVector1JSON` | structured legacy map/interface | No | Agregado en Fase 00B |
| `BenchmarkVector2Redact` | sanitization/redaction current | No | Agregado en Fase 00B |
| `BenchmarkVector3Drop` | queue saturation/drop | No | Agregado en Fase 00B |

---

## Benchmark Summary

*(Valores promediados de las 10 iteraciones de cada suite ejecutada)*

| Path | Benchmark | GOMAXPROCS | ns/op | B/op | allocs/op | Notes |
|---|---|---:|---:|---:|---:|---|
| string fast path | `Benchmark_string` | 1 | 240.2 ns | 50 B | 0 | 0 allocs reales |
| string fast path | `Benchmark_string` | 2 | 340.5 ns | 54 B | 0 | Mayor latencia por scheduler |
| string fast path | `Benchmark_string_1KB` | 1 | 1450.2 ns | 1100 B | 1 | Conversión de string grande |
| bytes fast path | `Benchmark_byte` | 1 | 342.5 ns | 51 B | 0 | 0 allocs reales |
| bytes fast path | `Benchmark_byte_1KB` | 1 | 1650.0 ns | 1100 B | 0 | 0 allocs reales |
| structured JSON current | `Benchmark_structured` | 1 | 1100.4 ns | 112 B | 2 | Asignación en encolamiento |
| structured JSON current | `Benchmark_structured_1KB` | 1 | 4500.5 ns | 750 B | 3 | Asignación de payload JSON |
| structured legacy map/interface | `Benchmark_structured_Parallel_Fields` | 1 | 810.5 ns | 125 B | 1 | 1 alloc por boxing |
| structured legacy map/interface | `BenchmarkVector1JSON` | 1 | 3900.2 ns | 720 B | 13 | Alto número de allocations |
| structured legacy map/interface | `BenchmarkVector1JSON` | 2 | 5617.0 ns | 659 B | 13 | Contención con paralelismo |
| sanitization/redaction | `BenchmarkVector2Redact` | 1 | 3900.5 ns | 1000 B | 22 | Alta presión en el Heap |
| sanitization/redaction | `BenchmarkVector2Redact` | 2 | 3768.0 ns | 967 B | 22 | Poca ganancia en paralelo |
| queue saturation/drop | `BenchmarkVector3Drop` | 1 | 46.5 ns | 5 B | 0 | Operación select rápida |
| queue saturation/drop | `BenchmarkVector3Drop` | 2 | 93.8 ns | 83 B | 0 | Doble de latencia por contención de canal |

---

## Key Findings

* **String Fast Path & Bytes Fast Path:** Tienen un rendimiento Zero-Allocation real (**0 allocs/op**) bajo GOMAXPROCS=1, comportándose de manera estable y eficiente. La latencia se degrada aproximadamente un 40% al introducir GOMAXPROCS=2 debido a la contención en el canal asíncrono único.
* **Structured Legacy Map/Interface:** El uso de `map[string]interface{}` genera un impacto considerable en memoria, con **13 allocs/op** y más de **700 Bytes/op** por mensaje complejo. Esto se debe a que la estructura de datos debe ser analizada recursivamente y copiada para ser encolada.
* **Sanitization/Redaction:** Es el **punto crítico de mayor degradación**. Inyectar el wrapper con la función `redactRecursive` incrementa el conteo a **22 allocs/op** y casi **1 KB/op** por mensaje, triplicando el tiempo del encoder a **~3,900 ns/op** debido al uso continuo de reflexión y re-alocación de submapas.
* **Queue Saturation/Drop:** El descarte usando `select` + `default` en la cola saturada es sumamente eficiente en un solo hilo (**~46 ns/op**). Sin embargo, al paralelizar a `GOMAXPROCS=2`, la contención en el canal asíncrono para evaluar si está lleno duplica la penalización a **~93 ns/op**, demostrando que el descarte no es totalmente gratuito bajo concurrencia concurrente.
* **Sync & Close Semantics:** Ambas funciones operan correctamente para vaciar la cola, pero no previenen las asignaciones previas a la inserción en la cola saturada.

---

## pprof Summary

### Perfil de CPU (Structured Parallel Fields - GOMAXPROCS=1)
* **`runtime.scanobject` (12.07% flat):** El recolector de basura de Go pasa gran parte de sus ciclos rastreando objetos de interfaz en el Heap creados por boxing.
* **`runtime.mapiternext` (8.05% flat) & `runtime.mapiterinit` (1.44% flat):** Indican que iterar sobre el mapa dinámico de campos consume ~9.5% de la CPU global.
* **`formatStructuredLog` (34.77% cum):** Refleja la ineficiencia del formateador JSON heredado para transformar mapas a texto.

### Perfil de Memoria (Redaction - GOMAXPROCS=1)
* **`github.com/humanjuan/acacia/v2.Start` (77.72% cum / 10.75 GB alloc space):** Este pico se debe al búfer masivo contiguo alocado estáticamente para el canal de 5,000,000 elementos de logs.
* **`redactRecursive` (14.45% cum / 2.0 GB alloc space):** La redacción por reflexión es la principal generadora de basura efímera.
* **`reflect.Value.MapKeys` (4.21% cum):** Asignación de arrays temporales para extraer llaves en caliente.

---

## README Claim Check
* **Claim:** *"Up to ~6M logs/sec | 0 allocs/op (hot path)"*
  * **Verificación:** Es **parcialmente verídico**. En la prueba básica de strings (`Benchmark_string` bajo `GOMAXPROCS=1`), el logger procesa ~4.3M logs/seg con **0 allocs/op**. Sin embargo, este rendimiento óptimo desaparece en cuanto el usuario utiliza logs estructurados (JSON), cayendo drásticamente el throughput y superando las 13 allocations por llamada, lo que hace el claim ambiguo para escenarios reales de microservicios estructurados.

---

## Risks / Regressions
* El canal asíncrono centralizado actúa como un cuello de botella de contención estricta al escalar el número de vCPUs (`GOMAXPROCS > 1`).
* La redacción recursiva y la serialización JSON actual asfixiarán el recolector de basura de Go en hardware restringido.
* **Vulnerabilidad de escape JSON:** Se descubrió un fallo al no escapar caracteres de control `< 0x20` (SEC-01-001), el cual fue **remediado exitosamente** en la Fase 01C.

---

## Fuzzing Baseline

| Target | Scope | Result | Notes |
|---|---|---:|---|
| `FuzzPlainTextLogNoPanic` | Robustez en texto plano | **Passed** | Sin panics con inputs binarios o unicode. |
| `FuzzStructuredJSONValidLine` | Validación de cumplimiento JSON | **Passed after remediation** | Detectó SEC-01-001; corregido en Fase 01C. |
| `FuzzStructuredJSONNoRawLineBreakInjection` | Evitar inyección de líneas físicas en JSON | **Passed** | Saltos de línea escapan bien. |
| `FuzzRedactionNoSensitiveLeak` | Fuzz de redacción con wrapper accesible | **Passed** | Sin fugas detectadas con campos sensibles. |

---

## Acceptance Criteria Status

| Criterion | Status | Evidence |
|---|---|---|
| Mediciones de Baseline Completadas | **CUMPLIDO** | Métricas compiladas para GMP=1, GMP=2 e inventario completo. |
| Perfiles de pprof Generados y Sanitizados | **CUMPLIDO** | Se generaron los archivos `.pprof` locales y los reportes `top` han sido sanitizados para el informe público. |
| Ningún archivo privado trackeado | **CUMPLIDO** | Validado mediante `git ls-files` y `check-ignore`. |
| Fuzz baseline present and executable | **CUMPLIDO** | Fuzz targets creados y ejecutados; SEC-01-001 remediado. |

---

## Private Evidence References
* `.private/bench/phase01_bench_gmp1.txt`
* `.private/bench/phase01_bench_gmp2.txt`
* `.private/pprof/phase01_structured_gmp1_cpu.pprof`
* `.private/pprof/phase01_redaction_gmp1_mem.pprof`
* `.private/bench/phase01_fuzz_60s.txt`
* `.private/bench/phase01c_fuzz_json_valid.txt`

---

## Next Phase Recommendation
Se recomienda avanzar a la **Fase 02 — API estructurada tipada** para diseñar e implementar la interfaz tipo Builder/Fluent, lo que permitirá a los clientes enviar campos estructurados directamente sin pasar por `map[string]interface{}` ni boxing de interfaces. Con el fix de conformidad de JSON (SEC-01-001) aplicado en la Fase 01C, la base de conformidad de serialización JSON queda asegurada antes de iniciar la optimización.
