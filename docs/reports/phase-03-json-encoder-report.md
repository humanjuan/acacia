# Phase 03 — Append-Only JSON Encoder Report

* **Date:** 2026-07-07
* **Branch:** `dev`
* **Commit:** `d3ad0ac feat: implement typed structured event builder and add comprehensive testing`
* **Go version:** `go1.23.2 darwin/amd64`
* **OS/Arch:** `darwin/amd64`
* **Scope:** Optimización del motor de serialización JSON estructurado mediante un codificador manual append-only y pooling dinámico.

---

## Objective
Optimizar y consolidar la serialización de JSON estructurado dentro del hot path de la API builder tipada de Acacia, reduciendo drásticamente las alocaciones de memoria y el tiempo de CPU en mensajes pequeños, medianos y de gran tamaño (1KB+), eliminando regresiones identificadas en hitos anteriores.

---

## Scope Decision
* **Optimizado:** El motor de codificación de strings y escapes de JSON (`escapeJSONInto`), así como el control de crecimiento y pooling del búfer de bytes (`ensureJSONCapacity`) del hot path estructurado de `Event`.
* **Mantener sin cambios:** La API pública del constructor de eventos, la lógica asíncrona de escritura/enrutado y las firmas de llamadas del path legacy, preservando compatibilidad retroactiva completa.

---

## Encoder Design
* **Codificación Append-only:** Se implementó una escritura manual directa sin recurrir a paquetes reflexivos como `reflect` ni a codificadores dinámicos como `encoding/json.Marshal`. Se usan funciones nativas rápidas como `strconv.AppendInt`, `strconv.AppendFloat` y `time.AppendFormat`.
* **Escapado de Cuerdas Eficiente:** En `escapeJSONInto` se realiza una estimación y pre-alocación al principio del recorrido. Esto asegura que los caracteres comunes sin escapar se escriban de manera secuencial sin llamadas de comprobación o crecimiento de memoria adicionales.
* **Estrategia de Búfer Dinámico Reutilizable:** Mediante `ensureJSONCapacity`, cuando el búfer de bytes supera su capacidad actual:
  1. Se solicita un nuevo búfer con el doble de capacidad al pool de Acacia (`getJSONBufCap`).
  2. Se copia el contenido acumulado.
  3. Se retorna el búfer viejo inmediatamente al pool correspondiente (`putJSONBuf`).
  Esto reduce las alocaciones y el crecimiento explosivo del búfer al reutilizar buckets internos de Acacia. Sin embargo, benchmem todavía reporta alocaciones de control asociadas a sync.Pool/type assertions, especialmente en saltos de capacidad.

---

## Files Changed
* **acacia.go**: Modificación de `escapeJSONInto` para integrar pre-alocación óptima, inserción de `ensureJSONCapacity`, y actualización de todos los métodos builder de `Event` para escribir con capacidad garantizada.
* **test/phase03_json_encoder_test.go**: Suite de pruebas unitarias para el codificador manual (tipos primitivos, escapes, redacción de secretos y payloads grandes).
* **test/phase03_json_encoder_benchmark_test.go**: Suite de benchmarks para evaluar latencia y consumo de memoria del codificador.
* **test/phase03_json_encoder_fuzz_test.go**: Suite de fuzzing para evaluar robustez ante escapes, campos mixtos y entradas masivas.

---

## Public API Impact
* **Ninguno.** No se rompe la compatibilidad pública de v2.x. La API builder estructurada tipada mantiene exactamente sus firmas previas.

---

## Tests Executed

| Command | Result | Notes |
|---|---:|---|
| `go test ./...` | **PASS** | Todas las pruebas pasan exitosamente. |
| `go test -race ./...` | **PASS** | Sin data races detectados. |

---

## Fuzzing Summary

| Target | Result | Notes |
|---|---:|---|
| `FuzzJSONEncoderStringEscaping` | **PASS** | Resistencia del codificador ante escapes y control bytes. |
| `FuzzJSONEncoderBuilderMixedFields` | **PASS** | Comportamiento seguro ante diferentes combinaciones. |
| `FuzzJSONEncoderLargePayload` | **PASS** | Conformance de validez JSON ante payloads masivos de hasta 16KB. |

---

## Benchmark Summary

### GOMAXPROCS=1
| Benchmark | ns/op | B/op | allocs/op | Compared To | Notes |
|---|---:|---:|---:|---|---|
| `BenchmarkJSONEncoderDisabledLevel` | 19.3 ns | 0 B | 0 | `Fase 02` | Sin alocaciones |
| `BenchmarkJSONEncoderBuilderSmall` | 723.4 ns | 292 B | 1 | `Fase 02` | Retorno rápido |
| `BenchmarkJSONEncoderBuilderMixed` | 770.0 ns | 255 B | 1 | `Fase 02` | Mantiene alocación única |
| `BenchmarkJSONEncoderBuilder1KB` | 13552 ns | 17273 B | 4 | `Fase 02 (10 allocs)` | Reducción de alocaciones |
| `BenchmarkJSONEncoderBuilder4KB` | 48593 ns | 76137 B | 6 | - | Deuda de pre-alocación |
| `BenchmarkJSONEncoderBuilder8KB` | 132953 ns | 151260 B | 7 | - | Deuda de pre-alocación |

### GOMAXPROCS=2
| Benchmark | ns/op | B/op | allocs/op | Compared To | Notes |
|---|---:|---:|---:|---|---|
| `BenchmarkJSONEncoderDisabledLevel` | 23.3 ns | 0 B | 0 | `Fase 02` | Descarte ultra rápido |
| `BenchmarkJSONEncoderBuilderSmall` | 701.3 ns | 271 B | 1 | `Fase 02` | Alocación de sync.Pool interface |
| `BenchmarkJSONEncoderBuilderMixed` | 734.9 ns | 272 B | 1 | `Fase 02` | 1 alloc única |
| `BenchmarkJSONEncoderBuilder1KB` | 13884 ns | 9771 B | 4 | `Fase 02 (10 allocs)` | 2.5x reducción de alocaciones |
| `BenchmarkJSONEncoderBuilder4KB` | 57420 ns | 77950 B | 6 | - | Crecimiento controlado |
| `BenchmarkJSONEncoderBuilder8KB` | 162102 ns | 153884 B | 7 | - | Crecimiento controlado |

---

## 1KB / Large Payload Analysis

Comparación directa en `BenchmarkJSONEncoderBuilder1KB` (Fase 02 vs Fase 03):

| Fase | Latencia (ns/op) | Alocaciones (allocs/op) | Consumo Memoria (B/op) |
|---|---:|---:|---:|
| **Fase 02** (Append clásico de Go) | ~16,588 ns | 10 | ~31,817 B |
| **Fase 03** (Encoder Pooling Append-only) | **~13,884 ns** | **4** | **~9,771 B** |
| **Mejora** | **16% de aceleración** | **60% menos alocaciones** | **69% menos uso de memoria** |

*Nota:* Las 4 alocaciones en el caso de 1KB se derivan de la interfaz retornada al extraer búferes de los buckets del pool en los saltos de crecimiento. Sin embargo, el consumo en bytes disminuyó 3.2x debido a la reutilización inmediata de la memoria liberada por los búferes viejos.

---

## pprof Summary
El análisis del perfilador de CPU para el benchmark de 1KB indica que:
* Las operaciones de copia y movimiento de bytes (`runtime.memmove`) consumen el 19.66% de CPU total.
* La serialización de strings y caracteres especiales (`escapeJSONInto`) representa el 19.26% de CPU.
* El gestor de capacidad y re-pooling de búferes (`ensureJSONCapacity`) consume el 9.13% de la CPU.
* El resto de la CPU se distribuye en operaciones de recolección (`runtime.scanobject` y `runtime.mallocgc`) derivadas de la contención por el tipo genérico de retorno de `sync.Pool`.

---

## Hot Path Restrictions Check
El hot path del nuevo encoder/builder estructurado tipado:
* **¿Utiliza reflect?** **NO**.
* **¿Utiliza fmt.Sprint/fmt.Sprintf?** **NO**.
* **¿Utiliza encoding/json.Marshal?** **NO**.
* **¿Utiliza map[string]interface{}?** **NO**.

---

## Acceptance Criteria Status

| Criterion | Status | Evidence |
|---|---|---|
| Encoder estructurado manual integrado | **CUMPLIDO** | Métodos optimizados implementados en `acacia.go`. |
| Disabled level en 0 allocs/op | **CUMPLIDO** | `BenchmarkJSONEncoderDisabledLevel` reporta 0 allocs/op. |
| Small/Mixed builder en <= 1 alloc/op | **CUMPLIDO** | Ambos reportan exactamente 1 alloc/op. |
| Reducción drástica de alocaciones en 1KB | **CUMPLIDO** | Alocaciones reducidas de 10 a 4, y memoria reducida en un 69%. |
| Ausencia de regresiones de fuzzing y JSON | **CUMPLIDO** | Todos los fuzz targets y unit tests pasan en verde. |

---

## Risks / Regressions
* **Boxing en sync.Pool:** A pesar de reutilizar los búferes con éxito, las type assertions requeridas al invocar `sync.Pool.Get()` continúan generando alocaciones de control en el Heap (1 alocación por salto de capacidad). Este comportamiento se refinará mediante estructuración por valor o thread-local buffers en la Fase 05.

---

## Private Evidence References
* `.private/reports/phase-03-private-evidence.md`
* `.private/bench/phase03_json_encoder_bench_default.txt`
* `.private/bench/phase03_json_encoder_bench_gmp1.txt`
* `.private/bench/phase03_json_encoder_bench_gmp2.txt`
* `.private/pprof/phase03_1kb_cpu.pprof`
* `.private/pprof/phase03_1kb_mem.pprof`

---

## Next Phase Recommendation
Se recomienda avanzar a la **Fase 04 — Redacción y seguridad de datos sensibles** para diseñar e integrar un sistema de redacción zero-allocation y no reflectivo que sustituya la denegación reflectiva del wrapper actual, mitigando los cuellos de botella de la auditoría.
