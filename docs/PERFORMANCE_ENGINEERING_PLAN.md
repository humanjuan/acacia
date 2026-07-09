# Plan de Ingeniería de Rendimiento (Performance Engineering Plan)

Este documento describe la estrategia de optimización para llevar a Acacia a un estado de **Zero-Allocation** en sus rutas lógicas calientes.

---

## 1. El Costo del Diseño Tradicional

El análisis de perfiles de la auditoría inicial demostró que las dos fuentes principales de degradación bajo alta concurrencia son:
1. **Boxing de Interfaces (`interface{}`):** Cuando los datos pasan a un método con firma de interfaces dinámicas, el runtime de Go realiza allocations en el Heap para almacenar los contenedores.
2. **Reflexión (`reflect`):** El paquete `reflect` requiere inspección en tiempo de ejecución, lo cual ralentiza el throughput e incrementa la basura generada en el Heap.

---

## 2. Pilares de la Optimización a Zero-Allocation

### A. Fluent API / Builder Pattern sin Interfaces
La transición hacia una API fluida elimina el boxing en el hot path. En lugar de:
```go
logger.Info(map[string]interface{}{"key": "value"})
```
El nuevo esquema utiliza:
```go
logger.Info().Str("key", "value").Send()
```
El compilador puede optimizar los métodos encadenados debido al tipado estricto de los parámetros (`string`, `int64`, etc.).

### B. Búfer Pooling con `sync.Pool`
* En lugar de asignar nuevos arrays de bytes con cada log, se mantiene un pool de structs de eventos reutilizables con un búfer interno pre-alocado.
* Al invocar `.Send()`, el búfer se encola (o escribe en disco) y la estructura del evento se retorna al `sync.Pool`.

### C. Hand-rolled JSON Encoder (Append-only)
* Se elimina por completo el uso de `encoding/json` y `fmt.Sprint`.
* El formateador serializa directamente sobre el búfer plano utilizando operaciones simples de `append` y `strconv.AppendInt`.

### D. Optimización de la Cola Asíncrona
* La cola asíncrona no debe recibir structs que alojen punteros dinámicos.
* Se implementará soporte para descarte no bloqueante óptimo en la encolación asíncrona, evitando el procesamiento y construcción de logs que se terminarán descartando.

---

## 3. Entorno de Validación Obligatorio

* **Restricción de Recursos:** Todos los benchmarks de rendimiento críticos se deben ejecutar con `GOMAXPROCS=1` para forzar la contención del planificador de Go.
* **Cero Tolerancia a Allocations:** String/bytes fast path: objetivo estricto 0 allocs/op. Builder simple: objetivo aspiracional 0 allocs/op; estado actual observado ~1 alloc/op, aceptable si está justificado y documentado.

---

## 4. Metodología de Validación Hard (Fase 09)

Para garantizar la precisión de las optimizaciones y evitar regresiones de performance y seguridad, se establece la siguiente metodología obligatoria:

### Requisitos y Prerrequisitos
- **Herramienta `benchstat`**: Prerrequisito indispensable para comparar estadísticamente las métricas de rendimiento antes (`old`) y después (`new`) de las optimizaciones.
- **Evitación de Ruido**: Ejecutar los benchmarks en un entorno local dedicado sin servicios en segundo plano pesados, y previniendo la suspensión del sistema (por ejemplo, con `caffeinate` en macOS).
- **Ignorado de Perfiles**: Todos los archivos generados de perfilado (`.pprof`, `.prof`, `.trace`, `memprofile`) deben almacenarse exclusivamente bajo `.private/` y no trackearse en Git.

### Parámetros de Ejecución
- **Repeticiones**: `count=20` para mitigar la varianza en las mediciones.
- **Perfiles Concurrencia (GOMAXPROCS)**: Validar bajo `GOMAXPROCS=1` (para evaluar contención del planificador), `GOMAXPROCS=2`, y el valor por defecto del sistema.
- **Fuzzing**: Tiempo mínimo de ejecución de 60 segundos (`-fuzztime=60s`).
- **Soak Testing**: Pruebas de remojo prolongadas opcionales habilitadas a través de la variable de entorno `ACACIA_HARD_SOAK=1`.

### Matriz de Modos a Testear
El plan de pruebas de rendimiento debe medir la latencia (`ns/op`), consumo de memoria (`B/op`) y allocations (`allocs/op`) en las siguientes configuraciones y modos:
1. **Text/Plain**: Escrituras de texto plano.
2. **Bytes**: Escritura directa de arrays de bytes.
3. **Writer**: Medición aislada del comportamiento del buffer e `io.Writer`.
4. **Legacy Map JSON**: Serialización clásica basada en `map[string]interface{}` (deuda técnica documentada).
5. **Builder JSON**: Serialización optimizada mediante el codificador append-only de la API fluida.
6. **Redaction**: Comportamiento y costo de la sanitización y redacción de datos sensibles.
7. **Backpressure**: Costo del encolamiento y descarte de eventos bajo las políticas del buffer de eventos.
8. **Sync/Close**: Latencia de las barreras de sincronización y vaciado de cola.
9. **Size Rotation**: Impacto en el hot-path del control y ejecución de rotación por tamaño.
10. **Backup Rotation**: Medición del impacto por cardinalidad y reubicación de backups.
11. **Daily Rotation Determinística**: Evaluación precisa de la rotación basada en tiempo.
12. **OTel Adapter**: Sobrecarga del adaptador opcional de OpenTelemetry (tanto habilitado como deshabilitado).
