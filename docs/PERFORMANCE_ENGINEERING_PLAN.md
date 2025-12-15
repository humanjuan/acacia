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
* **Cero Tolerancia a Allocations:** El criterio de aceptación para el Fast-path (String y Bytes) y para el Builder simple es estrictamente **0 allocs/op**.
