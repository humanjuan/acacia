# Plan de Mejora de Acacia (Improvement Roadmap)

Este documento detalla el plan incremental de fases, actualmente extendido hasta Fase 10, estructurado para transformar el logger Acacia en un motor de telemetría de alto rendimiento, con targets de Zero-Allocation, seguro y robusto frente a fallos.

---

## Fase 00 — Organización del repositorio

### Objetivo
Establecer la estructura documental, delimitar claramente el espacio de trabajo público frente al privado (local), y configurar las exclusiones en Git para evitar fugas de información.

### Entregables
* Estructura pública bajo `docs/` con planes de ingeniería, seguridad, runbooks y políticas de privacidad.
* Estructura privada local `.private/` para almacenamiento local e ignorado por Git.
* `.gitignore` actualizado con exclusión de carpetas privadas y archivos de perfilado.
* Reporte de organización de repositorio `docs/reports/phase-00-repo-organization-report.md`.

### Pruebas
* Ejecutar comandos de validación `git check-ignore` y `git status --short`.
* Validar que ningún archivo dentro de `.private/` o bajo directorios `private/` locales esté trackeado.

### Criterio de aceptación
* Ningún archivo privado o temporal trackeado por Git.
* Planes, plantillas de reporte y runbooks iniciales creados en la rama pública.

---

## Fase 01 — Baseline reproducible

### Objetivo
Medir con precisión científica el estado inicial de rendimiento de la versión actual bajo una configuración de hardware estrictamente acotada (`GOMAXPROCS=1` y `GOMAXPROCS=2`), aislando los cuellos de botella mediante perfiles de pprof.

### Entregables
* Evidencia cruda y perfiles locales en `.private/bench/` y `.private/pprof/`.
* Resumen público consolidado de rendimiento y cuellos de botella en `docs/reports/phase-01-baseline-report.md`.

### Criterio de aceptación
* Claims de rendimiento público detallados por cada ruta lógica:
  * String fast path
  * Bytes fast path
  * Structured legacy (map/interface)
  * Sanitización/Redacción recursiva actual
  * Cola saturada (Drop/select)
  * Rotación y escritura I/O
  * Sincronización (`Sync`/`Close`)

---

## Fase 02 — API estructurada tipada

### Objetivo
Diseñar e implementar una API tipo Builder (Fluent API) que elimine el uso de interfaces genéricas `interface{}` y mapas dinámicos en el hot path, erradicando por completo el boxing de datos en llamadas concurrentes.

### Ejemplo objetivo de API
```go
log.Info().
    Str("service", "owl").
    Int("status", 200).
    Bool("cached", true).
    Msg("request completed")
```

### Entregables
* Diseño técnico y propuesta de API fluida.
* Implementación incremental de la API tipo builder.
* Tests unitarios y de benchmarks comparativos.
* Reporte de fase sanitizado y perfiles locales privados.

### Criterio de aceptación
* Nivel deshabilitado: **0 allocs/op**.
* String/bytes fast path: **0 allocs/op**.
* Builder simple: objetivo de **0 allocs/op** (o máximo de 1 alloc/op justificado por el escape de la interfaz final de envío).
* Ningún llamado a `reflect`, `fmt.Sprint` ni `encoding/json` en el hot path del builder.

---

## Fase 03 — Encoder JSON append-only

### Objetivo
Reemplazar la serialización actual por un encoder manual optimizado basado puramente en operaciones `append` de bytes sobre un búfer reutilizable gestionado con `sync.Pool`.

### Entregables
* Implementación del codificador JSON inline sin reflexión.
* Soporte nativo para tipos primitivos (`string`, `int`, `uint`, `bool`, `float`, `error`, `time`, `duration`).
* Rutina robusta de escape JSON para strings.

### Criterio de aceptación
* Salida JSON estrictamente válida según especificación.
* Criterio de aceptación para mitigar ataques de log injection mediante sanitización de retornos de carro (`\n` y `\r`).
* Fuzz tests que validen el escape JSON de caracteres extraños.
* Target de cero asignaciones extras en benchmarks documentados bajo `GOMAXPROCS=1` al formatear JSON complejo.

---

## Fase 04 — Redacción y seguridad de datos sensibles

### Objetivo
Eliminar el uso de la sanitización reflectiva recursiva del camino caliente del logger. Reemplazarlo por redacción estática inline.

### Entregables
* API del builder equipada con métodos selectivos como `Secret(key, val)` y `StrRedacted(key, val)`.
* Denylist estática optimizada para coincidencia de claves comunes de seguridad.
* Tests exhaustivos que validen la redacción automática de:
  * `cookie`, `set-cookie`, `authorization`, `token`, `access_token`, `refresh_token`, `password`, `api_key`, `private_key`, `client_secret`.

### Criterio de aceptación
* Criterio de aceptación de que ningún dato sensible configurado aparezca en la salida JSON en las pruebas documentadas.
* Fuzzing sobre campos sensibles sin detectar bypasses de redacción.
* Rendimiento de redacción a **0 allocs/op** en la escritura a búfer.

---

## Fase 05 — Backpressure, cola y memoria

### Objetivo
Optimizar la estrategia de descarte y control de flujo del logger para evitar la degradación silenciosa y el acaparamiento de CPU del GC bajo saturación masiva.

### Entregables
* Implementación de políticas explícitas de descarte:
  * `BlockOnFull`: Productor bloquea hasta liberar espacio (backpressure real).
  * `DropNewest`: Descarta los logs nuevos entrantes cuando la cola está saturada.
  * `DropOldest`: Elimina los logs más viejos de la cola para insertar los más recientes.
  * `DropDebugOnly`: Descarta logs de menor prioridad (`DEBUG`/`INFO`) conservando los críticos.
* Métricas expuestas de telemetría:
  * `enqueued_total`, `written_total`, `dropped_total`, `queue_len`, `queue_cap`, `write_errors_total`, `rotation_errors_total`.
* Optimización para evitar la pre-construcción de eventos que terminarán siendo descartados.

### Criterio de aceptación
* Stress tests concurrentes forzando saturación extrema del canal.
* CPU dedicada a `mallocgc` y marca del GC por debajo del 10% bajo drops masivos.
* Visibilidad absoluta del contador de drops (`Dropped()`).

---

## Fase 06 — Filesystem, permisos y rotación segura

### Objetivo
Endurecer el subsistema de persistencia en disco de Acacia para garantizar la atomicidad en la rotación y prevenir vulnerabilidades de path traversal u operaciones inseguras de archivos.

### Entregables
* Mecanismo de validación estricta de rutas de directorios y nombres de archivos de log.
* Rechazo explícito de secuencias de escape como `../` y separadores de rutas en el nombre del archivo.
* Políticas estrictas para el manejo de enlaces simbólicos (symlinks).
* Implementación de rotación atómica (rename rápido y re-apertura protegida por mutex).

### Criterio de aceptación
* Tests de seguridad contra Path Traversal y Path Bypass.
* Pruebas de permisos que validen máscaras seguras (ej. `0600` para logs en lugar de `0644`).
* Pruebas de fallo físico de escritura (disco lleno, permisos removidos) sin panics inesperados.

---

## Fase 07 — Semántica formal de Sync y Close

### Objetivo
Establecer un protocolo formal determinista para las operaciones de sincronización (`Sync()`) y apagado (`Close()`), apuntando a evitar pérdidas de logs encolados antes del cierre en las suites de prueba.

### Entregables
* Estructuras de control y canalización interna para asegurar el vaciado total.
* Manejo de idempotencia en `Close()`.
* Definición formal del comportamiento post-cierre (p. ej., retornar un error estructurado si se escribe tras cerrar).

### Criterio de aceptación
* Pasa el race detector (`go test -race ./...`) bajo llamadas concurrentes y desordenadas a `Sync` y `Close`.
* Criterio de aceptación de cero pérdida de registros encolados al invocar `Close()` bajo pruebas documentadas.

---

## Fase 08 — Documentación, migración y release

### Objetivo
Alinear el README público con el nuevo rendimiento real y reproducible, documentar los cambios de la nueva API builder y preparar la entrega final del proyecto.

### Entregables
* Guía de Migración pública (`docs/MIGRATION_STRATEGY.md`).
* Documentación de benchmarks actualizados e instrucciones exactas de reproducibilidad.
* Tabla comparativa honesta y verificada frente a otros motores de la comunidad.
* Reporte final de fase 08 y recomendaciones para el etiquetado del release (tag de versión puente v2.4.x/v2.5.x, o candidato a v3.0.0 si hay corte limpio).

### Criterio de aceptación
* README público actualizado, transparente y técnicamente verificable.
* Aprobación del conjunto completo de pruebas e informe final firmado.

---

## Fase 09 — Performance Recovery, Architecture Review & OpenTelemetry Compatibility

### Estado
Completed (Sprint 09.1 & 09.2)

### Objetivo
Detectar las causas de las regresiones de rendimiento detectadas en la validación del release candidate y aplicar optimizaciones o reestructuraciones parciales de arquitectura que permitan recuperar la performance (particularmente en payloads grandes de 1KB y hot path de string/bytes) manteniendo los criterios de seguridad, estabilidad y compatibilidad con la rama v2.x. Adicionalmente, planificar y evaluar las bases arquitectónicas de compatibilidad con OpenTelemetry, difiriendo la implementación del adaptador opcional para fases posteriores.

### Motivación
La validación en SSD detectó regresiones de latencia (+21-23% en hot path de strings/bytes y hasta +92% en payloads de 1KB) causadas por la sobrecarga de controles atómicos y la estrategia de crecimiento del búfer. Es imperativo corregir estas regresiones antes del release definitivo para cumplir con las promesas de rendimiento del logger.

### Entregables
* Benchmarks de diagnóstico ejecutados y reportados de forma sanitizada.
* Archivos de perfilado CPU/Memoria almacenados localmente en `.private/pprof/`.
* Reporte público sanitizado del diagnóstico técnico de rendimiento.
* Registro de decisión de arquitectura (ADR) finalizado bajo `docs/adr/`.
* Plan de testing hard reproducible configurado con benchstat.
* Recuperación de al menos el 50% de la regresión de hot path y reducción del 60% de B/op en 1KB.
* Reporte de validación final y recomendación de release actualizada.

### Criterio de salida
* Cumplimiento de los targets de recuperación de performance.
* Fuzzing, race detector y stress tests sin advertencias ni errores.
* Compatibilidad con APIs públicas v2.x totalmente conservada.
* Criterios arquitectónicos y de diseño para la compatibilidad con OpenTelemetry establecidos; la implementación del adaptador opcional queda diferida para una fase posterior.

---

## Fase 10 — Structured JSON Maturity

### Estado
Planned / Proposed

### Objetivo
Llevar el soporte de JSON estructurado de Acacia a un nivel profesional y competitivo en el mercado, soportando tanto campos planos de alto rendimiento como estructuras JSON multi-nivel tipadas y seguras.

### Entregables
* Especificación de API pública y ADR 0004 de madurez de JSON estructurado.
* Implementación de Flat JSON builder hardening.
* Implementación de Object y Array builders seguros y eficientes sin reflexión en hot paths recomendados.
* Soporte para Groups/Namespaces y RawJSON seguro con auditoría de redacción recursiva.
* Any/Interface fallback documentado y acotado como camino lento.
* Cobertura de pruebas unitarias, race detector y fuzzing.
* Benchmarks comparativos detallados e integración de ejemplos prácticos en README.

### Criterio de salida
* `go test ./...` y `go test -race ./...` aprobados sin advertencias.
* Fuzz targets del codificador JSON y redacción recursiva pasando de forma exitosa.
* Pruebas de redacción recursiva validadas (claves sensibles no expuestas en objetos o arrays).
* Los builders de objetos/arrays evitan la reflexión en sus paths recomendados.
* La compatibilidad con el camino legacy de mapas se mantiene intacta.
* La documentación distingue claramente el camino rápido del fallback lento.
* Sin claims de rendimiento finales de release hasta completar la validación hard en SSD interno.
