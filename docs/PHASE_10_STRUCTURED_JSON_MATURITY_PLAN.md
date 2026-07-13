# Plan de Madurez de JSON Estructurado — Fase 10

## Objetivo
Llevar el soporte de JSON estructurado de Acacia a un nivel profesional y competitivo en el mercado, habilitando tanto campos planos de alto rendimiento como estructuras JSON multi-nivel tipadas, robustas, seguras y libres de reflexión en el hot path.

## Alcance
- Consolidación del builder para JSON plano (Flat JSON).
- Soporte para objetos anidados (`Object`) y colecciones tipadas (`Array`) utilizando builders eficientes y libres de reflexión.
- Agrupamiento mediante `Group` (objetos reales) y `Namespace` (claves aplanadas).
- Integración segura de JSON pre-serializado (`RawJSON`) con validación de sintaxis y mitigación de bypasses de redacción.
- Fallback seguro y explícitamente lento (`Any`) para compatibilidad dinámica.
- Auditoría rigurosa de redacción recursiva contra fugas de credenciales.
- Fuzzing, stress testing, benchmarks diagnósticos y documentación de tradeoffs.

## Exclusiones en esta Fase
- Implementación nativa de OpenTelemetry en el core.
- Validación final de release en SSD (diferida para el cierre de todo el desarrollo principal).
- Cambios disruptivos que rompan la compatibilidad hacia atrás en la rama v2.x.
- Dependencias externas pesadas para codificación JSON.

---

## Plan de Ejecución por Sprints

### Sprint 10.0 — Discovery & API Contract
- **Objetivo**: Definir y formalizar la especificación y contrato de la API pública para JSON simple y multi-nivel.
- **Detalle**:
  - Comparar ergonómicamente patrones comunes de la comunidad (flat structured fields, callbacks para builders, namespaces planos y marshaler interfaces) sin duplicar diseños existentes.
  - Asegurar la estabilidad de la rama v2.x.
  - Definir qué APIs se consideran de camino rápido (camino rápido con target de zero-allocation en benchmarks) y cuáles representan caminos lentos y compatibles (reflection/maps).
- **Criterios de Aceptación**:
  - Contrato público documentado y aprobado en el ADR 0004.
  - Reglas de redacción de seguridad y de escaping alineadas antes del desarrollo.

### Sprint 10.1 — Flat JSON Builder Hardening
- **Objetivo**: Robustecer la ruta crítica de JSON plano.
- **Detalle**:
  - Implementar métodos de builder tipados: `Str`, `Int`, `Int64`, `Uint64`, `Bool`, `Float64`, `Dur`, `Time`, `Err`.
  - Asegurar soporte de sanitización y redacción con `Secret` y `StrRedacted`.
  - Establecer políticas coherentes de escape de caracteres de control, comportamiento ante claves duplicadas y filtrado de valores nulos o vacíos.
- **Criterios de Aceptación**:
  - Cobertura de tests unitarios completa de escape JSON.
  - Target de 0 allocs/op en benchmarks documentados para campos planos.

### Sprint 10.2 — Object Builder
- **Objetivo**: Permitir la inserción de objetos JSON anidados con tipado estricto y sin reflexión.
- **Detalle**:
  - Diseñar el API builder basado en callbacks:
    ```go
    lg.InfoEvent().
        Object("user", func(u *acacia.Object) {
            u.Str("id", "u_123")
            u.Object("profile", func(p *acacia.Object) {
                p.Str("country", "CL")
            })
        }).
        Msg("completed")
    ```
  - Evitar el uso de `map[string]interface{}` y reflection.
  - Implementar redacción recursiva sobre claves del callback.
- **Criterios de Aceptación**:
  - Pruebas que validen que el JSON resultante sea sintácticamente correcto.
  - Prevención de fugas de pánicos si ocurre un error dentro de la función de callback.

### Sprint 10.3 — Array Builder
- **Objetivo**: Soportar arrays tipados y colecciones de objetos.
- **Detalle**:
  - Habilitar inserción de arrays primitivos y arrays de objetos mediante callbacks:
    ```go
    lg.InfoEvent().
        Array("items", func(a *acacia.Array) {
            a.Object(func(i *acacia.Object) {
                i.Str("sku", "ABC")
            })
            a.Str("notes")
        })
    ```
- **Criterios de Aceptación**:
  - Estructuración correcta de delimitadores de arrays (`[` y `]`).
  - Redacción de seguridad completamente funcional sobre objetos incrustados en arrays.

### Sprint 10.4 — Groups / Namespaces
- **Objetivo**: Facilitar la agregación y agrupamiento de campos estructurados.
- **Detalle**:
  - Definir la dualidad de agrupación:
    - `Group`: Crea un objeto anidado real `{ "http": { "method": "GET" } }`.
    - `Namespace`: Aplana las claves con un delimitador `{ "http.method": "GET" }`.
- **Criterios de Aceptación**:
  - Ambas opciones implementadas de forma ergonómica en el builder y diferenciadas en rendimiento.

### Sprint 10.5 — RawJSON Seguro
- **Objetivo**: Permitir la inclusión de payloads JSON pre-serializados con seguridad de tipos y validación sintáctica.
- **Detalle**:
  - Proveer la función `RawJSON(key, rawBytes)`.
  - Validar la integridad del JSON entrante antes de anexarlo al buffer. Si no es válido, escapar como string simple para prevenir inyecciones.
  - Tratar el input como de confianza selectiva ("trusted input only") y documentar explícitamente las limitaciones de redacción sobre payloads crudos.
- **Criterios de Aceptación**:
  - Fuzzing sobre inputs malformados sin corromper el log final.
  - Ausencia de bypasses de redacción documentada en el reporte.

### Sprint 10.6 — Any / Interface Fallback
- **Objetivo**: Proveer compatibilidad con tipos dinámicos sin penalizar el camino caliente.
- **Detalle**:
  - API `Any(key, value)`.
  - Utilizar codificación basada en reflection o `encoding/json` únicamente para esta rama, documentándola claramente como una ruta lenta.
- **Criterios de Aceptación**:
  - Aislamiento de asignaciones de memoria y CPU. Los benchmarks diagnósticos deben reportar las penalizaciones asociadas a este camino de manera clara.

### Sprint 10.7 — Recursive Redaction Audit
- **Objetivo**: Auditoría exhaustiva y estricta de seguridad sobre toda la estructura JSON.
- **Detalle**:
  - Diseñar tests específicos para verificar la redacción en múltiples niveles: en la raíz, en objetos anidados de profundidad variable y dentro de arrays.
  - Asegurar el filtro automático de claves prohibidas (`password`, `token`, `authorization`, etc.) sin importar si están en mayúsculas o minúsculas.
- **Criterios de Aceptación**:
  - Sin fugas observadas bajo la suite de redacción recursiva documentada.

### Sprint 10.8 — Fuzzing & Security Validation
- **Objetivo**: Garantizar la robustez sintáctica del codificador bajo fuzzing.
- **Detalle**:
  - Fuzzing de codificación de caracteres de control, UTF-8 inválido, payloads masivos y claves duplicadas.
- **Criterios de Aceptación**:
  - Ejecución limpia de `go test ./test -fuzz=FuzzJSON` durante al menos 60s sin panics ni corrupciones sintácticas.

### Sprint 10.9 — Benchmarks & Performance Gate
- **Objetivo**: Medición científica y aislada de los nuevos endpoints del builder.
- **Detalle**:
  - Benchmarks para builders planos, anidados, arrays y fallback Any.
  - Comparar cuantitativamente la nueva API tipada frente a la versión heredada con mapas.
- **Criterios de Aceptación**:
  - Evidencia almacenada localmente en la carpeta privada `.private/`.

### Sprint 10.10 — Documentation & Examples
- **Objetivo**: Integración final de ejemplos y guías de uso en el README público.
- **Detalle**:
  - Escribir casos prácticos de uso de cada tipo de builder y advertir sobre el coste de la API `Any`.
- **Criterios de Aceptación**:
  - Documentación de ejemplos sanitizada y clara para el usuario final.

---

## Testing Policy

SSD Hard Validation is deferred until the end of all major implementation phases.

During Phase 10, each implementation sprint must pass an Implementation Progress Gate:
- `go test ./...`
- `go test -race ./...`
- focused JSON tests
- fuzzing when encoder/redaction changes
- diagnostic benchmarks
- privacy/claim grep for docs
- no private artifacts committed

External-disk benchmark results may be used to compare relative changes within the same environment, but must not be presented as final release performance.
