# Phase 10 — Structured JSON API Contract

## 1. Principios de Diseño
Para mantener la consistencia y la identidad de Acacia Logger, la evolución hacia un JSON estructurado maduro se rige por los siguientes principios:

- **v2.x Compatibility First**: La API propuesta debe ser completamente compatible hacia atrás con la rama v2.x. La API legacy basada en mapas dinámicos seguirá funcionando.
- **Camino Rápido Explícito**: El hot path para JSON plano (`Str`, `Int`, etc.) se mantiene como la ruta recomendada de alto rendimiento con un target de 0 allocs/op en benchmarks documentados para el fast path de campos planos.
- **Estructuración Multi-nivel sin Reflexión**: Se introduce una Fluent API con callbacks para la definición de objetos y arrays anidados de manera secuencial en el búfer, erradicando el uso de `map[string]interface{}` o reflexión en los caminos recomendados.
- **Caminos Lentos Aislados**: El fallback para tipos dinámicos (`Any`) y la API legacy con mapas se mantendrán por compatibilidad, pero estarán explícitamente documentados y etiquetados como caminos lentos en los benchmarks.
- **RawJSON Seguro**: El paso de JSON pre-serializado será validado para asegurar la sintaxis estructurada. Se documentará como "trusted input only" (sin redacción interna) para evitar el sobrecoste de procesar el string.
- **Redacción Recursiva Mandatoria**: La sanitización de campos sensibles debe propagarse hacia objetos y colecciones anidadas.
- **Validación Hard Diferida**: La validación de rendimiento final de release se difiere para el SSD interno. Las comparativas en disco externo servirán solo para telemetría diagnóstica interna.

---

## 2. API Flat Builder (JSON Plano)
Es el camino óptimo recomendado para la mayoría de los logs operacionales.

### Ejemplo de Uso
```go
lg.InfoEvent().
    Str("service", "payments").
    Int("status", 200).
    Bool("cache_hit", true).
    Dur("latency", latency).
    Msg("request completed")
```

### Métodos del Builder
- `Str(key, val string) *Event`
- `Int(key string, val int) *Event`
- `Int64(key string, val int64) *Event`
- `Uint64(key string, val uint64) *Event`
- `Bool(key string, val bool) *Event`
- `Float64(key string, val float64) *Event`
- `Dur(key string, val time.Duration) *Event`
- `Time(key string, val time.Time) *Event`
- `Err(err error) *Event`
- `Secret(key, val string) *Event`
- `StrRedacted(key, val string) *Event`
- `Msg(msg string)`
- `Send()`

### Comportamiento de Casos Borde
- **Claves o Valores Vacíos**: Se emiten normalmente en el JSON (`"key": ""`).
- **Nil Errors**: Si `Err(nil)` es invocado, se omite el campo del log final.
- **Claves Duplicadas**: Para maximizar el rendimiento del fast path, no se realizará deduplicación en caliente (0 allocs/op). Se emitirán ambas claves al output JSON, delegando el comportamiento al ingestador/SIEM final (donde típicamente la última escritura prevalece: "last write wins").
- **Escapado de Caracteres de Control y Unicode**: Criterio de aceptación: el encoder debe escapar correctamente caracteres de control (`\x00` - `\x1f`), comillas (`"`), barras diagonales inversas (`\`), y retornos de carro (`\n`, `\r`) para evitar ataques de log injection.
- **UTF-8 Inválido**: Los strings con secuencias UTF-8 inválidas deben serializarse de forma segura usando la semántica de escape/sustitución definida por el encoder, asegurando que nunca produzcan JSON inválido. Se definirá un test/fuzz para validar este caso en la implementación.

---

## 3. Object Builder (Objetos Anidados)
Permite estructurar objetos anidados tipados mediante callbacks sin reflexión.

### Ejemplo de Uso
```go
lg.InfoEvent().
    Object("user", func(u *acacia.Object) {
        u.Str("id", "u_123")
        u.Object("profile", func(p *acacia.Object) {
            p.Str("country", "CL")
            p.Str("tier", "premium")
        })
    }).
    Msg("checkout completed")
```

### Contrato de Diseño
- **Callbacks Nil**: Un callback `nil` es tratado como no-op seguro, emitiendo un objeto vacío (`{}`).
- **Manejo de Panics**: Si la función de callback paniquea, el logger interceptará el pánico (`recover`), descartará la porción del objeto que se estaba escribiendo en el búfer para evitar JSON corrupto, y añadirá un campo descriptivo constante `"_acacia_error": "object callback panic"`. El detalle crudo del pánico no se registrará en el log por defecto para evitar la fuga accidental de datos sensibles o secretos.
- **Nesting**: La recursividad de objetos está soportada. Se recomienda un límite de profundidad de 5 niveles para evitar el desbordamiento de la pila de llamadas del sistema.
- **Redacción Recursiva**: Las claves declaradas sensibles serán redactadas automáticamente dentro del objeto anidado.

---

## 4. Array Builder (Colecciones)
Permite estructurar listas y arrays tipados en el flujo de salida.

### Ejemplo de Uso
```go
lg.InfoEvent().
    Array("items", func(a *acacia.Array) {
        a.Object(func(i *acacia.Object) {
            i.Str("sku", "ABC")
            i.Int("qty", 2)
        })
        a.Str("manual-note")
        a.Int(42)
    }).
    Msg("cart evaluated")
```

### Contrato de Diseño
- **Métodos Mínimos**: El builder de arrays soportará adiciones tipadas: `Str`, `Int`, `Int64`, `Uint64`, `Bool`, `Float64`, `Time`, `Dur`, `Err`, `Object`, `RawJSON`, `Any`.
- **Arrays Anidados (Arrays dentro de arrays)**: Se postergan para una etapa posterior como una extensión futura para limitar la complejidad inicial. Para el Sprint 10.3 inicial, se soportan únicamente arrays primitivos y arrays de objetos.
- **Arrays Vacíos**: Emiten `[]`.
- **Redacción Recursiva**: Se aplica de forma automática sobre los elementos del array (especialmente en arrays de objetos).
- **Callbacks Nil o Panics**: Siguen la misma política de rollback seguro y adición del campo constante `"_acacia_error": "array callback panic"` para evitar corromper la sintaxis de salida (el detalle crudo del pánico no se registrará en el log).

---

## 5. Group vs Namespace
Se proveerán ambas alternativas arquitectónicas debido a sus distintos casos de uso:

- **Group (Estructura Anidada Real)**:
  ```go
  lg.InfoEvent().Group("http", func(g *acacia.Object) {
      g.Str("method", "GET")
  })
  ```
  *Salida*: `{"http": {"method": "GET"}}` (Ideal para SIEMs relacionales modernos).

- **Namespace (Claves Planas con Prefijo)**:
  ```go
  lg.InfoEvent().Namespace("http", func(n *acacia.Namespace) {
      n.Str("method", "GET")
  })
  ```
  *Salida*: `{"http.method": "GET"}` (Ideal para indexadores planos tradicionales que penalizan la anidación física). El scope de Namespace no debe exponer `Msg()` ni `Send()`.

---

## 6. RawJSON Seguro
Permite inyectar bytes pre-serializados de forma directa y de alto rendimiento.

### Ejemplo de Uso
```go
lg.InfoEvent().
    RawJSON("payload", rawBytes).
    Msg("received payload")
```

### Contrato de Diseño
- **Validación**: RawJSON acepta cualquier JSON value válido según `json.Valid`: object, array, string, number, bool y null.
- **Redacción de Datos**: Se documentará formalmente como **"trusted input only"**. El logger no realizará parsing ni redacción sobre `RawJSON` debido al alto coste en CPU de decodificar datos ya serializados en el hot path. Si se requiere redacción, se debe usar `Object` o `Any`. Si el JSON es inválido, se degradará de forma segura a un string escapado seguro para evitar corrupciones.

---

## 7. Any / Interface Fallback
Ruta de compatibilidad dinámica y flexible de tipo "camino lento".

### Ejemplo de Uso
```go
lg.InfoEvent().
    Any("payload", structValue).
    Msg("debug payload")
```

### Contrato de Diseño
- **Rendimiento**: Se procesa con reflection o `encoding/json` en un buffer temporal. No debe ser utilizado en caminos de alta frecuencia.
- **Redacción**: Best-effort (redacción básica si puede ser transformado a un mapa/estructura estándar, pero sin compromisos absolutos).
- **Ciclos / Tipos Inválidos**: Si se detecta un puntero cíclico, funciones o canales, se registrará un error seguro en lugar de paniquear.

---

## 8. Redacción Recursiva Obligatoria
Se auditará la redacción recursiva (Case-Insensitive) de las siguientes claves prohibidas:
`password`, `passwd`, `pwd`, `token`, `access_token`, `refresh_token`, `authorization`, `cookie`, `set-cookie`, `api_key`, `apikey`, `secret`, `client_secret`, `private_key`.

Cualquier clave que coincida parcial o totalmente con este listado reemplazará su valor por `[REDACTED]`.

---

## 9. Concurrencia y Propiedad de Datos (Ownership)
- **Seguridad Concurrente**: Los builders de eventos (`*Event`, `*Object`, `*Array`) **no son thread-safe**. Deben utilizarse de manera secuencial dentro de una sola goroutine.
- **Reutilización**: Una vez invocado `Msg()` o `Send()`, el builder queda liberado y no debe volver a referenciarse.
- **Punteros Retenidos**: Las funciones de callback no deben almacenar o conservar referencias a `*Object` o `*Array` fuera del contexto del callback de ejecución.
