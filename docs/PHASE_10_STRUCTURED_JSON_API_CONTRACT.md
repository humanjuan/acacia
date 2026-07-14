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
- **Floats No Finitos (NaN y +/-Inf)**: Los valores numéricos no finitos (`math.NaN()`, `math.Inf(1)` y `math.Inf(-1)`) no se serializarán como literales numéricos directos (lo que resultaría en JSON inválido), sino que se degradarán a strings JSON válidos: `"NaN"`, `"+Inf"` y `"-Inf"`, respectivamente.

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
- **Métodos Mínimos (Sprint 10.3 Core)**: El builder de arrays soporta adiciones tipadas primitivas (`Str`, `Int`, `Int64`, `Uint64`, `Bool`, `Float64`, `Time`, `Dur`, `Err`) y objetos (`Object`).
- **Postergaciones**: Se posterga el soporte para arrays anidados (arrays dentro de arrays), `RawJSON` y `Any` dentro de arrays para fases posteriores.
- **Arrays Vacíos o Nil**: Emiten `[]` de forma segura.
- **Redacción Recursiva**: Se aplica de forma automática sobre los campos sensibles de cualquier objeto estructurado dentro del array.
- **Callbacks Nil o Panics**:
  - Un callback `nil` en el array emite `[]`.
  - Un pánico en el callback del array completo ejecuta rollback del campo y añade `"_acacia_error": "array callback panic"` en el elemento padre.
  - Un pánico dentro de un elemento `Object` individual dentro del array ejecuta rollback de ese elemento concreto y emite `{"_acacia_error": "object callback panic"}` dentro del array, preservando el resto de los elementos.
- **Err y Float64**: `Err(nil)` es no-op. `Err(error)` emite el mensaje como string JSON. `Float64` no finitos se serializan como strings JSON válidos.

---

## 5. Group vs Namespace
Se proveerán ambas alternativas arquitectónicas debido a sus distintos casos de uso:

### 5.1 Group (Estructura Anidada Real)
```go
lg.InfoEvent().Group("http", func(g *acacia.Object) {
    g.Str("method", "GET")
})
```
*Salida*: `{"http":{"method":"GET"}}` (Ideal para SIEMs relacionales modernos).
- **Semántica**: Group es un alias de `Object`. `Event.Group` delega a `Event.Object` y `Object.Group` delega a `Object.Object`.
- **Manejo de Panics**: Si el callback del grupo paniquea, se realiza rollback del grupo completo y se añade `"_acacia_error":"object callback panic"` en el nivel superior.
- **Callbacks Nil/Vacíos**: Un callback `nil` o vacío emite `{}` de forma segura.

### 5.2 Namespace (Claves Planas con Prefijo)
```go
lg.InfoEvent().Namespace("http", func(n *acacia.Namespace) {
    n.Str("method", "GET")
})
```
*Salida*: `{"http.method":"GET"}` (Ideal para indexadores planos tradicionales que penalizan la anidación física).
- **Alcance dedicado**: El callback recibe un `*acacia.Namespace` que no expone `Msg()` ni `Send()` para evitar cierres prematuros de línea.
- **Zero-Value**: Una instancia zero-value de `acacia.Namespace` es no-op y segura contra pánicos.
- **Callbacks Nil/Vacíos**: Si el callback es `nil` o no escribe campos, el namespace no emite salida ni corrompe el JSON.
- **Prefix Vacío**:
  - Si `prefix == ""`, la clave final es `key`.
  - Si `key == ""` y `prefix != ""`, la clave final es `prefix + "."`.
  - Si ambos son vacíos, se emite clave vacía `""`.
- **Escapado**: Tanto el prefijo como la clave son escapados usando `escapeJSONInto`.
- **Manejo de Panics**:
  - En `Event.Namespace`, un panic hace rollback a la posición inicial y añade `,"_acacia_error":"namespace callback panic"` a nivel raíz.
  - En `Object.Namespace`, un panic hace rollback y añade `,"_acacia_error":"namespace callback panic"` dentro del objeto contenedor, restaurando correctamente el estado del delimitador coma (`o.first`).
- **Redacción de Seguridad**: Se realiza de forma case-insensitive y sin allocs de memoria sobre:
  - Clave hoja sensible.
  - Prefijo sensible.
  - Algún segmento sensible dentro de la clave o el prefijo (delimitados por `.`, `_`, `-`).
  - Clave compuesta resultante.
- **Valores Especiales**:
  - `Float64` no finitos se convierten a strings (`"NaN"`, `"+Inf"`, `"-Inf"`).
  - `Err(nil)` es no-op. `Err(err)` y `ErrKey(key, err)` emiten el error como un string JSON estructurado bajo la clave con prefijo correspondiente.

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
- **Métodos Disponibles**: `Event.RawJSON(key, raw)` y `Object.RawJSON(key, raw)`. Las versiones para `Array` y `Namespace` quedan diferidas.
- **Validación y Degradación**: Acepta cualquier valor JSON válido (object, array, string, number, bool, null) según `json.Valid`. Si el payload es inválido, se degrada de forma segura a un string escapado (se serializa como un literal de string JSON con caracteres de control escapados) previniendo corrupciones de sintaxis en el archivo final.
- **Nil y Vacíos**: Un payload `nil` o `[]byte{}` vacío se degrada de forma segura a un string vacío (`""`).
- **Protección contra Log Injection y CR/LF**: Si un payload válido contiene saltos de línea (`\n` o `\r`), se procesa mediante `json.Compact` en un paso de compresión interna antes de escribirlo para que el evento completo se emita en una sola línea física.
- **Redacción de Datos**:
  - **Externa**: Si la clave externa (key) se detecta como sensible, la totalidad del payload se redacta a `"[REDACTED]"`.
  - **Interna (Trusted Input Only)**: Si la clave externa no es sensible, el payload no se parsea ni se redacta internamente, asumiendo que es una entrada confiable pre-redactada.
- **Diferimientos**: `Array.RawJSON` y `Namespace.RawJSON` siguen diferidos. No se incluye soporte para Any ni OpenTelemetry.

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
