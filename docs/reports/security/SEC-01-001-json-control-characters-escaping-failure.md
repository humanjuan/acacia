# Security Finding: Control Character Escaping Failure in Structured JSON

## Finding ID: SEC-01-001

### Metadata
- **Severity:** Medium
- **Vector Type:** Invalid JSON Generation / Control Character Leak
- **Date Identified:** 2026-07-07
- **Status:** Remediated

### Vulnerability Description
El codificador estructurado de JSON de Acacia (`escapeJSONInto` en `acacia.go`) no realiza el escape de caracteres de control ASCII en el rango `U+0000` a `U+001F` (por ejemplo, bytes nulos `\x00`, start of heading `\x01`, etc.).

De acuerdo con el estándar RFC 8259 (Sección 7), todos los caracteres de control en el rango `U+0000` a `U+001F` **deben ser escapados** obligatoriamente en strings JSON (por ejemplo, usando la notación `\u00xx`). Al omitir este escape, el logger inyecta estos bytes crudos en el mensaje serializado, produciendo una salida JSON que no cumple con el estándar y es inválida para parsers estrictos.

### Impact
* **Denegación de Servicio en Ingesta (SIEM/Parsers):** Los recolectores de logs (como Logstash, Fluentd, Splunk o el parser nativo de Go `encoding/json`) pueden rechazar por completo la línea de log o fallar al parsear el registro completo. Esto puede ser explotado por usuarios malintencionados enviando payloads con bytes de control para sabotear la visibilidad de auditoría de la aplicación.
* **Corrupción de Visibilidad:** Pérdida de trazas críticas cuando el log contiene payloads binarios imprevistos.

### Code Snippet Reference
En [acacia.go:L531-L548](file:///Volumes/Juan_Ext/projects/go/acacia/acacia.go#L531-L548):
```go
func escapeJSONInto(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\\', '"':
			dst = append(dst, '\\', c)
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			dst = append(dst, c) // Inyección de control bytes < 0x20
		}
	}
	return dst
}
```

### Proof of Concept (PoC)
Ejecutando el fuzz target `FuzzStructuredJSONValidLine` con el input conteniendo bytes nulos u otros caracteres no imprimibles:
```go
lg.StructuredJSON(true)
lg.Info(map[string]interface{}{"msg": "\x00\x01\x02"})
```
Produce la siguiente salida inválida:
```json
{"ts":"Jul 7, 2026 17:16:56.097796 -04","level":"INFO","msg":"\x00\x01\x02"}
```
Al intentar parsear con `encoding/json.Valid`:
```go
json.Valid([]byte(line)) // retorna false
```

### Remediation Plan
En la **Fase 01C**, se aplicó una corrección directa sobre la función `escapeJSONInto` en `acacia.go` para filtrar y formatear adecuadamente los bytes menores a `0x20` a sus secuencias de escape hexadecimales `\u00xx` correspondientes, junto con el escape de backspace `\b` y form feed `\f`.

### Remediation Evidence
La corrección se validó con éxito ejecutando la suite completa de pruebas unitarias y fuzzing:
```bash
# Ejecución exitosa de pruebas unitarias y race detector
go test ./...
go test -race ./...

# Ejecución de fuzzing por 20 segundos por target
go test ./test -run=FuzzStructuredJSONValidLine -fuzz=FuzzStructuredJSONValidLine -fuzztime=20s
```
Todos los fuzz targets, incluyendo `FuzzStructuredJSONValidLine`, reportaron **PASS** tras completar el tiempo asignado sin crashes ni generación de JSON inválido.
