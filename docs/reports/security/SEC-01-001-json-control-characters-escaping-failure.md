# Security Finding: Control Character Escaping Failure in Structured JSON

*This is a historical engineering report. The filename is kept for audit continuity.*

## Finding ID: SEC-01-001

### Metadata
- **Severity:** Medium
- **Vector Type:** Invalid JSON Generation / Control Character Leak
- **Date Identified:** 2026-07-07
- **Status:** Remediated

### Vulnerability Description
Acacia's structured JSON encoder (`escapeJSONInto` in `acacia.go`) does not escape ASCII control characters in the range `U+0000` to `U+001F` (for example, null bytes `\x00`, start of heading `\x01`, etc.).

According to the RFC 8259 standard (Section 7), all control characters in the range `U+0000` to `U+001F` **must be escaped** in JSON strings (for example, using `\u00xx` notation). By omitting this escape, the logger injects these raw bytes into the serialized message, producing JSON output that does not comply with the standard and is invalid for strict parsers.

### Impact
* **Ingestion Denial of Service (SIEM/Parsers):** Log collectors (such as Logstash, Fluentd, Splunk, or Go's native `encoding/json` parser) may reject the log line entirely or fail to parse the complete record. This can be exploited by malicious users sending payloads with control bytes to sabotage the application's audit visibility.
* **Visibility Corruption:** Loss of critical traces when the log contains unexpected binary payloads.

### Code Snippet Reference
In [acacia.go:L531-L548](../../acacia.go#L531-L548):
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
			dst = append(dst, c) // Injection of control bytes < 0x20
		}
	}
	return dst
}
```

### Proof of Concept (PoC)
Running the fuzz target `FuzzStructuredJSONValidLine` with input containing null bytes or other non-printable characters:
```go
lg.StructuredJSON(true)
lg.Info(map[string]interface{}{"msg": "\x00\x01\x02"})
```
Produces the following invalid output:
```json
{"ts":"Jul 7, 2026 17:16:56.097796 -04","level":"INFO","msg":"\x00\x01\x02"}
```
When attempting to parse with `encoding/json.Valid`:
```go
json.Valid([]byte(line)) // returns false
```

### Remediation Plan
In **Phase 01C**, a direct fix was applied to the `escapeJSONInto` function in `acacia.go` to filter and properly format bytes smaller than `0x20` into their corresponding hexadecimal escape sequences `\u00xx`, along with escaping backspace `\b` and form feed `\f`.

### Remediation Evidence
The fix was successfully validated by running the complete suite of unit tests and fuzzing:
```bash
# Successful execution of unit tests and race detector
go test ./...
go test -race ./...

# Fuzzing execution for 20 seconds per target
go test ./test -run=FuzzStructuredJSONValidLine -fuzz=FuzzStructuredJSONValidLine -fuzztime=20s
```
All fuzz targets, including `FuzzStructuredJSONValidLine`, reported **PASS** after completing the allocated time without any crashes or generation of invalid JSON.
