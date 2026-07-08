# Security Finding

## Finding ID: SEC-06-001

### Metadata
- **Severity:** High
- **Vector Type:** Path Traversal
- **Date Identified:** 2026-07-08
- **Status:** Remediated

### Vulnerability Description
El logger original de Acacia resolvía la ruta de destino uniendo el directorio base y el nombre del archivo de log mediante la función `filepath.Join(logPath, logName)` sin validar o sanear el parámetro `logName`. Si una aplicación utilizaba un nombre de archivo configurable de forma dinámica por usuarios maliciosos, un atacante podía inyectar secuencias de retroceso de directorio como `../` o rutas absolutas para forzar al logger a escribir logs estructurados fuera del directorio base seguro.

### Impact
Un atacante local o remoto con control sobre el parámetro de configuración del nombre del archivo de log podía sobrescribir o corromper archivos críticos del sistema en directorios arbitrarios del servidor (p. ej., inyectando claves SSH en `.ssh/authorized_keys` o alterando archivos de configuración del sistema), provocando escalada de privilegios o ejecución remota de código (RCE).

### Code Snippet Reference
Antes de la corrección:
```go
fullPath := filepath.Join(logPath, logName)
f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
```

### Proof of Concept (PoC)
```go
// Intento de escribir un archivo fuera de la ruta base configurada
logger, err := acacia.Start("../../../etc/cron.d/evil_log", "/var/log/acacia", "INFO")
// Si tiene éxito, se escribe un cronjob malicioso o se sobrescribe una configuración sensible.
```

### Remediation Plan
Se implementó una función pura de validación defensiva `ValidateLogFilename(name)` que verifica que el nombre de archivo no contenga bytes nulos (`\x00`), secuencias de retroceso (`..`), barras de separación (`/` o `\`), y comprueba que `filepath.Clean` y `filepath.Base` retornen exactamente el mismo nombre original. Se integró esta validación antes de abrir cualquier archivo en `Start`.
