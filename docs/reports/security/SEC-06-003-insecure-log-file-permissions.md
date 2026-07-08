# Security Finding

## Finding ID: SEC-06-003

### Metadata
- **Severity:** Medium
- **Vector Type:** Insecure Log File Permissions
- **Date Identified:** 2026-07-08
- **Status:** Remediated

### Vulnerability Description
El logger Acacia originalmente creaba y abría todos los archivos de logs con permisos de máscara `0644`. Esta configuración otorga permisos de lectura pública (world-readable) a cualquier usuario del sistema local, lo cual es inseguro para logs que contienen información sensible o confidencial. Adicionalmente, el logger no proveía opciones para configurar de forma explícita las máscaras de permisos de archivos o directorios.

### Impact
Fuga de información confidencial (credenciales, tokens de acceso, secretos comerciales o datos de identificación de clientes) a usuarios locales no privilegiados que comparten el mismo entorno físico de ejecución.

### Code Snippet Reference
Antes de la corrección:
```go
f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
```

### Proof of Concept (PoC)
1. El logger escribe logs que contienen tokens de acceso en `/var/log/acacia/app.log`.
2. Un usuario local sin privilegios lee el archivo: `cat /var/log/acacia/app.log` y roba los secretos debido a los permisos `0644`.

### Remediation Plan
Se cambiaron los permisos por defecto para archivos nuevos a `0600` (lectura/escritura exclusiva para el propietario) y para directorios a `0700` (lectura/escritura/ejecución exclusiva para el propietario). Adicionalmente, se agregaron las opciones configurables `WithFileMode(mode)` y `WithDirMode(mode)` que sanitizan los permisos para evitar el establecimiento de bits de escritura pública o escritura grupal.
