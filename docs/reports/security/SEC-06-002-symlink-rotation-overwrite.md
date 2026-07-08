# Security Finding

## Finding ID: SEC-06-002

### Metadata
- **Severity:** High
- **Vector Type:** Symlink Attack
- **Date Identified:** 2026-07-08
- **Status:** Remediated

### Vulnerability Description
Durante el proceso de rotación de logs (tanto diario como por tamaño), Acacia renombraba archivos y backups (`log.N -> log.(N+1)`) utilizando `os.Rename` y abría/creaba el nuevo log activo con `os.OpenFile` sin validar si la ruta de destino u origen apuntaba a un enlace simbólico (symlink). Si un atacante local con acceso al directorio de logs pre-creaba un symlink apuntando a un archivo crítico del sistema, la rotación seguía el enlace simbólico y sobrescribía el contenido del archivo objetivo.

### Impact
Destrucción, corrupción o manipulación de archivos clave de configuración del sistema operativo o datos de otras aplicaciones alojadas en la misma máquina física, permitiendo ataques de denegación de servicio (DoS) o elevación de privilegios de forma local.

### Code Snippet Reference
Antes de la corrección:
```go
for i := maxRot - 1; i >= 0; i-- {
    src := fmt.Sprintf("%s.%d", target, i)
    dst := fmt.Sprintf("%s.%d", target, i+1)
    if _, err := os.Stat(src); err == nil {
        _ = os.Rename(src, dst)
    }
}
```

### Proof of Concept (PoC)
1. Atacante crea un symlink: `ln -s /etc/shadow /var/log/acacia/app.log.0`
2. El logger activo alcanza el límite de tamaño y desencadena la rotación.
3. El logger ejecuta `os.Rename("app.log", "app.log.0")`, lo que sigue el enlace simbólico y sobrescribe `/etc/shadow` con el contenido del log activo.

### Remediation Plan
Se implementó un helper `IsSymlink(path)` basado en `os.Lstat` que comprueba si un archivo es un enlace simbólico. Se agregaron validaciones antes de cualquier operación de renombrado u apertura de archivos en `rotateByDate` y `logRotate`. Si alguna ruta involucrada es un symlink, la operación se cancela de forma no bloqueante, se incrementa el contador atómico `RotationErrorsTotal` y se retorna `ErrUnsafeSymlink`.
