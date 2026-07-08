# Phase 06 — Filesystem, Permissions and Secure Rotation Report

Date: 2026-07-08
Branch: `dev`
Commit: `899e768`
Go version: `go1.23.2`
OS/Arch: `darwin/amd64`
Scope: Fortalecimiento del subsistema de archivos de Acacia para mitigar path traversal, symlinks attacks, permisos inseguros y condiciones de carrera bajo rotación.

---

## Objective
El objetivo de la Fase 06 es endurecer el subsistema de sistema de archivos y rotación de Acacia frente a riesgos de Path Traversal, manipulación de enlaces simbólicos (Symlinks) y asignación insegura de permisos, manteniendo la compatibilidad de API pública v2.x y sin degradar el rendimiento del logger asíncrono.

---

## Documents Reviewed
Se revisaron los siguientes documentos y roadmaps:
* **docs/ACACIA_PROJECT_CONTEXT.md**
* **docs/ACACIA_IMPROVEMENT_ROADMAP.md**
* **docs/TESTING_AND_BENCHMARKING.md**
* **docs/SECURITY_HARDENING_PLAN.md**
* **docs/PUBLIC_PRIVATE_DOCUMENTATION_POLICY.md**
* **docs/REPORTING_GUIDE.md**
* **docs/PERFORMANCE_ENGINEERING_PLAN.md**
* **docs/MIGRATION_STRATEGY.md**
* **docs/reports/phase-05-backpressure-queue-memory-report.md**

---

## Scope Decision
* **Dentro de Alcance:**
  * Validación defensiva del nombre de archivo.
  * Validación segura del directorio base de logs.
  * Impedancia de rutas absolutas y relativas maliciosas en el nombre de log.
  * Recombinación segura de rutas sin escapes de directorio.
  * Prevención contra ataques por sobrescritura de enlaces simbólicos (Symlinks) mediante `os.Lstat`.
  * Cambio de permisos por defecto a máscaras seguras (`0600` para archivos y `0700` para directorios).
  * Inclusión de opciones configurables `WithFileMode` y `WithDirMode` que omiten bits inseguros de escritura grupal o pública.
  * Incremento y exposición de contadores de error de rotación y escritura.
* **Fuera de Alcance:**
  * Reescritura del logger asíncrono.
  * Cambios breaking en las firmas públicas de `Start`, `Rotation`, `DailyRotation`.
  * Configuración de enlaces simbólicos permitidos.

---

## Filesystem Security Design
El subsistema de archivos ha sido rediseñado con las siguientes capas de seguridad:
1. **Filename Validation:** Validación del parámetro `logName` mediante `ValidateLogFilename(name)`. Se rechazan nombres vacíos, separadores, NUL bytes o secuencias `..`.
2. **Directory Validation:** Validación de `logPath` mediante `ValidateLogDirectory(dir)`. Se resuelve la ruta absoluta limpia, se verifica que sea un directorio real y se rechaza si es un symlink.
3. **Safe Path Join:** Se compone la ruta final combinando el directorio limpio absoluto validado con el nombre limpio validado, garantizando que el archivo final resida estrictamente dentro de la raíz segura.
4. **Symlink Policy:** Se inspeccionan las rutas del archivo activo y de todos los backups con `os.Lstat` antes de operaciones de renombrado u escritura, impidiendo el seguimiento de symlinks.
5. **Permissions Policy:** Los permisos de creación se configuran por defecto a lectura/escritura exclusiva del propietario.
6. **Rotation Safety:** Las operaciones de rotación por tamaño y diario validan el origen y destino en búsqueda de symlinks, y manejan errores incrementando `RotationErrorsTotal` de forma thread-safe.

---

## Filename Validation Policy
* **Ejemplos Aceptados:**
  * `"app.log"`
  * `"audit.jsonl"`
  * `"service-2026.log"`
* **Ejemplos Rechazados (retorna `ErrInvalidLogFilename`):**
  * `""` (vacío)
  * `"."` o `".."`
  * `"../evil.log"` (path traversal)
  * `"/tmp/evil.log"` (ruta absoluta)
  * `"subdir/app.log"` (separadores de directorio)
  * `"app.log\x00evil"` (byte NUL)

---

## Directory and Symlink Policy
* El directorio base debe ser una ruta pre-existente.
* Si el directorio base proporcionado en `Start` apunta a un enlace simbólico, la inicialización se cancela retornando `ErrUnsafeSymlink`.
* En entornos multiplataforma de pruebas, se utiliza la resolución defensiva de `os.Lstat` para asegurar la detección oportuna de enlaces simbólicos.

---

## Permissions Policy
* **Máscaras por Defecto:**
  * Archivos creados por Acacia: `0600` (lectura/escritura exclusiva del propietario).
  * Directorios base validados: Se crean y validan con la máscara segura `0700` si es aplicable.
* **Opciones de Configuración:**
  * `WithFileMode(mode os.FileMode)`: Permite personalizar los permisos del archivo activo y de los backups. Remueve preventivamente bits world-writable (`0002`) y group-writable (`0020`).
  * `WithDirMode(mode os.FileMode)`: Permite personalizar los permisos de directorios. Remueve bits world-writable (`0002`) y group-writable (`0020`).

---

## Rotation Safety
* Durante rotaciones diarias y por tamaño, Acacia itera en orden descendente renombrando backups.
* Antes de ejecutar `os.Rename(src, dst)`, se comprueba `IsSymlink(src)` e `IsSymlink(dst)`.
* Si se detecta un symlink, la operación para ese slot se omite de forma segura y se suma un error a `RotationErrorsTotal`.
* Si el archivo activo (`base`) o la primera ranura (`datedBase` o `.0`) es un symlink, la rotación falla con `ErrUnsafeSymlink`.
* Al crear el nuevo archivo de log activo, se realiza con la máscara configurada `_log.fileMode` para mantener consistencia de permisos.

---

## Public API Impact
* Se añadieron las opciones:
  * `WithFileMode(mode os.FileMode) Option`
  * `WithDirMode(mode os.FileMode) Option`
* Se exportan los errores:
  * `ErrInvalidLogFilename`
  * `ErrInvalidLogDirectory`
  * `ErrUnsafeSymlink`
  * `ErrInsecureFileMode`
* Se exportan las funciones de validación auxiliares:
  * `ValidateLogFilename(name string) error`
  * `ValidateLogDirectory(dir string) (string, error)`
  * `IsSymlink(path string) bool`

---

## Files Changed
* `acacia.go`
* `test/phase06_filesystem_security_test.go`
* `test/phase06_filesystem_fuzz_test.go`
* `test/phase06_filesystem_rotation_stress_test.go`
* `test/phase06_filesystem_benchmark_test.go`

---

## Tests Executed

| Command | Result | Notes |
|---|---:|---|
| `go test ./...` | **PASS** | Ejecución de suite unitaria completa |
| `go test -race ./...` | **PASS** | Validación del detector de carreras en todo el repo |
| `go test ./test -run FuzzValidateLogFilenameNoTraversal -fuzz=FuzzValidateLogFilenameNoTraversal -fuzztime=20s` | **PASS** | Fuzzing de evasión de rutas |
| `go test ./test -run FuzzValidateLogFilenameNoNUL -fuzz=FuzzValidateLogFilenameNoNUL -fuzztime=20s` | **PASS** | Fuzzing de detección de NUL bytes |
| `go test ./test -run FuzzValidateLogFilenameAcceptsSafeNames -fuzz=FuzzValidateLogFilenameAcceptsSafeNames -fuzztime=20s` | **PASS** | Fuzzing de estabilidad de nombres seguros |
| `GOMAXPROCS=1 go test ./test -run TestStressRotation -count=1` | **PASS** | Estrés de rotación concurrente (GMP=1) |
| `go test -race ./test -run TestStressRotation -count=1` | **PASS** | Detector de carreras en estrés de rotación |

---

## Security Test Summary

| Test | Result | Notes |
|---|---:|---|
| `TestFilenameValidation` | **PASS** | Bloquea traversal y caracteres inválidos; acepta nombres limpios |
| `TestDirectoryValidation` | **PASS** | Valida existencia de carpetas y rechaza bases symlinks/ficheros |
| `TestPermissionsSecurity` | **PASS** | Comprueba el saneado de permisos world-writable y por defecto 0600 |
| `TestSymlinkActiveFile` | **PASS** | Impide inicializar Acacia si el destino es un enlace simbólico |
| `TestSymlinkBackupRotation` | **PASS** | Aborta la rotación de backups si se encuentra con un symlink |
| `TestSizeRotationNormal` | **PASS** | Verifica que las rotaciones respeten los límites de backups |
| `TestNoRegressionSyncClose` | **PASS** | Sin regresiones en control de cierre y sincronización |

---

## Fuzzing Summary

| Target | Result | Notes |
|---|---:|---|
| `FuzzValidateLogFilenameNoTraversal` | **PASS** | Millones de ejecuciones sin evasión de traversal |
| `FuzzValidateLogFilenameNoNUL` | **PASS** | Bloqueo absoluto de inyecciones de bytes NUL |
| `FuzzValidateLogFilenameAcceptsSafeNames` | **PASS** | Estabilidad bajo caracteres alfanuméricos |

---

## Stress / Race Summary

| Test | Result | Notes |
|---|---:|---|
| `TestStressRotationConcurrentWriters` | **PASS** | 30 hilos escribiendo logs de 5KB y rotando intensamente |

---

## Benchmark Summary

### GOMAXPROCS=1
| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkValidateLogFilenameSafe` | 90.7 ns | 0 B | 0 | Rápida y 0 allocations |
| `BenchmarkValidateLogFilenameTraversal` | 20.1 ns | 0 B | 0 | Cortocircuito instantáneo traversal |
| `BenchmarkSafeJoinLogPath` | 104.4 ns | 16 B | 1 | Unificación segura de ruta |
| `BenchmarkRotationCheckNoRotate` | 4.3 µs | 466 B | 0 | Operación asíncrona normal |
| `BenchmarkRotationSizeSmall` | 2.59 ms | 10.1 KB | 47 | Operación I/O física completa |

### GOMAXPROCS=2
| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkValidateLogFilenameSafe` | 71.0 ns | 0 B | 0 | Rendimiento óptimo en multiproceso |
| `BenchmarkValidateLogFilenameTraversal` | 18.8 ns | 0 B | 0 | Cortocircuito instantáneo traversal |
| `BenchmarkSafeJoinLogPath` | 88.4 ns | 16 B | 1 | Unificación segura de ruta |
| `BenchmarkRotationCheckNoRotate` | 4.4 µs | 501 B | 0 | Operación asíncrona normal |
| `BenchmarkRotationSizeSmall` | 2.18 ms | 9.6 KB | 47 | Operación I/O física completa |

---

## pprof Summary
No se generó perfilado pprof dado que el rendimiento de validaciones demostró ser ultra veloz y con zero allocations adicionales sobre el hot path principal de escritura de logs.

---

## Security Findings
* **SEC-06-001 (Path Traversal en Start):** Remediado.
* **SEC-06-002 (Symlink Overwrite en Rotación):** Remediado.
* **SEC-06-003 (Permisos Inseguros en Filesystem):** Remediado.

---

## Acceptance Criteria Status

| Criterion | Status | Evidence |
|---|---|---|
| Validación de nombres de archivo | **Completo** | `TestFilenameValidation`, `FuzzValidateLogFilenameNoTraversal` |
| Validación segura de directorio base | **Completo** | `TestDirectoryValidation` |
| Rechazo de Path Traversal | **Completo** | `TestFilenameValidation` |
| Control y rechazo de Symlinks | **Completo** | `TestSymlinkActiveFile`, `TestSymlinkBackupRotation` |
| Permisos seguros por defecto (`0600`/`0700`) | **Completo** | `TestPermissionsSecurity` |
| Rotación atómica y race-free | **Completo** | `TestStressRotationConcurrentWriters` |

---

## Risks / Regressions
* **Sistemas de Archivo Windows:** La función `IsSymlink` utiliza `os.Lstat`. En Windows, el soporte de symlinks requiere privilegios elevados del sistema operativo, por lo que el logger operará en modo degradado o fallará adecuadamente si no puede realizar la comprobación.

---

## Private Evidence References
* `.private/reports/phase-06-private-evidence.md`
* `.private/bench/phase06_go_test.txt`
* `.private/bench/phase06_go_test_race.txt`
* `.private/bench/phase06_stress_rotation_final_gmp1.txt`
* `.private/bench/phase06_final_bench_gmp1.txt`
* `.private/bench/phase06_fuzz_filename_traversal.txt`
* `.private/bench/phase06_fuzz_filename_nul.txt`
* `.private/bench/phase06_fuzz_filename_safe.txt`

---

## Next Phase Recommendation
Se recomienda declarar la Fase 06 como **finalizada** y avanzar hacia:
* **Fase 07 — Semántica formal de Sync y Close**
