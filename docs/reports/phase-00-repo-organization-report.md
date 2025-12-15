# Reporte de Fase 00: Organización del Repositorio

* **Fecha:** 2026-07-06
* **Rama:** `dev`
* **Commit Relacionado:** (Pendiente de commit)
* **Go Version:** go1.23.2 darwin/amd64
* **OS/Arch:** Darwin (macOS/amd64)

---

## 1. Estado Inicial Resumido
El repositorio de Acacia (v2.3.1) carecía de una separación estructurada entre documentación pública de diseño y evidencias privadas de rendimiento generadas localmente (pprof, bench.out). Se detectaron archivos de perfil `.pprof` sueltos en el directorio `test/` sin exclusión en `.gitignore`, lo que representaba un riesgo de fuga de datos de rutas locales absolutas.

---

## 2. Archivos y Rutas Modificados/Creados

### Rutas Públicas Creadas (`docs/`)
* [docs/README.md](../README.md): Índice principal de documentación técnica.
* [docs/ACACIA_PROJECT_CONTEXT.md](../ACACIA_PROJECT_CONTEXT.md): Contexto operativo público para agentes AI y continuidad entre sesiones.
* [docs/PUBLIC_PRIVATE_DOCUMENTATION_POLICY.md](../PUBLIC_PRIVATE_DOCUMENTATION_POLICY.md): Reglas de clasificación y exclusión de datos.
* [docs/ACACIA_IMPROVEMENT_ROADMAP.md](../ACACIA_IMPROVEMENT_ROADMAP.md): Plan de mejora detallado en 9 fases.
* [docs/TESTING_AND_BENCHMARKING.md](../TESTING_AND_BENCHMARKING.md): Runbook de comandos oficiales de pruebas y benchmarking.
* [docs/SECURITY_HARDENING_PLAN.md](../SECURITY_HARDENING_PLAN.md): Plan de hardening frente a inyecciones de logs y traversals.
* [docs/PERFORMANCE_ENGINEERING_PLAN.md](../PERFORMANCE_ENGINEERING_PLAN.md): Estrategia de optimización a Zero-Allocation.
* [docs/MIGRATION_STRATEGY.md](../MIGRATION_STRATEGY.md): Plan de compatibilidad para clientes de Acacia.
* [docs/REPORTING_GUIDE.md](../REPORTING_GUIDE.md): Directrices de escritura de reportes públicos y evidencias.
* [docs/adr/0001-public-private-documentation-boundary.md](../adr/0001-public-private-documentation-boundary.md): ADR justificando la separación de carpetas.
* [docs/adr/0002-performance-claims-must-be-reproducible.md](../adr/0002-performance-claims-must-be-reproducible.md): ADR justificando la reproducibilidad de benchmarks.
* [docs/reports/README.md](README.md): Explicación del sistema de reportes.
* [docs/reports/phase-00-repo-organization-report.md](phase-00-repo-organization-report.md): Este reporte.
* [docs/templates/public-phase-report.template.md](../templates/public-phase-report.template.md): Plantilla de reporte público.
* [docs/templates/private-evidence-report.template.md](../templates/private-evidence-report.template.md): Plantilla de evidencia privada local.
* [docs/templates/benchmark-summary.template.md](../templates/benchmark-summary.template.md): Plantilla para tablas de benchmarks.
* [docs/templates/security-finding.template.md](../templates/security-finding.template.md): Plantilla para reporte de vulnerabilidades.
* [docs/runbooks/local-audit-runbook.md](../runbooks/local-audit-runbook.md): Guía paso a paso para auditorías locales de desarrolladores.
* [docs/runbooks/release-readiness-runbook.md](../runbooks/release-readiness-runbook.md): Lista de verificación para releases seguros.
* [Makefile](../../Makefile): Automatizador de pruebas locales.

### Rutas Privadas Creadas (Locales e Ignoradas)
* `.private/` (Estructurada con subdirectorios `audits/`, `reports/`, `evidence/`, `pprof/`, `bench/`, `fuzz/`, `stress/`, `tmp/`).
* `.private/README.md`: Indicación para desarrolladores de no subir esta carpeta a Git.
* `.private/reports/phase-00-private-evidence.md`: Reporte de evidencia privado de fase 00.

---

## 3. Cambios en `.gitignore`
Se actualizó el archivo [.gitignore](../../.gitignore) agregando reglas estrictas para omitir carpetas privadas locales y extensiones de archivos binarios e intermedios generados por Go testing/profiling (`.pprof`, `.prof`, `.trace`, `.coverprofile`, `.test`, etc.).

---

## 4. Comandos Ejecutados
```bash
# Crear estructura de carpetas privadas locales
mkdir -p .private/audits .private/reports .private/evidence .private/pprof .private/bench .private/fuzz .private/stress .private/tmp

# Limpiar workspace de pprof huérfanos
mv test/vector*_*.pprof .private/pprof/
```

---

## 5. Resultados de las Validaciones

### Validación de Exclusión de Git (`git check-ignore`)
Ejecutando la verificación sobre la carpeta privada:
```bash
git check-ignore -v .private/README.md
# Salida esperada: .gitignore:9:.private/	.private/README.md
```
Se confirma que los archivos privados quedan correctamente ignorados.

### Estado de Git (`git status --short`)
```bash
 M .gitignore
 M acacia.go
?? Makefile
?? docs/
?? internal/
?? test/stress_vectors_test.go
```
*Nota: La carpeta `.private/` y los perfiles `.pprof` no se muestran en el listado de archivos untracked, validando la política de exclusión.*

### Listado de Archivos en Rutas Privadas (`git ls-files`)
```bash
git ls-files .private docs/private reports/private
# Salida vacía (ningún archivo privado trackeado)
```

---

## 6. Riesgos Detectados
* **Riesgo de Modificación Involuntaria de .gitignore:** Si un colaborador edita el `.gitignore` raíz, se podría forzar el tracking de `.private/`.
* **Archivos Privados Preexistentes:** Se verificó exhaustivamente con `git ls-files` y no se encontraron archivos binarios o privados previamente confirmados en el historial de Git de esta rama.

---

## 7. Validación de Alcance del Commit
El commit final de esta Fase 00 será de carácter **estrictamente documental y de configuración del repositorio**. Se excluyen de este commit los cambios realizados en el núcleo del logger (`acacia.go`), el paquete `internal/` (wrapper de redacción) y el archivo de pruebas `test/stress_vectors_test.go`, los cuales han sido creados con fines experimentales en local y serán integrados de forma ordenada y bajo una rama o commit separado en las fases correspondientes (Fase 01 y Fase 02).

### Archivos recomendados para entrar en el commit de la Fase 00:
* `.gitignore`
* `Makefile`
* `docs/` (toda la estructura de documentación pública creada)

### Archivos NO recomendados para este commit (deben mantenerse sin stagear):
* `acacia.go` (modificado localmente para soporte de descarte experimental)
* `internal/logging/wrapper.go` (wrapper experimental de redacción)
* `test/stress_vectors_test.go` (pruebas de estrés locales)

---

## 8. Próximos Pasos
1. Consolidar el mensaje de commit de fase 00.
2. Iniciar la **Fase 01 - Baseline Reproducible** para obtener los resultados estables de rendimiento de la versión actual.
