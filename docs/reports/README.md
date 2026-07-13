# Reportes Públicos de Fases (Phase Reports)

Este directorio almacena los reportes de cierre de cada fase de optimización y reestructuración de Acacia.

## Índice de Reportes
* [Reporte de Fase 00: Organización del Repositorio](phase-00-repo-organization-report.md)
* [Reporte de Fase 01: Baseline reproducible](phase-01-baseline-report.md)
* [Reporte de Fase 02: API estructurada tipada](phase-02-typed-api-report.md)
* [Reporte de Fase 03: Encoder JSON append-only](phase-03-json-encoder-report.md)
* [Reporte de Fase 04: Redacción y seguridad de datos sensibles](phase-04-redaction-security-report.md)
* [Reporte de Fase 05: Backpressure, cola y memoria](phase-05-backpressure-queue-memory-report.md)
* [Reporte de Fase 06: Filesystem, permisos y rotación segura](phase-06-filesystem-permissions-rotation-report.md)
* [Reporte de Fase 07: Semántica formal de Sync y Close](phase-07-sync-close-semantics-report.md)
* [Reporte de Fase 08: Documentación, migración y release](phase-08-docs-migration-release-report.md)
* [Reporte de Validación Hard de Performance del Release Candidate](release-candidate-hard-performance-validation-report.md)
* [Reporte de Diagnóstico de Fase 09](phase-09-diagnostics-report.md)
* [Reporte de Optimización Incremental de Fase 09](phase-09-incremental-optimization-report.md)
* [Reporte de Hoja de Ruta de Fase 10: Madurez de JSON Estructurado](phase-10-structured-json-roadmap-report.md)
* [Reporte de Contrato de API de Fase 10: Madurez de JSON Estructurado](phase-10-api-contract-report.md)
* [Reporte de Hardening del Builder de JSON Plano de Fase 10](phase-10-flat-json-builder-hardening-report.md)




## Hallazgos de Seguridad (Security Findings)
* [SEC-01-001: Fallo de escape de caracteres de control en JSON estructurado](security/SEC-01-001-json-control-characters-escaping-failure.md)
* [SEC-06-001: Path Traversal en Start](security/SEC-06-001-filesystem-path-traversal.md)
* [SEC-06-002: Symlink Overwrite en Rotación](security/SEC-06-002-symlink-rotation-overwrite.md)
* [SEC-06-003: Permisos Inseguros en Filesystem](security/SEC-06-003-insecure-log-file-permissions.md)
* [SEC-07-001: Deadlock en BlockOnFull durante Close](security/SEC-07-001-sync-close-deadlock.md)
* [SEC-07-002: Panic por escritura post-cierre](security/SEC-07-002-write-after-close-panic.md)
* [SEC-07-003: Pérdida de logs aceptados en Close](security/SEC-07-003-accepted-log-loss-on-close.md)

## Reglas para nuevos reportes:
* Cada reporte debe seguir la estructura definida en [public-phase-report.template.md](../templates/public-phase-report.template.md).
* Asegurar la exclusión de cualquier dato de infraestructura o ruta local privada antes de realizar el commit.
