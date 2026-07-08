# Reportes Públicos de Fases (Phase Reports)

Este directorio almacena los reportes de cierre de cada fase de optimización y reestructuración de Acacia.

## Índice de Reportes
* [Reporte de Fase 00: Organización del Repositorio](phase-00-repo-organization-report.md)
* [Reporte de Fase 01: Baseline reproducible](phase-01-baseline-report.md)
* [Reporte de Fase 02: API estructurada tipada](phase-02-typed-api-report.md)
* [Reporte de Fase 03: Encoder JSON append-only](phase-03-json-encoder-report.md)
* [Reporte de Fase 04: Redacción y seguridad de datos sensibles](phase-04-redaction-security-report.md)

## Hallazgos de Seguridad (Security Findings)
* [SEC-01-001: Fallo de escape de caracteres de control en JSON estructurado](security/SEC-01-001-json-control-characters-escaping-failure.md)

## Reglas para nuevos reportes:
* Cada reporte debe seguir la estructura definida en [public-phase-report.template.md](../templates/public-phase-report.template.md).
* Asegurar la exclusión de cualquier dato de infraestructura o ruta local privada antes de realizar el commit.
