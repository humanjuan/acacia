# Acacia Documentation

Bienvenido a la documentación pública de **Acacia**, el subsistema de logging asíncrono de alto rendimiento para Go. 

## Propósito de la Documentación
Este espacio centraliza las guías técnicas, el plan de trabajo (roadmap), las políticas de seguridad y rendimiento, y las directrices para colaboradores de Acacia. Su objetivo es asegurar que la evolución del logger sea transparente, segura y reproducible.

## Frontera Documental: Pública vs Privada
Para proteger la integridad del proyecto y de la infraestructura local:
* **Documentación Pública (`docs/`)**: Seguro para subir a GitHub. Contiene especificaciones, decisiones de arquitectura (ADR), metodologías de prueba y resúmenes de rendimiento sanitizados.
* **Documentación Privada (`.private/`)**: Ignorada por Git (nunca se sube al repositorio). Contiene volcados crudos de rendimiento, perfiles de `pprof`, datos de trazas del sistema, secretos y credenciales locales.

> [!WARNING]
> NUNCA subas perfiles de `pprof` crudos, volcados de logs ni secretos locales al repositorio público. Todos estos deben ubicarse en `.private/`.

## Índice de Documentación Pública
* [Roadmap de Mejora de Acacia](ACACIA_IMPROVEMENT_ROADMAP.md): Fases de refactorización y optimización de Acacia.
* [Contexto del Proyecto para Agentes AI](ACACIA_PROJECT_CONTEXT.md): Contexto operativo público para agentes AI, ChatGPT Projects y colaboradores.
* [Política de Documentación Pública/Privada](PUBLIC_PRIVATE_DOCUMENTATION_POLICY.md): Reglas de clasificación y exclusión de datos.
* [Guía de Pruebas y Benchmarking](TESTING_AND_BENCHMARKING.md): Comandos y entornos para ejecutar stress tests, fuzzing y profiling.
* [Plan de Hardening de Seguridad](SECURITY_HARDENING_PLAN.md): Estrategias para evitar log injection y sanitizar payloads.
* [Plan de Ingeniería de Rendimiento](PERFORMANCE_ENGINEERING_PLAN.md): Lineamientos para el diseño Zero-Allocation.
* [Estrategia de Migración](MIGRATION_STRATEGY.md): Plan de compatibilidad para la transición a la API builder (candidato a v3).
* [Guía de Reportes de Fase](REPORTING_GUIDE.md): Metodología de documentación al cierre de cada hito.
* [Decisiones de Diseño de Arquitectura (ADR)](adr/):
  * [ADR 0001: Límite entre Documentación Pública y Privada](adr/0001-public-private-documentation-boundary.md)
  * [ADR 0002: Requisitos de Reproducibilidad en Claims de Rendimiento](adr/0002-performance-claims-must-be-reproducible.md)
* [Reportes de Ejecución](reports/):
  * [Reporte de Fase 00: Organización del Repositorio](reports/phase-00-repo-organization-report.md)
* [Plantillas de Trabajo](templates/):
  * [Plantilla de Reporte Público](templates/public-phase-report.template.md)
  * [Plantilla de Evidencia Privada](templates/private-evidence-report.template.md)
  * [Plantilla de Resumen de Benchmarks](templates/benchmark-summary.template.md)
  * [Plantilla de Hallazgo de Seguridad](templates/security-finding.template.md)
* [Runbooks Operativos](runbooks/):
  * [Runbook de Auditoría Local](runbooks/local-audit-runbook.md)
  * [Runbook de Preparación para Release](runbooks/release-readiness-runbook.md)
 
## Estado Actual
* **Fase Activa:** `Fase 00 - Organización del Repositorio` (Cierre documental).
* **Próxima Fase:** `Fase 01 - Baseline Reproducible`.
