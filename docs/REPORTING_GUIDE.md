# Guía de Reportes de Fase (Reporting Guide)

Al culminar cada una de las fases del Roadmap de Acacia, es obligatorio consolidar la información técnica y de rendimiento tanto en la documentación pública como en la privada.

---

## 1. Estructura Dual de Reportes

Cada hito de desarrollo requiere dos reportes:

```text
Fase Finalizada
  ├── Reporte Público (Sanitizado) -> docs/reports/phase-XX-report.md
  └── Evidencia Privada (Cruda)      -> .private/reports/phase-XX-private-evidence.md
```

### A. El Reporte Público (`docs/reports/`)
* **Propósito:** Informar a la comunidad y colaboradores externos del progreso, métricas de rendimiento sanitizadas y decisiones de seguridad adoptadas.
* **Seguridad:** No debe contener secretos, volcados de logs que expongan datos privados de la infraestructura local, ni rutas absolutas locales (ej. `/Volumes/Juan_Ext/...` o `/Users/username/...`).
* **Reproducibilidad:** Cualquier métrica de rendimiento (ns/op, B/op, allocs/op) debe estar acompañada por el comando exacto con el que se generó para que cualquier persona en hardware similar pueda reproducirla.

### B. El Reporte de Evidencia Privada (`.private/reports/`)
* **Propósito:** Documentar los resultados locales a nivel de bajo nivel (trazas, dumps de CPU/memoria) específicos del entorno de hardware del autor.
* **Contenido:**
  * Capturas de comandos completos ejecutados.
  * Rutas absolutas a los archivos de perfil `.pprof` almacenados en `.private/pprof/`.
  * Logs completos de salida de las pruebas de estrés.
  * Análisis detallado de cualquier vulnerabilidad local antes de que se parchee de forma pública.

---

## 2. Reglas de Redacción

* **Honestidad en las Pruebas:** Si una prueba o benchmark falla, se reporta de forma transparente junto con su diagnóstico. No se ocultan regresiones de rendimiento ni fallos de integración.
* **Uso de Plantillas:** Los reportes públicos deben seguir la estructura definida en [public-phase-report.template.md](../templates/public-phase-report.template.md).
* **Validación de Exclusión:** Antes de subir cualquier cambio público, verifica que los perfiles y el reporte privado estén en `.gitignore`.
