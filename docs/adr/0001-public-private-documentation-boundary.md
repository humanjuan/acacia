# ADR 0001: Límite entre Documentación Pública y Privada

## Estatus
Aceptado

## Contexto
Durante las auditorías de rendimiento y seguridad de Acacia, se generan archivos de perfilado (pprof), volcados de logs y reportes técnicos detallados. Estos archivos contienen nombres de usuario locales, rutas absolutas del disco duro, y potencialmente credenciales o tokens de infraestructura de desarrollo. Subir esta información de manera involuntaria al repositorio de Git expone datos sensibles y viola las buenas prácticas de seguridad.

## Decisión
Establecer un límite estricto entre los contenidos del repositorio público y las evidencias locales:
1. **Directorio `.private/` y Carpetas `private/` Locales:** No se versionarán en Git. Todo el contenido almacenado aquí (pprof, trazas, logs, secretos) queda estrictamente excluido a través de `.gitignore`.
2. **Reportes Públicos Sanitizados:** Los reportes de fase en Git (bajo `docs/reports/`) solo incluirán estadísticas consolidadas y métricas reproducibles sin exponer información del host.
3. **Validación Sistemática:** Se establece el uso obligatorio de `git check-ignore` y `git ls-files` como parte del checklist de pre-commit para garantizar que ningún archivo confidencial quede bajo el control de Git.

## Consecuencias
* **Positivas:** Seguridad mejorada frente a fugas de datos locales, repo limpio de binarios pprof pesados.
* **Negativas:** Los perfiles locales deben ser archivados de forma manual e individual por los desarrolladores en sus respectivas estaciones de trabajo si desean conservarlos a largo plazo.
