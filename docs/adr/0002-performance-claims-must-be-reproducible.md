# ADR 0002: Requisitos de Reproducibilidad en Claims de Rendimiento

## Estatus
Aceptado

## Contexto
El rendimiento de Acacia es uno de sus principales atributos de venta y diseño (claims de ~6M logs/sec y 0 allocs/op). Sin embargo, en el desarrollo de software es fácil cometer errores de medición o publicar afirmaciones falsas que minan la confianza del usuario final. En la versión actual de Acacia, el camino structured JSON genera asignaciones dinámicas y overhead que contradicen los claims generales de "0 allocations".

## Decisión
Para garantizar la veracidad de toda métrica de rendimiento reportada públicamente:
1. **Comandos Reproducibles:** Todo número o claim de rendimiento en la documentación o en el README debe contar con un comando `go test` reproducible documentado.
2. **Prueba Hostil con `GOMAXPROCS=1`:** Se exige validar los benchmarks bajo la restricción de `GOMAXPROCS=1` como prueba extrema obligatoria para revelar la contención y la presión real sobre el recolector de basura.
3. **Clasificación por Ruta:** No se permiten claims generales de "Zero-Allocation" que cubran de forma ambigua todas las funcionalidades. Los benchmarks y reportes deben separar estrictamente los resultados de:
   * Fast path (String / []byte)
   * Structured path (JSON Builder v3)
   * Legacy path (Compatibilidad con mapas v2)
   * Sanitized path (Redacción de campos sensibles)
4. **Comparaciones Transparentes:** No se publicarán comparaciones contra otros loggers (como Zap, Zerolog) a menos que se incluya la metodología de prueba exacta y el código fuente del benchmark comparativo.

## Consecuencias
* **Positivas:** Mayor transparencia técnica, eliminación de falsos positivos en las mediciones, y un desarrollo guiado por datos reproducibles reales.
* **Negativas:** Mayor esfuerzo requerido por el desarrollador para documentar y correr múltiples combinaciones de configuraciones y ambientes de prueba.
