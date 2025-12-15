# Runbook de Preparación para Release (Release Readiness Runbook)

Este runbook detalla los pasos de verificación obligatorios a realizar antes de etiquetar y liberar una nueva versión de Acacia.

---

## 1. Verificaciones del Core y Pruebas
1. **Tests Unitarios Completos:**
   Todos los tests en todos los paquetes deben estar en verde:
   ```bash
   go test -v ./...
   ```
2. **Race Detector:**
   No debe reportarse ninguna alerta de concurrencia:
   ```bash
   go test -race ./...
   ```

---

## 2. Auditoría de Rendimiento (Límite Zero-Allocation)
1. **Validación de Allocations:**
   Ejecuta la suite completa de benchmarks y comprueba que:
   * Las funciones de fast path (`Info`, `Warn` con strings y `[]byte`) reportan **0 allocs/op**.
   * La Fluent API (Builder) reporta **0 allocs/op** en su uso base.
2. **Comparativa frente a Baseline:**
   Compara los tiempos de ejecución de la nueva versión contra la anterior para descartar regresiones de latencia superiores al 5%.

---

## 3. Lista de Chequeo de Seguridad
* **Sanitización de Retornos de Línea:** Verificar que el codificador JSON reemplace o escape los caracteres `\n` y `\r` para evitar Log Injection.
* **Permisos del Filesystem:** Confirmar que los archivos de logs creados por rotación o arranque sigan la máscara restrictiva `0600`.
* **Path Traversal:** Comprobar que los nombres de archivos pasados al logger no admitan secuencias `../` ni caracteres especiales que alteren directorios externos.

---

## 4. Validación de Limpieza y Exclusión de Datos
Antes de realizar el tag y push definitivo:
1. Asegura que no existan perfiles `.pprof` ni reportes privados bajo tracking ejecutando:
   ```bash
   git ls-files .private docs/private reports/private
   ```
   *Debe resultar en una lista vacía.*
2. Comprobar que no haya comentarios `TODO` o notas en el código que apunten a URLs locales o IPs del entorno privado de desarrollo.
3. Generar el reporte público final de la versión y actualizar el CHANGELOG en el `README.md`.
