# Plan de Hardening de Seguridad (Security Hardening Plan)

Este documento detalla la estrategia de seguridad y mitigación de vectores de ataque aplicables a Acacia.

---

## 1. Vectores de Ataque Críticos

### A. Inyección de Logs (Log Injection / Log Forgery)
* **Riesgo:** Si un atacante inyecta saltos de línea (`\n` o `\r`) dentro de un parámetro de usuario, puede crear líneas falsas en el log que imiten logs del sistema (ej. inyectar una línea con nivel `[CRITICAL]` o alterar registros de auditoría).
* **Mitigación:** El encoder JSON de la **Fase 03** debe escapar de forma agresiva cualquier retorno de carro, salto de línea o barra invertida.

### B. Path Traversal y Escritura Arbitraria
* **Riesgo:** Si el nombre del archivo de log se configura dinámicamente con entradas de usuario, un atacante podría inyectar caracteres de retroceso de directorio `../` para escribir logs sobre archivos críticos del sistema (como `.ssh/authorized_keys` o configuraciones del sistema operativo).
* **Mitigación:** La **Fase 06** implementará validaciones estrictas sobre el nombre del archivo de log y el directorio base, rechazando cualquier intento de manipulación de rutas.

### C. Permisos Inseguros en el Filesystem
* **Riesgo:** Crear archivos de logs con máscaras de permisos excesivamente permisivas (como `0666` o `0777`) expone información sensible a otros usuarios del mismo host físico.
* **Mitigación:** Asegurar que todos los archivos creados tengan permisos restrictivos por defecto (como `0600`), permitiendo únicamente lectura/escritura al propietario del proceso.

### D. Ataques de Enlaces Simbólicos (Symlink Attacks)
* **Riesgo:** Durante el proceso de rotación, un atacante local con acceso al directorio de logs podría crear enlaces simbólicos apuntando a archivos críticos del sistema. Si el logger sigue el enlace simbólico al renombrar o recrear el archivo, podría sobrescribir datos confidenciales de la máquina.
* **Mitigación:** Configurar validaciones mediante `os.Lstat` para asegurar que no se interactúe con enlaces simbólicos.

---

## 2. Estrategia de Redacción de Datos Sensibles (Fase 04)

* **Denylist Dinámica y Estática:** Búsqueda rápida de palabras claves (como `authorization`, `cookie`, `token`, `password`, `secret`).
* **Redacción Eficiente:** En lugar de recorrer mapas de manera reflexiva y recursiva, el builder fluente detectará y redactará los campos marcados como sensibles o confidenciales en tiempo de escritura al búfer.

---

## 3. Pruebas de Seguridad Continuas

* **Fuzz Testing:** Suite de fuzzing dedicada para inyectar cadenas corruptas e intentar saltar el codificador/redactor.
* **Path Traversal Test Suite:** Pruebas unitarias dedicadas en la **Fase 06** que intentarán pasar nombres de archivos y rutas con secuencias `../` para asegurar que el sistema falle de forma segura y controlada.
