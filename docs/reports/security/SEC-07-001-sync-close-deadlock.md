# Security Finding

## Finding ID: SEC-07-001

### Metadata
- **Severity:** High
- **Vector Type:** Deadlock
- **Date Identified:** 2026-07-08
- **Status:** Remediated

### Vulnerability Description
El logger Acacia en su versión previa permitía bloquear indefinidamente a hilos productores de logs bajo la política `BlockOnFull` si el logger era cerrado mediante `Close()`. Esto ocurría porque los productores quedaban atrapados intentando enviar al canal `_log.events` sin selectores alternativos, y el canal nunca era drenado o desbloqueado de forma asíncrona, provocando un bloqueo completo en hilos críticos de la aplicación.

### Impact
Denegación de servicio (DoS) del proceso de producción o del servicio completo de la aplicación, ya que los hilos de ejecución que emiten logs bajo congestión quedan colgados de forma indefinida, agotando pools de hilos u otras conexiones del servidor.

### Code Snippet Reference
Antes de la corrección:
```go
case BlockOnFull:
    _log.events <- ev
    atomic.AddUint64(&_log.enqueuedSeq, 1)
```

### Proof of Concept (PoC)
1. Iniciar logger con buffer de tamaño 5 y política `BlockOnFull`.
2. Lanzar 10 goroutines escribiendo logs concurrentemente para llenar el buffer.
3. Llamar a `lg.Close()` desde la goroutine principal.
4. Las goroutines productoras bloqueadas en el canal se quedan colgadas de forma permanente, impidiendo el apagado correcto y colgando la aplicación.

### Remediation Plan
Se rediseñó la función de encolado `enqueueEvent` utilizando sentencias `select` junto con `case <-_log.done:`. Al cerrar el canal `_log.done` al inicio de `Close()`, todas las goroutines de envío bloqueadas se liberan instantáneamente de forma segura, descartando los mensajes pendientes hacia la cola y finalizando sin deadlocks.
