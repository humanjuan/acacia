# Security Finding

## Finding ID: SEC-07-002

### Metadata
- **Severity:** High
- **Vector Type:** Send on Closed Channel
- **Date Identified:** 2026-07-08
- **Status:** Remediated

### Vulnerability Description
El logger Acacia originalmente cerraba directamente el canal asíncrono de eventos `_log.events` dentro del bloque de exclusión de `Close()`. Dado que múltiples goroutines de producción encolaban logs de forma concurrente sin bloqueos de sincronización mutua, cualquier intento de escribir logs mediante `Info()`, `Warn()`, `Error()`, etc., de manera concurrente o posterior a la invocación de `Close()` generaba un pánico de tipo `send on closed channel` en el runtime de Go.

### Impact
Parada abrupta y crash completo del proceso de la aplicación en producción debido a excepciones no capturadas al emitir trazas de log durante o después de la fase de apagado del servicio.

### Code Snippet Reference
Antes de la corrección:
```go
func (_log *Log) Close() {
    _log.closeOnce.Do(func() {
        ...
        if _log.events != nil {
            close(_log.events)
        }
        ...
    })
}
```

### Proof of Concept (PoC)
1. Lanzar una goroutine escribiendo logs concurrentes de forma indefinida en Acacia.
2. Llamar concurrentemente a `lg.Close()` desde la goroutine principal.
3. El runtime de Go lanza `panic: send on closed channel` provocando la caída de la aplicación.

### Remediation Plan
Se rediseñó la semántica de cierre de Acacia para **nunca cerrar físicamente** el canal `_log.events`. En su lugar, se implementó un control de estado atómico de tipo máquina de estados (`stateOpen`, `stateClosing`, `stateClosed`). El método `Close()` avisa al escritor a través de un canal de control dedicado (`_log.control`) utilizando la señal `isClose: true`, y cualquier llamada de log posterior a `Close()` verifica de forma atómica el estado y descarta el mensaje limpiando los buffers de inmediato sin provocar panics.
