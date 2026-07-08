# Security Finding

## Finding ID: SEC-07-003

### Metadata
- **Severity:** Medium
- **Vector Type:** Leak / Log Loss
- **Date Identified:** 2026-07-08
- **Status:** Remediated

### Vulnerability Description
El logger asíncrono no garantizaba el vaciado (drain) completo de los eventos de log que habían sido formalmente aceptados en el buffer/cola de Acacia antes de finalizar el cierre del archivo físico. Esto ocurría porque la rutina del escritor finalizaba precipitadamente cuando se cerraba el canal sin asegurar el procesamiento del remanente encolado de forma secuencial.

### Impact
Pérdida silenciosa de registros críticos de auditoría y trazas de diagnóstico de la fase de cierre de la aplicación. Esto es crítico en auditorías de seguridad, ya que los logs previos al apagado inmediato (ej. trazas de errores fatales, alertas de intrusión, desconexión de servicios) se perdían de forma irreversible.

### Code Snippet Reference
Antes de la corrección:
```go
		case ev, ok := <-_log.events:
			if !ok {
				_log.flush()
				return
			}
```

### Proof of Concept (PoC)
1. Encolar 50 mensajes de log identificables.
2. Llamar inmediatamente a `lg.Close()`.
3. Validar el archivo físico resultante y notar que solo se escribieron una parte de las 50 líneas, perdiendo silenciosamente los eventos finales.

### Remediation Plan
Se implementó un protocolo de drenaje determinista dentro del manejador de la señal `isClose` del canal de control. El escritor asíncrono ejecuta un bucle de lectura selectiva no bloqueante sobre `_log.events` hasta vaciar completamente todos los elementos presentes en la cola antes de proceder con el `flush` final y el `f.Close()` físico del archivo descriptor.
