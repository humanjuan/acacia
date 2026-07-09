# Guía de Pruebas y Benchmarking (Testing & Benchmarking)

Esta guía detalla los comandos oficiales y directrices técnicas para validar el rendimiento, la seguridad y la concurrencia en Acacia.

---

## 1. Comandos de Pruebas Estándar

### Ejecución de Pruebas Unitarias
Para correr la suite de pruebas regular:
```bash
go test ./...
```

### Detección de Condiciones de Carrera (Race Detector)
Es obligatorio que todos los cambios pasen el race detector limpio:
```bash
go test -race ./...
```

---

## 2. Pruebas de Desempeño (Benchmarks)

Para garantizar la fiabilidad del análisis de rendimiento:
* **Filtro de ejecución (`-run '^$'`)**: Evita que se ejecuten tests unitarios durante la sesión de benchmark para no distorsionar las métricas.
* **Repetición (`-count=10`)**: Se deben realizar al menos 10 iteraciones de cada benchmark para mitigar las fluctuaciones del planificador del sistema operativo.
* **Memoria (`-benchmem`)**: Reporta las asignaciones de memoria en bytes y el contador de allocations por operación.

### Benchmarks Multihilo (Uso de CPU Completo)
```bash
go test ./... -bench=. -benchmem -run '^$' -count=10
```

### Benchmark Hostil (GOMAXPROCS=1)
Prueba crítica obligatoria para medir el impacto de la sincronización de goroutines sin paralelismo real de hardware:
```bash
GOMAXPROCS=1 go test ./... -bench=. -benchmem -run '^$' -count=10
```

### Benchmark de Concurrencia Regulada (GOMAXPROCS=2)
```bash
GOMAXPROCS=2 go test ./... -bench=. -benchmem -run '^$' -count=10
```

---

## 3. Pruebas de Seguridad y Fuzzing

El fuzzing se utiliza en el encoder JSON y en la redacción para detectar desbordamientos o caracteres de escape no controlados.

```bash
go test ./... -run Fuzz -fuzz=Fuzz -fuzztime=60s
```

---

## 4. Pruebas de Estrés (Stress Testing)

Para validar el descarte de logs (cola llena) y backpressure:
```bash
go test ./... -run TestStress -count=1
```

---

## 5. Automatización mediante Makefile

El repositorio cuenta con un `Makefile` en su raíz que expone atajos rápidos para el desarrollador local:

| Comando | Acción equivalente | Propósito |
|---|---|---|
| `make test` | `go test ./...` | Suite básica de validación lógica |
| `make race` | `go test -race ./...` | Chequeo de seguridad concurrente |
| `make bench` | `go test ./... -bench=. -benchmem -run '^$$' -count=10` | Medición de rendimiento completa |
| `make bench-gmp1` | `GOMAXPROCS=1 ...` | Prueba hostil de asignaciones en un hilo |
| `make fuzz` | `go test ./... -run Fuzz -fuzz=Fuzz -fuzztime=60s` | Pruebas de fuzzing automatizadas |
| `make audit` | Ejecuta `test`, `race` y `bench-gmp1` | Auditoría de pre-entrega local completa |

---

## 6. Captura de Perfiles (pprof)

Al perfilar código para diagnosticar cuellos de botella:
1. Asegúrate de guardar los perfiles en `.private/pprof/` para no subirlos a Git accidentalmente.
2. Ejemplo de ejecución y almacenamiento seguro:
   ```bash
   go test -cpuprofile=.private/pprof/cpu.out -memprofile=.private/pprof/mem.out -bench=. -run=^$ ./...
   ```
3. Para visualizar localmente el perfil sin filtrar:
   ```bash
   go tool pprof -http=:8080 .private/pprof/cpu.out
   ```

---

## 7. Protocolos de Testing Hard (Fase 09)

Para la validación del release candidate y el análisis de regresiones, se establece un protocolo estricto de testing que complementa las herramientas básicas.

### Comparación Estadística con Benchstat
Una comparación manual o mediante scripts no especializados puede ignorar fluctuaciones estadísticas. Se requiere:
1. Instalar la herramienta oficial:
   ```bash
   go install golang.org/x/perf/cmd/benchstat@latest
   ```
2. Capturar el benchmark del baseline (`v2.3.0` o `old`):
   ```bash
   git checkout v2.3.0
   go test ./... -bench=. -benchmem -run '^$' -count=20 > .private/bench/old.txt
   ```
3. Capturar el benchmark del código optimizado (`new`):
   ```bash
   git checkout dev
   go test ./... -bench=. -benchmem -run '^$' -count=20 > .private/bench/new.txt
   ```
4. Ejecutar la comparación estadística:
   ```bash
   benchstat .private/bench/old.txt .private/bench/new.txt
   ```

### Pruebas de Soak (Remojo)
Para descartar fugas de memoria o degradaciones en periodos de ejecución prolongados, se implementan soak tests bajo la variable `ACACIA_HARD_SOAK=1`:
```bash
ACACIA_HARD_SOAK=1 go test ./test -run TestPhase09Soak -v -timeout=15m
```

### Matriz de Escenarios Evaluados
Todos los perfiles de CPU, Memoria y Trazas deben generarse de forma privada:
```bash
go test -cpuprofile=.private/pprof/cpu.out -memprofile=.private/pprof/mem.out -bench=. -run=^$ ./...
```

La matriz a cubrir incluye mediciones aisladas para:
* **Escritura plana**: `text/plain` y `bytes`.
* **I/O y buffer**: Aislamiento del `io.Writer`.
* **JSON**: Serialización tradicional vs Builder.
* **Seguridad**: Redaction en el hot path.
* **Flujo**: Backpressure y Sync/Close.
* **Rotación**: Rotación por tamaño, daily rotation determinística y backup rotation.
* **Integración**: Overhead del OTel adapter opcional.

