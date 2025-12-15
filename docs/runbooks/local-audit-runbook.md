# Runbook de Auditoría Local (Local Audit Runbook)

Este runbook describe los pasos obligatorios para auditar el código localmente antes de proponer cualquier cambio al repositorio público.

---

## Paso 1: Pruebas de Correctitud Lógica y Concurrencia
Ejecuta la suite de pruebas unitarias y activa el race detector para descartar condiciones de carrera en el pool de buffers o la cola asíncrona:
```bash
go test ./...
go test -race ./...
```

---

## Paso 2: Ejecución de Benchmarks de Contención (Hostiles)
Ejecuta los benchmarks de contención forzando un solo hilo para simular hardware de bajo recurso y maximizar la competencia por el planificador de Go:
```bash
GOMAXPROCS=1 go test ./... -bench=. -benchmem -run '^$' -count=10 > .private/bench/bench_gmp1.txt
```
Analiza el archivo `.private/bench/bench_gmp1.txt` para garantizar que no haya regresiones en `allocs/op` o en `ns/op`.

---

## Paso 3: Generación de Perfiles y Almacenamiento Seguro
Si detectas regresiones de rendimiento, genera los perfiles de CPU y Heap, guardándolos estrictamente en el directorio privado:
```bash
go test -cpuprofile=.private/pprof/cpu.out -memprofile=.private/pprof/mem.out -bench=. -run=^$ ./...
```

---

## Paso 4: Validación de Fugas en Git (Exclusión Check)
Antes de preparar un commit, ejecuta estos comandos para verificar que no estás incluyendo archivos privados locales en el commit:

1. **Chequeo de Archivos Ignorados:**
   ```bash
   git check-ignore -v .private/README.md
   git check-ignore -v .private/reports/phase-00-private-evidence.md
   ```
   *Ambos comandos deben imprimir la regla de exclusión correspondiente de tu `.gitignore`.*

2. **Chequeo de tracking accidental:**
   ```bash
   git ls-files .private docs/private reports/private
   ```
   *Este comando debe retornar una salida completamente vacía.*

3. **Status Limpio:**
   ```bash
   git status --short
   ```
   *Verifica que no aparezca ningún archivo `.pprof`, `.prof`, `.out` ni `.trace` listado.*
