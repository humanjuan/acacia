package acacia_test

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
	"github.com/humanjuan/acacia/v2/internal/logging"
)

// Helper to set GOMAXPROCS=1
func limitCPU() func() {
	prev := runtime.GOMAXPROCS(1)
	return func() {
		runtime.GOMAXPROCS(prev)
	}
}

// Vector 1: Tormenta de Allocations y Reflection (El costo del JSON)
func TestVector1AllocationStorm(t *testing.T) {
	defer limitCPU()()

	// Enable profiling
	cpuFile, err := os.Create("vector1_cpu.pprof")
	if err != nil {
		t.Fatal(err)
	}
	defer cpuFile.Close()
	pprof.StartCPUProfile(cpuFile)
	defer pprof.StopCPUProfile()

	tmpDir := t.TempDir()
	lg, err := acacia.Start("stress_v1.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(500_000),
		acacia.WithBufferCap(256<<10),
		acacia.WithFlushInterval(100*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	lg.StructuredJSON(true)
	defer lg.Close()

	// Inyectar logs con payload complejo
	// 50,000 logs por segundo por 3 segundos = 150,000 logs
	const totalLogs = 150_000
	const rateLimit = 50_000 // logs per second
	ticker := time.NewTicker(time.Second / rateLimit)
	defer ticker.Stop()

	t.Log("Starting Vector 1: Allocation Storm...")
	start := time.Now()

	for i := 0; i < totalLogs; i++ {
		// Payload complejo con mapas anidados, strings y enteros
		payload := map[string]interface{}{
			"log_id":    i,
			"timestamp": time.Now().UnixNano(),
			"event":     "user_action_performed",
			"metadata": map[string]interface{}{
				"nested_key_1": "nested_value_1",
				"nested_key_2": 42,
				"nested_array": []interface{}{"a", "b", 100},
			},
			"user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		}
		lg.Info(payload)
		<-ticker.C
	}

	elapsed := time.Since(start)
	t.Logf("Finished Vector 1 in %v. Total logs sent: %d", elapsed, totalLogs)

	// Dump Alloc profile
	allocFile, err := os.Create("vector1_allocs.pprof")
	if err != nil {
		t.Fatal(err)
	}
	defer allocFile.Close()
	pprof.WriteHeapProfile(allocFile)
}

// Vector 2: Estrés de Redacción (El costo de sanitizar)
func TestVector2RedactStress(t *testing.T) {
	defer limitCPU()()

	// Enable profiling
	cpuFile, err := os.Create("vector2_cpu.pprof")
	if err != nil {
		t.Fatal(err)
	}
	defer cpuFile.Close()
	pprof.StartCPUProfile(cpuFile)
	defer pprof.StopCPUProfile()

	tmpDir := t.TempDir()
	lg, err := acacia.Start("stress_v2.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(500_000),
		acacia.WithBufferCap(256<<10),
	)
	if err != nil {
		t.Fatal(err)
	}
	lg.StructuredJSON(true)
	defer lg.Close()

	wrapper := logging.NewLogger(lg)

	// Simulación de cabeceras masivas
	headers := make(map[string]interface{})
	for i := 0; i < 20; i++ {
		headers[fmt.Sprintf("cookie_%d", i)] = fmt.Sprintf("session_token_value_xyz_%d", i)
	}
	headers["Authorization"] = "Bearer token_secret_1234567890_abcdef"
	headers["X-Access-Token"] = "token_another_secret_99999"
	headers["Host"] = "localhost:8080"
	headers["User-Agent"] = "Go-http-client/1.1"
	headers["X-Trash-1"] = "garbage_header_value_1"
	headers["X-Trash-2"] = "garbage_header_value_2"

	// 50,000 logs de redacción
	const iterations = 50_000
	t.Log("Starting Vector 2: Redaction Stress...")
	start := time.Now()

	for i := 0; i < iterations; i++ {
		// Creamos una copia para simular el map recibido por petición HTTP
		reqHeaders := make(map[string]interface{})
		for k, v := range headers {
			reqHeaders[k] = v
		}
		// Inyectamos a través del wrapper para que haga redactRecursive
		wrapper.Info(reqHeaders)
	}

	elapsed := time.Since(start)
	t.Logf("Finished Vector 2 in %v", elapsed)

	allocFile, err := os.Create("vector2_allocs.pprof")
	if err != nil {
		t.Fatal(err)
	}
	defer allocFile.Close()
	pprof.WriteHeapProfile(allocFile)
}

// Vector 3: Saturación de Cola y Penalización de Descarte (Drop Penalty)
func TestVector3DropPenalty(t *testing.T) {
	defer limitCPU()()

	// Enable profiling
	cpuFile, err := os.Create("vector3_cpu.pprof")
	if err != nil {
		t.Fatal(err)
	}
	defer cpuFile.Close()
	pprof.StartCPUProfile(cpuFile)
	defer pprof.StopCPUProfile()

	tmpDir := t.TempDir()
	// Reducir la cola a 8192 y activar dropOnFull
	lg, err := acacia.Start("stress_v3.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(8192),
		acacia.WithBufferCap(256<<10),
		acacia.WithDropOnFull(true),
		acacia.WithFlushInterval(1*time.Second), // Flush espaciado para asegurar saturación rápida
	)
	if err != nil {
		t.Fatal(err)
	}
	defer lg.Close()

	// Bombardear con goroutines concurrentes para saturar rápidamente el canal
	const workers = 10
	const logsPerWorker = 500_000
	var wg sync.WaitGroup
	wg.Add(workers)

	t.Log("Starting Vector 3: Drop Penalty (Queue Saturation)...")
	start := time.Now()

	for w := 0; w < workers; w++ {
		go func(workerID int) {
			defer wg.Done()
			msg := "Log storm line that will overflow the channel size of 8192 and hit the select default branch"
			for i := 0; i < logsPerWorker; i++ {
				lg.Info(msg)
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	dropped := lg.Dropped()

	t.Logf("Finished Vector 3 in %v. Total dropped: %d", elapsed, dropped)

	allocFile, err := os.Create("vector3_allocs.pprof")
	if err != nil {
		t.Fatal(err)
	}
	defer allocFile.Close()
	pprof.WriteHeapProfile(allocFile)
}

// Benchmark endpoints for comparison
func BenchmarkVector1JSON(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_v1.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	lg.StructuredJSON(true)
	defer lg.Close()

	payload := map[string]interface{}{
		"log_id":    100,
		"timestamp": time.Now().UnixNano(),
		"event":     "user_action_performed",
		"metadata": map[string]interface{}{
			"nested_key_1": "nested_value_1",
			"nested_key_2": 42,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info(payload)
	}
}

func BenchmarkVector2Redact(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_v2.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	lg.StructuredJSON(true)
	defer lg.Close()
	wrapper := logging.NewLogger(lg)

	headers := map[string]interface{}{
		"cookie_1":      "session_token_value_xyz_1",
		"cookie_2":      "session_token_value_xyz_2",
		"Authorization": "Bearer token_secret_1234567890_abcdef",
		"Host":          "localhost:8080",
		"User-Agent":    "Go-http-client/1.1",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wrapper.Info(headers)
	}
}

func BenchmarkVector3Drop(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_v3.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(8192),
		acacia.WithDropOnFull(true),
		acacia.WithFlushInterval(1*time.Second),
	)
	defer lg.Close()

	// Fill queue initially
	for i := 0; i < 9000; i++ {
		lg.Info("warmup log")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info("dropped log message")
	}
}
