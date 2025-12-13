// bench_test.go - Compatible con https://github.com/betterstack-community/go-logging-benchmarks
package acacia_test

import (
	"strings"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

// example data
var testJSON = map[string]interface{}{
	"event": "user_auth",
	"user":  "juan@example.com",
	"ip":    "192.168.1.1",
}

func Benchmark_structured(b *testing.B) {
	lg, _ := acacia.Start(
		"bench.log",
		b.TempDir(),
		acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
		acacia.WithDrainBurst(1024),
		acacia.WithFlushInterval(10*time.Millisecond),
	)
	lg.StructuredJSON(true)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info("The quick brown fox jumps over the lazy dog")
	}
}

func Benchmark_structured_Parallel(b *testing.B) {
	lg, _ := acacia.Start(
		"bench.log",
		b.TempDir(),
		acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
		acacia.WithDrainBurst(1024),
		acacia.WithFlushInterval(10*time.Millisecond),
	)
	lg.StructuredJSON(true)
	defer lg.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lg.Info("The quick brown fox jumps over the lazy dog")
		}
	})
}

func Benchmark_structured_1KB(b *testing.B) {
	lg, _ := acacia.Start(
		"bench.log",
		b.TempDir(),
		acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
		acacia.WithDrainBurst(1024),
		acacia.WithFlushInterval(10*time.Millisecond),
	)
	lg.StructuredJSON(true)
	defer lg.Close()

	msg := strings.Repeat("X", 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info(msg)
	}
}

func Benchmark_structured_Parallel_1KB(b *testing.B) {
	lg, _ := acacia.Start(
		"bench.log",
		b.TempDir(),
		acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
		acacia.WithDrainBurst(1024),
		acacia.WithFlushInterval(10*time.Millisecond),
	)
	lg.StructuredJSON(true)
	defer lg.Close()

	msg := strings.Repeat("X", 1024)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lg.Info(msg)
		}
	})
}

func Benchmark_structured_Parallel_Fields(b *testing.B) {
	lg, _ := acacia.Start(
		"bench.log",
		b.TempDir(),
		"INFO",
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
		acacia.WithDrainBurst(1024),
		acacia.WithFlushInterval(10*time.Millisecond),
	)
	lg.StructuredJSON(true)
	defer lg.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lg.Info(testJSON)
		}
	})
}

/*
# Benchmark básico
go test -bench=Benchmark_structured -benchmem -benchtime=5s

# Benchmark paralelo (importante para producción)
go test -bench=Benchmark_structured_Parallel -benchmem -benchtime=5s

# Con mensajes de 1KB
go test -bench=Benchmark_structured_1KB -benchmem -benchtime=5s

# Paralelo + 1KB
go test -bench=Benchmark_structured_Parallel_1KB -benchmem -benchtime=5s
*/
