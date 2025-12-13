// bench_test.go - Compatible con https://github.com/betterstack-community/go-logging-benchmarks
package acacia_test

import (
	"strings"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func Benchmark_string(b *testing.B) {
	lg, _ := acacia.Start(
		"bench.log",
		b.TempDir(),
		acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
		acacia.WithDrainBurst(1024),
		acacia.WithFlushInterval(10*time.Millisecond))
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info("The quick brown fox jumps over the lazy dog")
	}
}

func Benchmark_string_Parallel(b *testing.B) {
	lg, _ := acacia.Start(
		"bench.log",
		b.TempDir(),
		acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
		acacia.WithDrainBurst(1024),
		acacia.WithFlushInterval(10*time.Millisecond))
	defer lg.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lg.Info("The quick brown fox jumps over the lazy dog")
		}
	})
}

func Benchmark_string_1KB(b *testing.B) {
	lg, _ := acacia.Start(
		"bench.log",
		b.TempDir(),
		acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
		acacia.WithDrainBurst(1024),
		acacia.WithFlushInterval(10*time.Millisecond))
	defer lg.Close()

	msg := strings.Repeat("X", 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info(msg)
	}
}

func Benchmark_string_Parallel_1KB(b *testing.B) {
	lg, _ := acacia.Start(
		"bench.log",
		b.TempDir(),
		acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
		acacia.WithDrainBurst(1024),
		acacia.WithFlushInterval(10*time.Millisecond))
	defer lg.Close()

	msg := strings.Repeat("X", 1024)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lg.Info(msg)
		}
	})
}

/*
# Benchmark básico
go test -bench=Benchmark_string -benchmem

# Benchmark paralelo (importante para producción)
go test -bench=Benchmark_string_Parallel -benchmem

# Con mensajes de 1KB
go test -bench=Benchmark_string_1KB -benchmem

# Paralelo + 1KB (concurrencia real)
go test -bench=Benchmark_string_Parallel_1KB -benchmem
*/
