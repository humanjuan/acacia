// bench_test.go - Compatible con https://github.com/betterstack-community/go-logging-benchmarks
package acacia_test

import (
	"strings"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

// -----------------------------------------------------------
//                   B E N C H M A R K S
// -----------------------------------------------------------

func Benchmark_byte(b *testing.B) {
	lg, _ := acacia.Start(
		"bench.log",
		b.TempDir(),
		acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
		acacia.WithDrainBurst(1024),
		acacia.WithFlushInterval(10*time.Millisecond),
	)
	defer lg.Close()

	byteMessage := []byte("The quick brown fox jumps over the lazy dog")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoBytes(byteMessage)
	}
}

func Benchmark_byte_Parallel(b *testing.B) {
	lg, _ := acacia.Start(
		"bench.log",
		b.TempDir(),
		acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),              // channel capacity (producers→queue)
		acacia.WithBufferCap(256<<10),                 // writer’s internal buffers: 256 KB
		acacia.WithDrainBurst(1024),                   // drain more per wake-up for better batching
		acacia.WithFlushInterval(10*time.Millisecond)) // lower latency; increase for max throughput-only)
	defer lg.Close()

	byteMessage := []byte("The quick brown fox jumps over the lazy dog")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lg.InfoBytes(byteMessage)
		}
	})
}

func Benchmark_byte_1KB(b *testing.B) {
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
	byteMessage := []byte(msg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoBytes(byteMessage)
	}
}

func Benchmark_byte_Parallel_1KB(b *testing.B) {
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
	byteMessage := []byte(msg)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lg.InfoBytes(byteMessage)
		}
	})
}

/*
# Benchmark básico
go test -bench=Benchmark_byte -benchmem -benchtime=5s

# Benchmark paralelo (importante para producción)
go test -bench=Benchmark_byte_Parallel -benchmem -benchtime=5s

# Con mensajes de 1KB
go test -bench=Benchmark_byte_1KB -benchmem -benchtime=5s

# Paralelo + 1KB (concurrencia real)
go test -bench=Benchmark_byte_Parallel_1KB -benchmem -benchtime=5s
*/
