package acacia_test

import (
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkPhase09QueueEnqueueOpenState(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_queue_open.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(500000),
	)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info("log line")
	}
}

func BenchmarkPhase09QueueDropNewestFull(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_queue_drop_newest.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(10),
		acacia.WithBackpressurePolicy(acacia.DropNewest),
	)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info("log line")
	}
}

func BenchmarkPhase09QueueDropOldestFull(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_queue_drop_oldest.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(10),
		acacia.WithBackpressurePolicy(acacia.DropOldest),
	)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info("log line")
	}
}

func BenchmarkPhase09QueueBlockOnFull(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_queue_block.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(10),
		acacia.WithBackpressurePolicy(acacia.BlockOnFull),
	)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info("log line")
	}
}

func BenchmarkPhase09QueueDropDebugOnly(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_queue_drop_debug.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(10),
		acacia.WithBackpressurePolicy(acacia.DropDebugOnly),
	)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Debug("debug log line")
	}
}
