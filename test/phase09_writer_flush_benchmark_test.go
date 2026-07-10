package acacia_test

import (
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkPhase09WriterFlushSmallBatch(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_flush_small.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(10),
	)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info("small batch log line")
	}
}

func BenchmarkPhase09WriterFlushLargeBatch(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_flush_large.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(50000),
	)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info("large batch log line")
	}
}

func BenchmarkPhase09WriterLagSmallPayload(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_lag_small.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
	)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info("small payload log")
	}
}

func BenchmarkPhase09WriterLag1KBPayload(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_lag_1kb.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
	)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	payload := string(make([]byte, 1024))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info(payload)
	}
}

func BenchmarkPhase09WriterLag4KBPayload(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_lag_4kb.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
	)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	payload := string(make([]byte, 4096))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info(payload)
	}
}
