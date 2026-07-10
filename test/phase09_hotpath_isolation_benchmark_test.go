package acacia_test

import (
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkPhase09HotPathStringSmall(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_hotpath_str_small.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(500000),
	)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info("small string message")
	}
}

func BenchmarkPhase09HotPathBytesSmall(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_hotpath_bytes_small.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(500000),
	)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	msg := []byte("small byte message")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Write(msg)
	}
}

func BenchmarkPhase09HotPathString1KB(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_hotpath_str_1kb.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(500000),
	)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	msg := string(make([]byte, 1024))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info(msg)
	}
}

func BenchmarkPhase09HotPathBytes1KB(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_hotpath_bytes_1kb.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(500000),
	)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	msg := make([]byte, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Write(msg)
	}
}

func BenchmarkPhase09HotPathString4KB(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_hotpath_str_4kb.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(500000),
	)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	msg := string(make([]byte, 4096))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info(msg)
	}
}

func BenchmarkPhase09HotPathBytes4KB(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_hotpath_bytes_4kb.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(500000),
	)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	msg := make([]byte, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Write(msg)
	}
}
