package acacia_test

import (
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkPhase09StatsRead(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_stats_read.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = lg.Stats()
	}
}

func BenchmarkPhase09DroppedCounterRead(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_dropped_read.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = lg.Dropped()
	}
}

func BenchmarkPhase09StatsUnderWriteContention(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_stats_contention.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(500000),
	)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var idx int
		for pb.Next() {
			if idx%10 == 0 {
				_ = lg.Stats()
			} else {
				lg.Info("active logging contention message")
			}
			idx++
		}
	})
}
