package acacia_test

import (
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkPhase09StateCheck(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_state_check.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = lg.Status()
	}
}

func BenchmarkPhase09StateCheckParallel(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_state_check_par.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = lg.Status()
		}
	})
}
