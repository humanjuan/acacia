package acacia_test

import (
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkEventBuilderDisabledLevel(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_disabled.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// DEBUG level is disabled by default since minimum level is INFO
		lg.DebugEvent().
			Str("service", "owl").
			Int("status", 200).
			Msg("disabled message")
	}
}

func BenchmarkEventBuilderSmall(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_small.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("service", "owl").
			Int("status", 200).
			Send()
	}
}

func BenchmarkEventBuilderSmallParallel(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_small_parallel.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lg.InfoEvent().
				Str("service", "owl").
				Int("status", 200).
				Send()
		}
	})
}

func BenchmarkEventBuilderStringFields(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_strings.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("k1", "val1").
			Str("k2", "val2").
			Str("k3", "val3").
			Send()
	}
}

func BenchmarkEventBuilderMixedFields(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_mixed.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	now := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("service", "owl").
			Int("status", 200).
			Bool("cached", true).
			Time("ts", now).
			Send()
	}
}

func BenchmarkEventBuilder1KB(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_1kb.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	// Build a string that makes the output ~1KB
	largeString := string(make([]byte, 100))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("k1", largeString).
			Str("k2", largeString).
			Str("k3", largeString).
			Str("k4", largeString).
			Str("k5", largeString).
			Str("k6", largeString).
			Str("k7", largeString).
			Str("k8", largeString).
			Str("k9", largeString).
			Send()
	}
}
