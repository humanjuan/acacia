package acacia_test

import (
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkBackpressureDropNewestFullQueue(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_drop_newest.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.DropNewest),
		acacia.WithFlushInterval(10*time.Second),
	)
	defer lg.Close()

	// Fill queue first
	for i := 0; i < 1005; i++ {
		lg.Info("filling")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info("benchmark message drop newest")
	}
}

func BenchmarkBackpressureBlockOnFull(b *testing.B) {
	tmpDir := b.TempDir()
	// Larger buffer and fast flush to bench BlockOnFull throughput
	lg, _ := acacia.Start("bench_block_full.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBackpressurePolicy(acacia.BlockOnFull),
		acacia.WithFlushInterval(1*time.Millisecond),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info("benchmark message block on full")
	}
}

func BenchmarkBackpressureDropOldestFullQueue(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_drop_oldest.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.DropOldest),
		acacia.WithFlushInterval(10*time.Second),
	)
	defer lg.Close()

	for i := 0; i < 1005; i++ {
		lg.Info("filling")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info("benchmark message drop oldest")
	}
}

func BenchmarkBackpressureDropDebugOnlyFullQueue(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_drop_debug_only.log", tmpDir, acacia.Level.DEBUG,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.DropDebugOnly),
		acacia.WithFlushInterval(10*time.Second),
	)
	defer lg.Close()

	for i := 0; i < 1005; i++ {
		lg.Debug("filling")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Debug("benchmark message drop debug only")
	}
}

func BenchmarkBackpressureStats(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_stats.log", tmpDir, acacia.Level.INFO)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats := lg.Stats()
		_ = stats.QueueLen
	}
}

func BenchmarkBackpressureDropped(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_dropped.log", tmpDir, acacia.Level.INFO)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = lg.Dropped()
	}
}

func BenchmarkBackpressureBuilderDropNewestFullQueue(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_builder_drop.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.DropNewest),
		acacia.WithFlushInterval(10*time.Second),
	)
	defer lg.Close()

	for i := 0; i < 1005; i++ {
		lg.InfoEvent().Str("k", "v").Send()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().Str("k", "v").Send()
	}
}

func BenchmarkBackpressureBytesDropNewestFullQueue(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_bytes_drop.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.DropNewest),
		acacia.WithFlushInterval(10*time.Second),
	)
	defer lg.Close()

	payload := []byte("payload")
	for i := 0; i < 1005; i++ {
		lg.InfoBytes(payload)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoBytes(payload)
	}
}

func BenchmarkBackpressureLegacyMapDropNewestFullQueue(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_map_drop.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.DropNewest),
		acacia.WithFlushInterval(10*time.Second),
	)
	defer lg.Close()
	lg.StructuredJSON(true)

	m := map[string]interface{}{"user": "juan"}
	for i := 0; i < 1005; i++ {
		lg.Info(m)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info(m)
	}
}
