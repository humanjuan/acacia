package acacia_test

import (
	"fmt"
	"sync"
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkSyncEmpty(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_sync_empty.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		b.Fatalf("failed to start logger: %v", err)
	}
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Sync()
	}
}

func BenchmarkSyncWithBufferedEvents(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_sync_buf.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5000),
	)
	if err != nil {
		b.Fatalf("failed to start logger: %v", err)
	}
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info("some log line message")
		lg.Sync()
	}
}

func BenchmarkCloseEmpty(b *testing.B) {
	tmpDir := b.TempDir()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		lg, err := acacia.Start(fmt.Sprintf("bench_close_empty_%d.log", i), tmpDir, acacia.Level.INFO)
		if err != nil {
			b.Fatalf("failed to start: %v", err)
		}
		b.StartTimer()
		lg.Close()
	}
}

func BenchmarkCloseWithBufferedEvents(b *testing.B) {
	tmpDir := b.TempDir()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		lg, err := acacia.Start(fmt.Sprintf("bench_close_buf_%d.log", i), tmpDir, acacia.Level.INFO,
			acacia.WithBufferSize(1000),
		)
		if err != nil {
			b.Fatalf("failed to start: %v", err)
		}
		for j := 0; j < 50; j++ {
			lg.Info("some logs in buffer")
		}
		b.StartTimer()
		lg.Close()
	}
}

func BenchmarkRepeatedClose(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_repeat_close.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Close()
	}
}

func BenchmarkWriteAfterClose(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_write_after_close.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info("write log message after close")
	}
}

func BenchmarkSyncAfterClose(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_sync_after_close.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Sync()
	}
}

func BenchmarkConcurrentSync(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_concurrent_sync.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lg.Sync()
		}
	})
}

func BenchmarkCloseUnderBlockOnFull(b *testing.B) {
	tmpDir := b.TempDir()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		lg, err := acacia.Start(fmt.Sprintf("bench_close_block_%d.log", i), tmpDir, acacia.Level.INFO,
			acacia.WithBufferSize(5),
			acacia.WithBackpressurePolicy(acacia.BlockOnFull),
		)
		if err != nil {
			b.Fatalf("failed to start: %v", err)
		}
		var wg sync.WaitGroup
		wg.Add(10)
		for g := 0; g < 10; g++ {
			go func() {
				defer wg.Done()
				lg.Info("blocked event log")
			}()
		}
		b.StartTimer()
		lg.Close()
		wg.Wait()
	}
}
