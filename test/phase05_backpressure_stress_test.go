package acacia_test

import (
	"sync"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func TestStressBackpressureDropNewest(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("stress_drop_newest.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.DropNewest),
		acacia.WithFlushInterval(10*time.Second), // slow flush
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	const numGoroutines = 64
	const numLogsPerGoroutine = 5000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < numLogsPerGoroutine; i++ {
				lg.Info("stress message drop newest")
			}
		}()
	}

	wg.Wait()
	lg.Close()

	stats := lg.Stats()
	if stats.DroppedTotal == 0 {
		t.Error("expected dropped messages under high concurrency and DropNewest, got 0")
	}
	if stats.EnqueuedTotal+stats.DroppedTotal != numGoroutines*numLogsPerGoroutine {
		t.Errorf("sum of enqueued %d and dropped %d should match total attempts %d",
			stats.EnqueuedTotal, stats.DroppedTotal, numGoroutines*numLogsPerGoroutine)
	}
}

func TestStressBackpressureBlockOnFull(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("stress_block_on_full.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.BlockOnFull),
		acacia.WithFlushInterval(1*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	const numGoroutines = 64
	const numLogsPerGoroutine = 500

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < numLogsPerGoroutine; i++ {
				lg.Info("stress message block on full")
			}
		}()
	}

	wg.Wait()
	lg.Sync()

	stats := lg.Stats()
	if stats.DroppedTotal != 0 {
		t.Errorf("expected 0 dropped messages under BlockOnFull, got %d", stats.DroppedTotal)
	}
	if stats.EnqueuedTotal != numGoroutines*numLogsPerGoroutine {
		t.Errorf("expected %d enqueued messages, got %d",
			numGoroutines*numLogsPerGoroutine, stats.EnqueuedTotal)
	}

	lg.Close()
}

func TestStressBackpressureDropDebugOnly(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("stress_drop_debug.log", tmpDir, acacia.Level.DEBUG,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.DropDebugOnly),
		acacia.WithFlushInterval(10*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	const numGoroutines = 64
	const numLogsPerGoroutine = 5000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < numLogsPerGoroutine; i++ {
				if gid%2 == 0 {
					lg.Debug("debug log")
				} else {
					lg.Error("error log")
				}
			}
		}(g)
	}

	wg.Wait()
	lg.Close()

	stats := lg.Stats()
	if stats.DroppedTotal == 0 {
		t.Error("expected dropped debug logs, got 0")
	}
}

func TestStressStatsConcurrent(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("stress_stats.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.DropNewest),
		acacia.WithFlushInterval(1*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				lg.Info("log message")
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				stats := lg.Stats()
				_ = stats.QueueLen
				_ = lg.Dropped()
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	lg.Close()
}

func TestStressSyncCloseUnderSaturation(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("stress_sync_close.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.DropNewest),
		acacia.WithFlushInterval(10*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(4)
	for g := 0; g < 4; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				lg.Info("stress log")
			}
		}()
	}

	// Concurrent Sync calls
	for i := 0; i < 3; i++ {
		go func() {
			lg.Sync()
		}()
	}

	wg.Wait()
	lg.Close()
}
