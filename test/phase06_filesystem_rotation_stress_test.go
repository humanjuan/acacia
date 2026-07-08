package acacia_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func TestStressRotationConcurrentWriters(t *testing.T) {
	tmpDir := t.TempDir()

	lg, err := acacia.Start("stress_rotate.log", tmpDir, acacia.Level.INFO,
		acacia.WithFlushInterval(1*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}
	lg.Rotation(1, 3) // 1MB, 3 backups

	const numGoroutines = 30
	const numLogsPerGoroutine = 100
	largeMsg := strings.Repeat("S", 5000) // 5KB msg to trigger rotations concurrent

	var wg sync.WaitGroup
	wg.Add(numGoroutines + 2)

	// Producer goroutines
	for g := 0; g < numGoroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < numLogsPerGoroutine; i++ {
				lg.Info(largeMsg)
			}
		}()
	}

	// Concurrent Sync caller goroutine
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			lg.Sync()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Concurrent Stats caller goroutine
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			stats := lg.Stats()
			_ = stats.RotationErrorsTotal
			_ = stats.WriteErrorsTotal
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
	lg.Close()
}
