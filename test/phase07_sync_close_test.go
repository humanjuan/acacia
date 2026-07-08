package acacia_test

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

// A. Close idempotente
func TestCloseIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("idempotent.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	lg.Info("hello 1")
	lg.Info("hello 2")

	// Call Close multiple times
	lg.Close()
	lg.Close()
	lg.Close()
}

// B. Sync múltiple
func TestMultipleSync(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("multi_sync.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}
	defer lg.Close()

	lg.Info("log before sync 1")
	lg.Sync()
	lg.Info("log before sync 2")
	lg.Sync()
	lg.Sync()
}

// C. Sync concurrente
func TestConcurrentSync(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("concurrent_sync.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}
	defer lg.Close()

	const numProducers = 10
	const numSyncers = 32
	var wg sync.WaitGroup
	wg.Add(numProducers + numSyncers)

	for i := 0; i < numProducers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				lg.Info(fmt.Sprintf("producer-%d-log-%d", id, j))
			}
		}(i)
	}

	for i := 0; i < numSyncers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				lg.Sync()
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
}

// D. Close concurrente
func TestConcurrentClose(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("concurrent_close.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	const numClosers = 32
	var wg sync.WaitGroup
	wg.Add(numClosers)

	for i := 0; i < numClosers; i++ {
		go func() {
			defer wg.Done()
			lg.Close()
		}()
	}

	wg.Wait()
}

// E. Sync durante Close
func TestSyncDuringClose(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("sync_during_close.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			lg.Sync()
			time.Sleep(1 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		lg.Close()
	}()

	wg.Wait()
}

// F. Write después de Close
func TestWriteAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("write_after_close.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	lg.Info("valid log before close")
	lg.Close()

	// Operations after close should not panic
	lg.Info("this should be dropped silently")
	lg.Warn("warn after close")
	lg.Error("error after close")
	lg.Debug("debug after close")

	builder := lg.InfoEvent()
	if builder != nil {
		builder.Str("k", "v").Msg("builder msg after close")
	}

	// Verify that Status returns false
	if lg.Status() {
		t.Error("expected Status() to return false after Close()")
	}
}

// G. BlockOnFull + Close
func TestBlockOnFullClose(t *testing.T) {
	tmpDir := t.TempDir()
	// Small buffer of size 5
	lg, err := acacia.Start("block_on_full_close.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5),
		acacia.WithBackpressurePolicy(acacia.BlockOnFull),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	// Pause writer to force queue fill
	// Wait, we don't have a direct "pause writer" API, but we can flood with a lot of logs in parallel
	// or block sending.
	// Let's spawn 30 producers sending concurrent logs, they will quickly saturate buffer size 5
	const numProducers = 30
	var wg sync.WaitGroup
	var blockedCount int64

	wg.Add(numProducers)
	for i := 0; i < numProducers; i++ {
		go func(id int) {
			defer wg.Done()
			atomic.AddInt64(&blockedCount, 1)
			lg.Info("blocked-log-entry-concurrency-test")
		}(i)
	}

	// Wait a moment for goroutines to block
	time.Sleep(20 * time.Millisecond)

	// Close the logger. This should unlock all blocked producers
	lg.Close()

	// Wait for all producers to return (if they deadlock, the test will timeout/fail)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock: producers or Close hung under BlockOnFull")
	}
}

// H. DropNewest + Sync
func TestDropNewestSync(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("drop_newest_sync.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5),
		acacia.WithBackpressurePolicy(acacia.DropNewest),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}
	defer lg.Close()

	// Flooding
	for i := 0; i < 100; i++ {
		lg.Info("flood-log")
	}

	// Sync should return fast and not deadlock
	lg.Sync()
}

// I. DropOldest + Sync
func TestDropOldestSync(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("drop_oldest_sync.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5),
		acacia.WithBackpressurePolicy(acacia.DropOldest),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}
	defer lg.Close()

	// Flooding
	for i := 0; i < 100; i++ {
		lg.Info("flood-log")
	}

	// Sync should return fast and not deadlock
	lg.Sync()
}

// J. DropDebugOnly + Close
func TestDropDebugOnlyClose(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("drop_debug_only_close.log", tmpDir, acacia.Level.DEBUG,
		acacia.WithBufferSize(5),
		acacia.WithBackpressurePolicy(acacia.DropDebugOnly),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	// Flood queue with DEBUG/INFO logs
	for i := 0; i < 50; i++ {
		lg.Debug("debug-log")
		lg.Info("info-log")
	}

	// Send an error log
	lg.Error("critical-error-log")

	lg.Close()
}

// K. Rotation concurrente con Close
func TestRotationConcurrentClose(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("rot_close.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}
	lg.Rotation(1, 2) // 1MB size, 2 backups

	// Write large log payloads in background
	largeMsg := strings.Repeat("X", 5000)
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				lg.Info(largeMsg)
			}
		}()
	}

	time.Sleep(5 * time.Millisecond)
	lg.Close()

	wg.Wait()

	// Ensure final files are inspectable
	_, err = os.Stat(filepath.Join(tmpDir, "rot_close.log"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("unexpected error statting final log: %v", err)
	}
}
