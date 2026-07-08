package acacia_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func TestBackpressureDefaultPolicy(t *testing.T) {
	tmpDir := t.TempDir()
	// Config buffer size very small (MinBufferSize = 1000)
	lg, err := acacia.Start("default_policy.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
		acacia.WithFlushInterval(1*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}
	defer lg.Close()

	// Verify default stats
	stats := lg.Stats()
	if stats.QueueCap != 1000 {
		t.Errorf("expected QueueCap 1000, got %d", stats.QueueCap)
	}

	// Senders should block eventually if we saturate it under BlockOnFull,
	// but let's just make sure it does not drop by default when not saturated
	lg.Info("hello default")
	lg.Sync()

	stats = lg.Stats()
	if stats.EnqueuedTotal != 1 {
		t.Errorf("expected 1 enqueued, got %d", stats.EnqueuedTotal)
	}
	if stats.DroppedTotal != 0 {
		t.Errorf("expected 0 dropped, got %d", stats.DroppedTotal)
	}
}

func TestBackpressureDropNewest(t *testing.T) {
	tmpDir := t.TempDir()
	// MinBufferSize = 1000
	lg, err := acacia.Start("drop_newest.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.DropNewest),
		acacia.WithFlushInterval(10*time.Second), // prevent writer draining too fast
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	// Write 100,000 to trigger drops
	for i := 0; i < 100000; i++ {
		lg.Info("saturating queue with message")
	}

	stats := lg.Stats()
	if stats.DroppedTotal == 0 {
		t.Error("expected dropped messages under DropNewest, got 0")
	}
	if lg.Dropped() != stats.DroppedTotal {
		t.Errorf("Dropped() %d does not match Stats().DroppedTotal %d", lg.Dropped(), stats.DroppedTotal)
	}

	lg.Close()
}

func TestBackpressureBlockOnFull(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("block_on_full.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.BlockOnFull),
		acacia.WithFlushInterval(1*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(5)
	for g := 0; g < 5; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				lg.Info(fmt.Sprintf("g=%d i=%d message", id, i))
			}
		}(g)
	}

	wg.Wait()
	lg.Sync()

	stats := lg.Stats()
	if stats.DroppedTotal != 0 {
		t.Errorf("expected 0 dropped under BlockOnFull, got %d", stats.DroppedTotal)
	}
	if stats.EnqueuedTotal != 2500 {
		t.Errorf("expected 2500 enqueued, got %d", stats.EnqueuedTotal)
	}

	lg.Close()
}

func TestBackpressureDropOldest(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("drop_oldest.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.DropOldest),
		acacia.WithFlushInterval(10*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	// Write 100,000 logs to force eviction of oldest logs
	for i := 0; i < 100000; i++ {
		lg.Info(fmt.Sprintf("seq=%d", i))
	}

	stats := lg.Stats()
	if stats.DroppedTotal == 0 {
		t.Error("expected dropped messages under DropOldest, got 0")
	}

	lg.Close()

	// Read log file to verify the latest ones are written, and oldest got evicted
	data, err := os.ReadFile(filepath.Join(tmpDir, "drop_oldest.log"))
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}
	content := string(data)

	// Since we wrote 100,000, seq=99999 should definitely be present
	if !strings.Contains(content, "seq=99999") {
		t.Error("expected recent message seq=99999 to be preserved under DropOldest")
	}
}

func TestBackpressureDropDebugOnly(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("drop_debug.log", tmpDir, acacia.Level.DEBUG,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.DropDebugOnly),
		acacia.WithFlushInterval(10*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	// Saturate queue with DEBUG logs
	for i := 0; i < 100000; i++ {
		lg.Debug("debug message")
	}

	// Now try to log critical messages, which should block/enqueue
	// Let's spawn a goroutine to log error/critical while the queue is full
	doneCh := make(chan struct{})
	go func() {
		lg.Error("critical error message")
		close(doneCh)
	}()

	select {
	case <-doneCh:
		// Critical got enqueued or blocked. Since the reader is sleeping,
		// it must have eventually been enqueued (if it was BlockOnFull path for Error).
	case <-time.After(3 * time.Second):
		t.Fatal("critical error log blocked for too long or got dropped")
	}

	stats := lg.Stats()
	if stats.DroppedTotal == 0 {
		t.Error("expected debug/info messages to be dropped, got 0 drops")
	}

	lg.Close()
}

func TestBackpressureSyncCloseUnderSaturation(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("sync_close.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.DropNewest),
		acacia.WithFlushInterval(10*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	// Saturate
	for i := 0; i < 3000; i++ {
		lg.Info("saturation")
	}

	// Call Sync() and Close() under saturation to verify no deadlock
	syncDone := make(chan struct{})
	go func() {
		lg.Sync()
		close(syncDone)
	}()

	select {
	case <-syncDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Sync() hung under saturation")
	}

	lg.Close()
}
