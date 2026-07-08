package acacia_test

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

// A. Zero loss before Sync
func TestZeroLossBeforeSync(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("loss_sync.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(100),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}
	defer lg.Close()

	const numLines = 50
	for i := 0; i < numLines; i++ {
		lg.Info(fmt.Sprintf("unique-sync-key-%d", i))
	}

	lg.Sync()

	// Read file and check
	filePath := filepath.Join(tmpDir, "loss_sync.log")
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}

	linesStr := string(content)
	for i := 0; i < numLines; i++ {
		key := fmt.Sprintf("unique-sync-key-%d", i)
		if !strings.Contains(linesStr, key) {
			t.Errorf("expected key %q in logs, not found", key)
		}
	}
}

// B. Zero loss before Close
func TestZeroLossBeforeClose(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("loss_close.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(100),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	const numLines = 50
	for i := 0; i < numLines; i++ {
		lg.Info(fmt.Sprintf("unique-close-key-%d", i))
	}

	lg.Close()

	// Read file and check
	filePath := filepath.Join(tmpDir, "loss_close.log")
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}

	linesStr := string(content)
	for i := 0; i < numLines; i++ {
		key := fmt.Sprintf("unique-close-key-%d", i)
		if !strings.Contains(linesStr, key) {
			t.Errorf("expected key %q in logs, not found", key)
		}
	}
}

// C. Zero loss concurrent accepted before Close
func TestZeroLossConcurrentAcceptedBeforeClose(t *testing.T) {
	tmpDir := t.TempDir()
	// Large queue capacity to make sure we don't drop under default DropNewest
	lg, err := acacia.Start("loss_concurrent_close.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5000),
		acacia.WithBackpressurePolicy(acacia.DropNewest),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	const numProducers = 10
	const logsPerProducer = 100
	var wg sync.WaitGroup
	wg.Add(numProducers)

	var attemptedTotal int64

	for i := 0; i < numProducers; i++ {
		go func(pid int) {
			defer wg.Done()
			for j := 0; j < logsPerProducer; j++ {
				lg.Info(fmt.Sprintf("producer-%d-index-%d-token", pid, j))
				atomic.AddInt64(&attemptedTotal, 1)
			}
		}(i)
	}

	// Wait for producers to finish enqueuing
	wg.Wait()

	// Capture stats of accepted events
	stats := lg.Stats()
	acceptedCount := stats.EnqueuedTotal

	// Close the logger to force flush
	lg.Close()

	// Count lines written to the physical file
	filePath := filepath.Join(tmpDir, "loss_concurrent_close.log")
	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("failed to open log file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var writtenCount uint64
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "-token") {
			writtenCount++
		}
	}

	if writtenCount != acceptedCount {
		t.Errorf("mismatch between accepted logs (%d) and written logs (%d)", acceptedCount, writtenCount)
	}
}

// D. Drop policies do not promise dropped events
func TestDropPoliciesNoPromise(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("loss_drop.log", tmpDir, acacia.Level.INFO,
		// Very tiny queue to force drops under concurrency
		acacia.WithBufferSize(2),
		acacia.WithBackpressurePolicy(acacia.DropNewest),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	const totalAttempts = 500
	var wg sync.WaitGroup
	wg.Add(10)

	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				lg.Info("attempt-log")
			}
		}()
	}

	wg.Wait()
	lg.Close()

	stats := lg.Stats()

	// Total enqueued + dropped should match total attempts (500)
	sum := stats.EnqueuedTotal + stats.DroppedTotal
	if sum != totalAttempts {
		t.Errorf("expected sum of enqueued (%d) and dropped (%d) to equal attempts (%d), got %d",
			stats.EnqueuedTotal, stats.DroppedTotal, totalAttempts, sum)
	}
}
