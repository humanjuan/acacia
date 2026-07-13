package acacia_test

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/humanjuan/acacia/v2"
)

func TestPhase09LargeBurstDoesNotLoseAcceptedLogs(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("burst.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(50000),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	const numWorkers = 10
	const numLogsPerWorker = 500
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numLogsPerWorker; j++ {
				lg.Info(fmt.Sprintf("worker %d log %d", workerID, j))
			}
		}(i)
	}

	wg.Wait()
	lg.Close()

	// Read and verify log count
	filePath := filepath.Join(tmpDir, "burst.log")
	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("failed to open log file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		if len(strings.TrimSpace(line)) > 0 {
			count++
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner error: %v", err)
	}

	expected := numWorkers * numLogsPerWorker
	if count != expected {
		t.Errorf("expected %d logs, got %d", expected, count)
	}
}

func TestPhase09FlushThresholdPreservesOrdering(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("order.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(10000),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	const totalLogs = 2000
	for i := 0; i < totalLogs; i++ {
		lg.Info(fmt.Sprintf("seq-%d", i))
	}
	lg.Close()

	filePath := filepath.Join(tmpDir, "order.log")
	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("failed to open log file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	index := 0
	for scanner.Scan() {
		line := scanner.Text()
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}
		expectedPrefix := fmt.Sprintf("seq-%d", index)
		if !strings.Contains(line, expectedPrefix) {
			t.Fatalf("out of order log: expected to find %q in %q at index %d", expectedPrefix, line, index)
		}
		index++
	}
	if index != totalLogs {
		t.Errorf("expected to read %d logs, but read %d", totalLogs, index)
	}
}

func TestPhase09SingleLargePayloadPreserved(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("large.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	// Payload of 300KB (larger than 256KB threshold)
	largeData := make([]byte, 300*1024)
	for i := range largeData {
		largeData[i] = 'A'
	}
	largeStr := string(largeData)

	lg.Info(largeStr)
	lg.Close()

	filePath := filepath.Join(tmpDir, "large.log")
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	if !strings.Contains(string(content), largeStr) {
		t.Errorf("large payload was truncated or not persisted")
	}
}

func TestPhase09CloseAfterThresholdFlushPersistsAcceptedLogs(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("close_flush.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	// Write 5 large messages of 80KB each (total 400KB, exceeding 256KB threshold)
	largeData := make([]byte, 80*1024)
	for i := range largeData {
		largeData[i] = 'B'
	}
	largeStr := string(largeData)

	for i := 0; i < 5; i++ {
		lg.Info(fmt.Sprintf("large-%d-%s", i, largeStr))
	}

	lg.Close()

	filePath := filepath.Join(tmpDir, "close_flush.log")
	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, len(buf))
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "large-") {
			count++
		}
	}
	if count != 5 {
		t.Errorf("expected 5 large logs, got %d", count)
	}
}

func TestPhase09SyncAfterThresholdFlushPersistsAcceptedLogs(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("sync_flush.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	defer lg.Close()

	// Write 5 large messages of 80KB each (total 400KB, exceeding 256KB threshold)
	largeData := make([]byte, 80*1024)
	for i := range largeData {
		largeData[i] = 'C'
	}
	largeStr := string(largeData)

	for i := 0; i < 5; i++ {
		lg.Info(fmt.Sprintf("sync-large-%d-%s", i, largeStr))
	}

	lg.Sync()

	filePath := filepath.Join(tmpDir, "sync_flush.log")
	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, len(buf))
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "sync-large-") {
			count++
		}
	}
	if count != 5 {
		t.Errorf("expected 5 large logs after Sync, got %d", count)
	}
}

func TestPhase09PostCloseWriteStillSafeAfterBufferOptimization(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("post_close.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	lg.Info("before close")
	lg.Close()

	// Write after close (should not panic or block)
	lg.Info("after close")

	filePath := filepath.Join(tmpDir, "post_close.log")
	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	beforeFound := false
	afterFound := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "before close") {
			beforeFound = true
		}
		if strings.Contains(line, "after close") {
			afterFound = true
		}
	}

	if !beforeFound {
		t.Errorf("log written before close was not found")
	}
	if afterFound {
		t.Errorf("log written after close should have been dropped")
	}
}

func TestPhase09LargePayloadFlushedImmediately(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("large_immediate.log", tmpDir, acacia.Level.INFO,
		acacia.WithFlushInterval(10*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}
	defer lg.Close()

	// Payload of 300KB (larger than 256KB threshold)
	largeData := make([]byte, 300*1024)
	for i := range largeData {
		largeData[i] = 'D'
	}
	largeStr := string(largeData)

	lg.Info(largeStr)

	// Poll the file until the content is written (timeout 2 seconds)
	filePath := filepath.Join(tmpDir, "large_immediate.log")
	written := false
	for attempt := 0; attempt < 20; attempt++ {
		content, err := os.ReadFile(filePath)
		if err == nil && strings.Contains(string(content), largeStr) {
			written = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !written {
		t.Errorf("large payload was not flushed immediately (within 2 seconds) under 10-second flush interval")
	}
}

func TestPhase09BackpressureDropOldestConcurrently(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("drop_oldest_concurrent.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.DropOldest),
		acacia.WithFlushInterval(10*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	// Write logs to force eviction of oldest logs
	// We use concurrent workers to ensure queue saturation
	const numWorkers = 5
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 40000; i++ {
				lg.Info("saturating queue message")
			}
		}()
	}
	wg.Wait()

	// Write recent sequence marker which should be preserved
	lg.Info("seq=99999")

	stats := lg.Stats()
	if stats.DroppedTotal == 0 {
		t.Error("expected dropped messages under DropOldest, got 0")
	}

	lg.Close()

	// Read log file to verify the latest ones are written, and oldest got evicted
	data, err := os.ReadFile(filepath.Join(tmpDir, "drop_oldest_concurrent.log"))
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}
	content := string(data)

	// Since seq=99999 was written last, it should definitely be present
	if !strings.Contains(content, "seq=99999") {
		t.Error("expected recent message seq=99999 to be preserved under DropOldest")
	}
}

func TestPhase09BackpressureDropDebugOnlyConcurrently(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("drop_debug_concurrent.log", tmpDir, acacia.Level.DEBUG,
		acacia.WithBufferSize(1000),
		acacia.WithBackpressurePolicy(acacia.DropDebugOnly),
		acacia.WithFlushInterval(10*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	// Saturate queue with DEBUG logs using concurrent workers to ensure saturation
	const numWorkers = 5
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 40000; i++ {
				lg.Debug("debug message")
			}
		}()
	}
	wg.Wait()

	// Now try to log critical messages, which should block/enqueue
	// Let's spawn a goroutine to log error/critical while the queue is full
	doneCh := make(chan struct{})
	go func() {
		lg.Error("critical error message")
		close(doneCh)
	}()

	select {
	case <-doneCh:
		// Critical got enqueued or blocked.
	case <-time.After(3 * time.Second):
		t.Fatal("critical error log blocked for too long or got dropped")
	}

	stats := lg.Stats()
	if stats.DroppedTotal == 0 {
		t.Error("expected debug/info messages to be dropped, got 0 drops")
	}

	lg.Close()
}
