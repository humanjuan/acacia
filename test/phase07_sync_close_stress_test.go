package acacia_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func TestStressSyncCloseConcurrentProducers(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("stress_concurrent.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	const numProducers = 32
	const numLogsPerProducer = 1000
	var wg sync.WaitGroup
	wg.Add(numProducers + 2)

	// Producers
	for i := 0; i < numProducers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numLogsPerProducer; j++ {
				lg.Info("stress concurrent producer entry")
			}
		}(i)
	}

	// Concurrent syncs
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			lg.Sync()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Close caller
	go func() {
		defer wg.Done()
		time.Sleep(30 * time.Millisecond)
		lg.Close()
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(10 * time.Second):
		t.Fatal("timeout: TestStressSyncCloseConcurrentProducers deadlocked")
	}
}

func TestStressSyncCloseBlockOnFull(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("stress_block.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(10),
		acacia.WithBackpressurePolicy(acacia.BlockOnFull),
	)
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	const numProducers = 64
	var wg sync.WaitGroup
	wg.Add(numProducers + 1)

	for i := 0; i < numProducers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				lg.Info("stress block on full entry log line")
			}
		}()
	}

	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		lg.Close()
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(10 * time.Second):
		t.Fatal("timeout: TestStressSyncCloseBlockOnFull deadlocked")
	}
}

func TestStressSyncCloseDropNewest(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("stress_drop_newest.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(10),
		acacia.WithBackpressurePolicy(acacia.DropNewest),
	)
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	const numProducers = 64
	var wg sync.WaitGroup
	wg.Add(numProducers + 2)

	for i := 0; i < numProducers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				lg.Info("stress drop newest entry log line")
			}
		}()
	}

	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			lg.Sync()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(15 * time.Millisecond)
		lg.Close()
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(10 * time.Second):
		t.Fatal("timeout: TestStressSyncCloseDropNewest deadlocked")
	}
}

func TestStressSyncCloseDropOldest(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("stress_drop_oldest.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(10),
		acacia.WithBackpressurePolicy(acacia.DropOldest),
	)
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	const numProducers = 64
	var wg sync.WaitGroup
	wg.Add(numProducers + 2)

	for i := 0; i < numProducers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				lg.Info("stress drop oldest entry log line")
			}
		}()
	}

	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			lg.Sync()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(15 * time.Millisecond)
		lg.Close()
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(10 * time.Second):
		t.Fatal("timeout: TestStressSyncCloseDropOldest deadlocked")
	}
}

func TestStressSyncCloseDropDebugOnly(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("stress_drop_debug.log", tmpDir, acacia.Level.DEBUG,
		acacia.WithBufferSize(10),
		acacia.WithBackpressurePolicy(acacia.DropDebugOnly),
	)
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	const numProducers = 64
	var wg sync.WaitGroup
	wg.Add(numProducers + 2)

	for i := 0; i < numProducers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				lg.Debug("stress drop debug entry log line")
				lg.Error("stress drop debug priority error entry log line")
			}
		}()
	}

	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			lg.Sync()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(15 * time.Millisecond)
		lg.Close()
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(10 * time.Second):
		t.Fatal("timeout: TestStressSyncCloseDropDebugOnly deadlocked")
	}
}

func TestStressSyncCloseRotation(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("stress_rot.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	lg.Rotation(1, 2)

	const numProducers = 32
	var wg sync.WaitGroup
	wg.Add(numProducers + 1)

	largeMsg := strings.Repeat("R", 5000)
	for i := 0; i < numProducers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				lg.Info(largeMsg)
			}
		}()
	}

	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond)
		lg.Close()
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(10 * time.Second):
		t.Fatal("timeout: TestStressSyncCloseRotation deadlocked")
	}
}

func TestStressRepeatedClose(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("stress_repeat_close.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func() {
			defer wg.Done()
			lg.Close()
		}()
	}

	wg.Wait()
}
