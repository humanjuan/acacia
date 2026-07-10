package acacia_test

import (
	"sync/atomic"
	"testing"
)

func BenchmarkPhase09AtomicStateLoad(b *testing.B) {
	var val uint32 = 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = atomic.LoadUint32(&val)
	}
}

func BenchmarkPhase09AtomicStateStore(b *testing.B) {
	var val uint32 = 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		atomic.StoreUint32(&val, 1)
	}
}

func BenchmarkPhase09AtomicEnqueuedSeqAdd(b *testing.B) {
	var val uint64 = 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		atomic.AddUint64(&val, 1)
	}
}

func BenchmarkPhase09AtomicDroppedSeqAdd(b *testing.B) {
	var val uint64 = 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		atomic.AddUint64(&val, 1)
	}
}

func BenchmarkPhase09AtomicMultiCounterContention(b *testing.B) {
	var counter1 uint64
	var counter2 uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			atomic.AddUint64(&counter1, 1)
			atomic.AddUint64(&counter2, 1)
		}
	})
}

type NoPadding struct {
	a uint64
	b uint64
}

type WithPadding struct {
	a uint64
	_ [7]uint64 // padding to prevent false sharing
	b uint64
}

func BenchmarkPhase09AtomicFalseSharingCandidate(b *testing.B) {
	b.Run("NoPadding", func(b *testing.B) {
		var np NoPadding
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				atomic.AddUint64(&np.a, 1)
				atomic.AddUint64(&np.b, 1)
			}
		})
	})
	b.Run("WithPadding", func(b *testing.B) {
		var wp WithPadding
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				atomic.AddUint64(&wp.a, 1)
				atomic.AddUint64(&wp.b, 1)
			}
		})
	})
}
