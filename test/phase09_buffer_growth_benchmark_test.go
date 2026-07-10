package acacia_test

import (
	"sync"
	"testing"
)

func BenchmarkPhase09BufferGrowthDynamic(b *testing.B) {
	b.Run("DynamicAppend", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var buf []byte
			for j := 0; j < 100; j++ {
				buf = append(buf, "some payload pattern that causes slice growth..."...)
			}
		}
	})
	b.Run("PreGrow", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := make([]byte, 0, 5000)
			for j := 0; j < 100; j++ {
				buf = append(buf, "some payload pattern that causes slice growth..."...)
			}
		}
	})
	b.Run("SyncPool", func(b *testing.B) {
		pool := &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, 0, 5000)
				return &buf
			},
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			p := pool.Get().(*[]byte)
			buf := *p
			buf = buf[:0]
			for j := 0; j < 100; j++ {
				buf = append(buf, "some payload pattern that causes slice growth..."...)
			}
			*p = buf
			pool.Put(p)
		}
	})
}
