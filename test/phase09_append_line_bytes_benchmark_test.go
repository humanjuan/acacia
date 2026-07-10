package acacia_test

import (
	"testing"
)

func equivalentAppendLineBytes(dst []byte, ts []byte, lvl []byte, msg []byte) []byte {
	if len(ts) > 0 {
		dst = append(dst, ts...)
	}
	dst = append(dst, ' ')
	dst = append(dst, '[')
	dst = append(numBytes(dst), lvl...)
	dst = append(dst, ']', ' ')
	dst = append(dst, msg...)
	if len(dst) == 0 || dst[len(dst)-1] != '\n' {
		dst = append(dst, '\n')
	}
	return dst
}

func numBytes(b []byte) []byte {
	return b
}

func BenchmarkPhase09AppendLineBytesSmall(b *testing.B) {
	ts := []byte("2026-07-09 12:00:00")
	lvl := []byte("INFO")
	msg := []byte("small message")
	var dst []byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = equivalentAppendLineBytes(dst[:0], ts, lvl, msg)
	}
}

func BenchmarkPhase09AppendLineBytes1KB(b *testing.B) {
	ts := []byte("2026-07-09 12:00:00")
	lvl := []byte("INFO")
	msg := make([]byte, 1024)
	var dst []byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = equivalentAppendLineBytes(dst[:0], ts, lvl, msg)
	}
}

func BenchmarkPhase09AppendLineBytes4KB(b *testing.B) {
	ts := []byte("2026-07-09 12:00:00")
	lvl := []byte("INFO")
	msg := make([]byte, 4096)
	var dst []byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = equivalentAppendLineBytes(dst[:0], ts, lvl, msg)
	}
}

func BenchmarkPhase09AppendLineBytes8KB(b *testing.B) {
	ts := []byte("2026-07-09 12:00:00")
	lvl := []byte("INFO")
	msg := make([]byte, 8192)
	var dst []byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = equivalentAppendLineBytes(dst[:0], ts, lvl, msg)
	}
}

func BenchmarkPhase09AppendLineBytesWithPreGrow1KB(b *testing.B) {
	ts := []byte("2026-07-09 12:00:00")
	lvl := []byte("INFO")
	msg := make([]byte, 1024)
	var dst []byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		needed := len(ts) + len(lvl) + len(msg) + 5
		if cap(dst) < needed {
			dst = make([]byte, 0, needed)
		}
		dst = equivalentAppendLineBytes(dst[:0], ts, lvl, msg)
	}
}

func BenchmarkPhase09AppendLineBytesWithPreGrow4KB(b *testing.B) {
	ts := []byte("2026-07-09 12:00:00")
	lvl := []byte("INFO")
	msg := make([]byte, 4096)
	var dst []byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		needed := len(ts) + len(lvl) + len(msg) + 5
		if cap(dst) < needed {
			dst = make([]byte, 0, needed)
		}
		dst = equivalentAppendLineBytes(dst[:0], ts, lvl, msg)
	}
}

func BenchmarkPhase09AppendLineBytesWithPreGrow8KB(b *testing.B) {
	ts := []byte("2026-07-09 12:00:00")
	lvl := []byte("INFO")
	msg := make([]byte, 8192)
	var dst []byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		needed := len(ts) + len(lvl) + len(msg) + 5
		if cap(dst) < needed {
			dst = make([]byte, 0, needed)
		}
		dst = equivalentAppendLineBytes(dst[:0], ts, lvl, msg)
	}
}
