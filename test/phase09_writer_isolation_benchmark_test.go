package acacia_test

import (
	"io"
	"testing"
)

type concreteDiscard struct{}

func (c *concreteDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}

func BenchmarkPhase09WriterInterfaceSmall(b *testing.B) {
	var w io.Writer = &concreteDiscard{}
	data := []byte("small log line message")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = w.Write(data)
	}
}

func BenchmarkPhase09WriterInterface1KB(b *testing.B) {
	var w io.Writer = &concreteDiscard{}
	data := make([]byte, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = w.Write(data)
	}
}

func BenchmarkPhase09WriterInterface4KB(b *testing.B) {
	var w io.Writer = &concreteDiscard{}
	data := make([]byte, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = w.Write(data)
	}
}

func BenchmarkPhase09WriterDirectSmall(b *testing.B) {
	w := &concreteDiscard{}
	data := []byte("small log line message")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = w.Write(data)
	}
}

func BenchmarkPhase09WriterDirect1KB(b *testing.B) {
	w := &concreteDiscard{}
	data := make([]byte, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = w.Write(data)
	}
}

func BenchmarkPhase09WriterDirect4KB(b *testing.B) {
	w := &concreteDiscard{}
	data := make([]byte, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = w.Write(data)
	}
}
