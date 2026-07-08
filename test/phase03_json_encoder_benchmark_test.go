package acacia_test

import (
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkJSONEncoderBuilderSmall(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_enc_small.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("service", "owl").
			Int("status", 200).
			Send()
	}
}

func BenchmarkJSONEncoderBuilderMixed(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_enc_mixed.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	now := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("service", "owl").
			Int("status", 200).
			Bool("cached", true).
			Time("ts", now).
			Send()
	}
}

func BenchmarkJSONEncoderBuilder1KB(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_enc_1kb.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	largeString := string(make([]byte, 100))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("k1", largeString).
			Str("k2", largeString).
			Str("k3", largeString).
			Str("k4", largeString).
			Str("k5", largeString).
			Str("k6", largeString).
			Str("k7", largeString).
			Str("k8", largeString).
			Str("k9", largeString).
			Send()
	}
}

func BenchmarkJSONEncoderBuilder4KB(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_enc_4kb.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(1<<20),
	)
	defer lg.Close()

	largeString := string(make([]byte, 400))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("k1", largeString).
			Str("k2", largeString).
			Str("k3", largeString).
			Str("k4", largeString).
			Str("k5", largeString).
			Str("k6", largeString).
			Str("k7", largeString).
			Str("k8", largeString).
			Str("k9", largeString).
			Send()
	}
}

func BenchmarkJSONEncoderBuilder8KB(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_enc_8kb.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(1<<20),
	)
	defer lg.Close()

	largeString := string(make([]byte, 800))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("k1", largeString).
			Str("k2", largeString).
			Str("k3", largeString).
			Str("k4", largeString).
			Str("k5", largeString).
			Str("k6", largeString).
			Str("k7", largeString).
			Str("k8", largeString).
			Str("k9", largeString).
			Send()
	}
}

func BenchmarkJSONEncoderStringEscaping(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_enc_esc.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	escapedString := "normal text \"quotes\" and \\backslash\nnewlines\r\ttabs"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("esc", escapedString).
			Send()
	}
}

func BenchmarkJSONEncoderSecret(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_enc_sec.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	secretVal := "my_super_secret_value_123"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Secret("authorization", secretVal).
			Send()
	}
}

func BenchmarkJSONEncoderDisabledLevel(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_enc_dis.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.DebugEvent().
			Str("service", "owl").
			Int("status", 200).
			Send()
	}
}
