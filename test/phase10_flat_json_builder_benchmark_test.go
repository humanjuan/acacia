package acacia_test

import (
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkPhase10FlatJSONBuilderSmall(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_small.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("service", "billing").
			Int("status", 200).
			Msg("payment received")
	}
}

func BenchmarkPhase10FlatJSONBuilderTenFields(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_ten.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("f1", "val1").
			Int("f2", 2).
			Int64("f3", 3).
			Uint64("f4", 4).
			Bool("f5", true).
			Float64("f6", 6.6).
			Dur("f7", 7*time.Second).
			Time("f8", time.Time{}).
			Str("f9", "val9").
			Int("f10", 10).
			Msg("ten fields log")
	}
}

func BenchmarkPhase10FlatJSONBuilderEscapedString(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_esc.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	escapedStr := "hello\nworld\twith\"quotes\"and\\slash"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("escaped", escapedStr).
			Msg("escaped msg")
	}
}

func BenchmarkPhase10FlatJSONBuilderRedactedField(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_redact.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("password", "highlysensitive").
			Msg("redacted msg")
	}
}

func BenchmarkPhase10FlatJSONBuilderDuplicateKeys(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_dup.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("dup", "val1").
			Str("dup", "val2").
			Msg("dup msg")
	}
}

func BenchmarkPhase10FlatJSONBuilderDisabledLevel(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_dis.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.DebugEvent().
			Str("ignored", "field").
			Msg("debug msg")
	}
}

func BenchmarkPhase10FlatJSONBuilderASCIIString(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_ascii.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	asciiStr := "normal_ascii_string_without_escapes"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("ascii", asciiStr).
			Msg("msg")
	}
}

func BenchmarkPhase10FlatJSONBuilderInvalidUTF8String(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_invalid.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	invalidStr := "hello\xffworld\xfeinvalid"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("invalid", invalidStr).
			Msg("msg")
	}
}
