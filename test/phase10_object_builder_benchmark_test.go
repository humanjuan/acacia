package acacia_test

import (
	"math"
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkPhase10ObjectBuilderSmall(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_small.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Object("user", func(o *acacia.Object) {
				o.Str("id", "u_123").
					Int("status", 200)
			}).
			Msg("payment completed")
	}
}

func BenchmarkPhase10ObjectBuilderNested(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_nested.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Object("user", func(o *acacia.Object) {
				o.Str("id", "u_123").
					Object("profile", func(p *acacia.Object) {
						p.Str("country", "CL").
							Str("tier", "premium")
					})
			}).
			Msg("nested msg")
	}
}

func BenchmarkPhase10ObjectBuilderFiveLevels(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_nested_five.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Object("l1", func(o1 *acacia.Object) {
				o1.Object("l2", func(o2 *acacia.Object) {
					o2.Object("l3", func(o3 *acacia.Object) {
						o3.Object("l4", func(o4 *acacia.Object) {
							o4.Object("l5", func(o5 *acacia.Object) {
								o5.Str("val", "nested")
							})
						})
					})
				})
			}).
			Msg("nested msg")
	}
}

func BenchmarkPhase10ObjectBuilderEscapedString(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_escaped.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	escapedStr := "hello\nworld\twith\"quotes\"and\\slash"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Object("obj", func(o *acacia.Object) {
				o.Str("escaped", escapedStr)
			}).
			Msg("msg")
	}
}

func BenchmarkPhase10ObjectBuilderRedactedField(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_redact.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Object("user", func(o *acacia.Object) {
				o.Str("password", "highlysensitive")
			}).
			Msg("msg")
	}
}

func BenchmarkPhase10ObjectBuilderFloat64(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_float.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Object("floats", func(o *acacia.Object) {
				o.Float64("nan", math.NaN()).
					Float64("normal", 3.14)
			}).
			Msg("msg")
	}
}
