package acacia_test

import (
	"math"
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkPhase10ArrayBuilderPrimitiveSmall(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_array_small.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Array("items", func(a *acacia.Array) {
				a.Str("sku-1").
					Int(42)
			}).
			Msg("msg")
	}
}

func BenchmarkPhase10ArrayBuilderPrimitiveTenItems(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_array_ten.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Array("items", func(a *acacia.Array) {
				a.Int(1).Int(2).Int(3).Int(4).Int(5).Int(6).Int(7).Int(8).Int(9).Int(10)
			}).
			Msg("msg")
	}
}

func BenchmarkPhase10ArrayBuilderObjectElements(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_array_objects.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Array("items", func(a *acacia.Array) {
				a.Object(func(o *acacia.Object) {
					o.Str("sku", "ABC").Int("qty", 2)
				}).
					Object(func(o *acacia.Object) {
						o.Str("sku", "DEF").Int("qty", 1)
					})
			}).
			Msg("msg")
	}
}

func BenchmarkPhase10ArrayBuilderNestedInObject(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_array_nested.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Object("cart", func(o *acacia.Object) {
				o.Array("items", func(a *acacia.Array) {
					a.Str("sku-1").Str("sku-2")
				})
			}).
			Msg("msg")
	}
}

func BenchmarkPhase10ArrayBuilderEscapedString(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_array_escaped.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	escapedStr := "val\"with\nquotes\\slash"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Array("items", func(a *acacia.Array) {
				a.Str(escapedStr)
			}).
			Msg("msg")
	}
}

func BenchmarkPhase10ArrayBuilderObjectRedaction(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_array_redact.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Array("items", func(a *acacia.Array) {
				a.Object(func(o *acacia.Object) {
					o.Str("password", "highly_secret")
				})
			}).
			Msg("msg")
	}
}

func BenchmarkPhase10ArrayBuilderFloat64(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_array_float.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Array("items", func(a *acacia.Array) {
				a.Float64(math.NaN()).Float64(3.14)
			}).
			Msg("msg")
	}
}
