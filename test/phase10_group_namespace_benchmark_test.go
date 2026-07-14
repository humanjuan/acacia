package acacia_test

import (
	"math"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkPhase10GroupEventSmall(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_group_small.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Group("http", func(g *acacia.Object) {
				g.Str("method", "GET").
					Int("status", 200)
			}).
			Msg("ok")
	}
}

func BenchmarkPhase10GroupObjectNested(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_group_nested.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Object("request", func(o *acacia.Object) {
				o.Group("http", func(g *acacia.Object) {
					g.Str("method", "GET")
				})
			}).
			Msg("ok")
	}
}

func BenchmarkPhase10NamespaceEventSmall(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_ns_small.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Namespace("http", func(n *acacia.Namespace) {
				n.Str("method", "GET").
					Int("status", 200)
			}).
			Msg("ok")
	}
}

func BenchmarkPhase10NamespaceEventTenFields(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_ns_ten.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Namespace("http", func(n *acacia.Namespace) {
				n.Str("method", "GET").
					Int("status", 200).
					Bool("ok", true).
					Str("path", "/api/v1/resource").
					Str("agent", "Mozilla/5.0").
					Int64("bytes_sent", 1024).
					Uint64("req_id", 99999).
					Dur("duration", time.Millisecond*50).
					Time("time", time.Unix(1600000000, 0)).
					Err(nil)
			}).
			Msg("ok")
	}
}

func BenchmarkPhase10NamespaceObjectSmall(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_ns_obj_small.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Object("request", func(o *acacia.Object) {
				o.Namespace("http", func(n *acacia.Namespace) {
					n.Str("method", "GET")
				})
			}).
			Msg("ok")
	}
}

func BenchmarkPhase10NamespaceEscapedKeyValue(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_ns_escaped.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Namespace("http\nctx", func(n *acacia.Namespace) {
				n.Str("method\rname", "GET\tval")
			}).
			Msg("ok")
	}
}

func BenchmarkPhase10NamespaceRedactedField(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_ns_redacted.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Namespace("auth", func(n *acacia.Namespace) {
				n.Str("token", "sensitive_pass_123")
			}).
			Msg("ok")
	}
}

func BenchmarkPhase10NamespaceFloat64(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_ns_float.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Namespace("metrics", func(n *acacia.Namespace) {
				n.Float64("nan", math.NaN()).
					Float64("pi", 3.14159)
			}).
			Msg("ok")
	}
}
