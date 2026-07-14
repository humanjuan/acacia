package acacia_test

import (
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkPhase10RawJSONEventSmallObject(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_raw_small_obj.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	payload := []byte(`{"id":123,"ok":true}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			RawJSON("payload", payload).
			Msg("msg")
	}
}

func BenchmarkPhase10RawJSONEventArray(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_raw_arr.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	payload := []byte(`[1,2,3,4,5]`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			RawJSON("payload", payload).
			Msg("msg")
	}
}

func BenchmarkPhase10RawJSONEventScalar(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_raw_scalar.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	payload := []byte(`"hello scalar"`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			RawJSON("payload", payload).
			Msg("msg")
	}
}

func BenchmarkPhase10RawJSONObjectSmall(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_raw_obj_small.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	payload := []byte(`{"a":1}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Object("user", func(o *acacia.Object) {
				o.RawJSON("raw", payload)
			}).
			Msg("msg")
	}
}

func BenchmarkPhase10RawJSONInvalidFallback(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_raw_invalid.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	payload := []byte(`{"broken":`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			RawJSON("payload", payload).
			Msg("msg")
	}
}

func BenchmarkPhase10RawJSONSensitiveKey(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_raw_sensitive.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	payload := []byte(`{"nested":"secret"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			RawJSON("password", payload).
			Msg("msg")
	}
}

func BenchmarkPhase10RawJSONMultilineCompaction(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_raw_multiline.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	payload := []byte("{\n  \"id\": 123,\n  \"ok\": true\n}")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			RawJSON("payload", payload).
			Msg("msg")
	}
}
