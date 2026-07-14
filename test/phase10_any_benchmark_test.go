package acacia_test

import (
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkPhase10AnyEventPrimitive(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_any_primitive.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Any("primitive", "hello").
			Msg("msg")
	}
}

func BenchmarkPhase10AnyEventSmallMap(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_any_small_map.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	payload := map[string]interface{}{"id": 123, "ok": true}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Any("payload", payload).
			Msg("msg")
	}
}

func BenchmarkPhase10AnyEventNestedMapRedaction(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_any_nested_map_redact.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	payload := map[string]interface{}{
		"username": "user",
		"password": "pwd",
		"nested": map[string]interface{}{
			"token": "secret_token",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Any("payload", payload).
			Msg("msg")
	}
}

type benchStruct struct {
	Name string `json:"user_name"`
	Age  int    `json:"user_age"`
}

func BenchmarkPhase10AnyEventStruct(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_any_struct.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	payload := benchStruct{Name: "Juan", Age: 30}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Any("payload", payload).
			Msg("msg")
	}
}

func BenchmarkPhase10AnyObjectSmallMap(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_any_obj_map.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	payload := map[string]interface{}{"id": 123}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Object("user", func(o *acacia.Object) {
				o.Any("raw", payload)
			}).
			Msg("msg")
	}
}

func BenchmarkPhase10AnySliceOfObjects(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_any_slice.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	payload := []interface{}{
		map[string]interface{}{"k1": "v1"},
		map[string]interface{}{"k2": "v2"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Any("payload", payload).
			Msg("msg")
	}
}

func BenchmarkPhase10AnySensitiveExternalKey(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_any_sensitive.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	payload := map[string]interface{}{"nested": "secret"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Any("password", payload).
			Msg("msg")
	}
}

func BenchmarkPhase10AnyUnsupportedFallback(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_any_unsupported.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Any("payload", func() {}).
			Msg("msg")
	}
}
