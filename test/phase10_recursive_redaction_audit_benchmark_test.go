package acacia_test

import (
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkPhase10RedactionAuditFlatSensitiveKey(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_audit_flat.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("password", "secret_pass_123").
			Msg("msg")
	}
}

func BenchmarkPhase10RedactionAuditObjectSensitiveKey(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_audit_obj.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Object("user", func(o *acacia.Object) {
				o.Str("password", "secret_pass_123")
			}).
			Msg("msg")
	}
}

func BenchmarkPhase10RedactionAuditArrayObjectSensitiveKey(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_audit_arr.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Array("users", func(a *acacia.Array) {
				a.Object(func(o *acacia.Object) {
					o.Str("password", "secret_pass_123")
				})
			}).
			Msg("msg")
	}
}

func BenchmarkPhase10RedactionAuditNamespaceSensitiveKey(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_audit_ns.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Namespace("auth", func(n *acacia.Namespace) {
				n.Str("token", "secret_token_123")
			}).
			Msg("msg")
	}
}

func BenchmarkPhase10RedactionAuditRawJSONSensitiveExternalKey(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_audit_raw.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	payload := []byte(`{"a":1}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			RawJSON("password", payload).
			Msg("msg")
	}
}

func BenchmarkPhase10RedactionAuditAnyNestedRedaction(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_audit_any.log", tmpDir, acacia.Level.INFO,
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
