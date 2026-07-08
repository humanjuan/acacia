package acacia_test

import (
	"errors"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkRedactionExplicitSecret(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_redact_exp.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Secret("authorization", "Bearer my-secret-token").
			Send()
	}
}

func BenchmarkRedactionAutoSensitiveStr(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_redact_auto_str.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("authorization", "Bearer my-secret-token").
			Send()
	}
}

func BenchmarkRedactionAutoSensitiveMixed(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_redact_auto_mixed.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	now := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Int("password", 123).
			Bool("token", true).
			Time("access_token", now).
			ErrKey("client_secret", errors.New("my error")).
			Send()
	}
}

func BenchmarkRedactionNonSensitiveStr(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_redact_non_sens.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("user", "juan").
			Send()
	}
}

func BenchmarkRedactionKeyCheckSensitive(b *testing.B) {
	// Directly bench key normalization and check to isolate overhead
	key := "X-API-Key"
	var res bool

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Call unexported helper using a reflectionless key-check test
		// Wait, since isSensitiveKey is not exported, we can just bench Event Str key checking:
		// But wait! We can bench a simulated checker using the same code, or just bench InfoEvent with Str
		res = simulatedIsSensitiveKey(key)
	}
	_ = res
}

func BenchmarkRedactionKeyCheckNonSensitive(b *testing.B) {
	key := "regular_field_name_xyz"
	var res bool

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res = simulatedIsSensitiveKey(key)
	}
	_ = res
}

func BenchmarkRedactionDisabledLevel(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_redact_dis.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.DebugEvent().
			Str("password", "secret").
			Send()
	}
}

func BenchmarkRedactionManyFields(b *testing.B) {
	tmpDir := b.TempDir()
	lg, _ := acacia.Start("bench_redact_many.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(256<<10),
	)
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.InfoEvent().
			Str("user", "juan").
			Int("status", 200).
			Str("password", "leak").
			Bool("cached", true).
			Str("api_key", "sec").
			Send()
	}
}

// simulatedIsSensitiveKey replicates the exact implementation of isSensitiveKey in acacia.go
func simulatedIsSensitiveKey(key string) bool {
	var buf [64]byte
	w := 0
	for i := 0; i < len(key); i++ {
		c := key[i]
		if c == '-' || c == '_' || c == '.' {
			continue
		}
		if w >= 64 {
			return false
		}
		if c >= 'A' && c <= 'Z' {
			buf[w] = c + ('a' - 'A')
		} else {
			buf[w] = c
		}
		w++
	}
	normalized := buf[:w]

	switch string(normalized) {
	case "authorization", "cookie", "setcookie", "token", "accesstoken", "refreshtoken",
		"password", "passwd", "pwd", "apikey", "privatekey", "clientsecret", "secret",
		"bearer", "jwt", "session", "sessionid", "csrf", "xsrf", "xapikey":
		return true
	default:
		return false
	}
}
