package acacia_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func TestRedactionExplicitSecret(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("redact_explicit.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(100),
		acacia.WithFlushInterval(1*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	lg.InfoEvent().
		Secret("authorization", "Bearer raw-token").
		StrRedacted("cookie", "session_id=123").
		Send()

	lg.Close()

	logPath := filepath.Join(tmpDir, "redact_explicit.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}

	logStr := string(data)
	if strings.Contains(logStr, "raw-token") || strings.Contains(logStr, "session_id=123") {
		t.Errorf("explicit secrets leaked in logs: %s", logStr)
	}

	var m map[string]interface{}
	json.Unmarshal(data, &m)
	if m["authorization"] != "[REDACTED]" || m["cookie"] != "[REDACTED]" {
		t.Errorf("explicit fields not redacted correctly: %v", m)
	}
}

func TestRedactionAutoSensitive(t *testing.T) {
	sensitiveKeys := []string{
		"authorization", "Authorization", "AUTHORIZATION",
		"cookie", "set-cookie", "token", "access_token", "refresh_token",
		"password", "api_key", "private_key", "client_secret", "secret",
		"x-api-key", "apiKey", "accessToken", "clientSecret", "session-id", "csrf",
	}

	for _, key := range sensitiveKeys {
		t.Run(key, func(t *testing.T) {
			tmpDir := t.TempDir()
			lg, err := acacia.Start("redact_auto.log", tmpDir, acacia.Level.INFO,
				acacia.WithBufferSize(100),
				acacia.WithFlushInterval(1*time.Millisecond),
			)
			if err != nil {
				t.Fatalf("failed to start logger: %v", err)
			}

			leakVal := "LEAK_" + key + "_VAL"
			lg.InfoEvent().Str(key, leakVal).Send()
			lg.Close()

			logPath := filepath.Join(tmpDir, "redact_auto.log")
			data, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatalf("failed to read log: %v", err)
			}

			logStr := string(data)
			if strings.Contains(logStr, leakVal) {
				t.Errorf("auto leak detected for key %q: %s", key, logStr)
			}

			var m map[string]interface{}
			json.Unmarshal(data, &m)
			// Find case-normalized key inside map
			found := false
			for k, v := range m {
				if strings.EqualFold(k, key) {
					found = true
					if v != "[REDACTED]" {
						t.Errorf("key %q was not redacted, got: %v", key, v)
					}
				}
			}
			if !found {
				t.Errorf("key %q not found in log output: %s", key, logStr)
			}
		})
	}
}

func TestRedactionNonSensitivePreserved(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("redact_non_sensitive.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(100),
		acacia.WithFlushInterval(1*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	lg.InfoEvent().
		Str("user", "juan").
		Str("service", "owl").
		Int("status", 200).
		Str("request_id", "req-123").
		Send()

	lg.Close()

	logPath := filepath.Join(tmpDir, "redact_non_sensitive.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}

	var m map[string]interface{}
	json.Unmarshal(data, &m)

	if m["user"] != "juan" || m["service"] != "owl" || m["status"] != float64(200) || m["request_id"] != "req-123" {
		t.Errorf("non-sensitive values were altered: %v", m)
	}
}

func TestRedactionMixedTypes(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("redact_mixed.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(100),
		acacia.WithFlushInterval(1*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	now := time.Now()
	lg.InfoEvent().
		Int("password", 123).
		Bool("token", true).
		Float64("api_key", 1.23).
		Time("access_token", now).
		Dur("refresh_token", time.Second).
		ErrKey("client_secret", errors.New("my error")).
		Send()

	lg.Close()

	logPath := filepath.Join(tmpDir, "redact_mixed.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}

	logStr := string(data)
	if strings.Contains(logStr, "123") && strings.Contains(logStr, `"password"`) {
		t.Error("leak: password int value was not redacted")
	}
	if strings.Contains(logStr, "true") && strings.Contains(logStr, `"token"`) {
		t.Error("leak: token bool value was not redacted")
	}
	if strings.Contains(logStr, "1.23") && strings.Contains(logStr, `"api_key"`) {
		t.Error("leak: api_key float value was not redacted")
	}
	if strings.Contains(logStr, "my error") && strings.Contains(logStr, `"client_secret"`) {
		t.Error("leak: client_secret error value was not redacted")
	}

	var m map[string]interface{}
	json.Unmarshal(data, &m)
	keys := []string{"password", "token", "api_key", "access_token", "refresh_token", "client_secret"}
	for _, k := range keys {
		if m[k] != "[REDACTED]" {
			t.Errorf("expected redacted for key %q, got: %v", k, m[k])
		}
	}
}

func TestRedactionLegacyCoexistence(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("redact_legacy.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(100),
		acacia.WithFlushInterval(1*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}
	lg.StructuredJSON(true)

	lg.Info(map[string]interface{}{"user": "juan", "status": 200})
	lg.Close()

	logPath := filepath.Join(tmpDir, "redact_legacy.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}

	var m map[string]interface{}
	json.Unmarshal(data, &m)
	if m["user"] != "juan" || m["status"] != float64(200) {
		t.Errorf("legacy map logging failed: %v", m)
	}
}
