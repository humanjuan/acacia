package acacia_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
	"github.com/humanjuan/acacia/v2/internal/logging"
)

func FuzzPlainTextLogNoPanic(f *testing.F) {
	seeds := []string{
		"normal message",
		"hello\n[CRITICAL] forged",
		"hello\r\nforged",
		"quote \" slash \\",
		"\x00\x01\x02",
		"authorization: Bearer secret",
		"🍃 acacia",
		strings.Repeat("A", 1024),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		tmpDir := t.TempDir()
		lg, err := acacia.Start("fuzz_plain.log", tmpDir, acacia.Level.INFO,
			acacia.WithBufferSize(100),
			acacia.WithFlushInterval(1*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("failed to start logger: %v", err)
		}

		lg.Info(input)
		lg.Close()

		// Verify file exists
		logPath := filepath.Join(tmpDir, "fuzz_plain.log")
		if _, err := os.Stat(logPath); err != nil {
			t.Errorf("log file does not exist: %v", err)
		}
	})
}

func FuzzStructuredJSONValidLine(f *testing.F) {
	seeds := []string{
		"normal message",
		"hello\n[CRITICAL] forged",
		"hello\r\nforged",
		"quote \" slash \\",
		"\x00\x01\x02",
		"authorization: Bearer secret",
		"🍃 acacia",
		strings.Repeat("A", 1024),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		tmpDir := t.TempDir()
		lg, err := acacia.Start("fuzz_json.log", tmpDir, acacia.Level.INFO,
			acacia.WithBufferSize(100),
			acacia.WithFlushInterval(1*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("failed to start logger: %v", err)
		}
		lg.StructuredJSON(true)

		lg.Info(map[string]interface{}{"msg": input})
		lg.Close()

		logPath := filepath.Join(tmpDir, "fuzz_json.log")
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("failed to read log file: %v", err)
		}

		if len(data) == 0 {
			return
		}

		lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
		for _, line := range lines {
			if len(line) == 0 {
				continue
			}
			if !json.Valid([]byte(line)) {
				t.Errorf("invalid JSON line produced: %q", line)
			}
		}
	})
}

func FuzzStructuredJSONNoRawLineBreakInjection(f *testing.F) {
	seeds := []string{
		"normal message",
		"hello\n[CRITICAL] forged",
		"hello\r\nforged",
		"quote \" slash \\",
		"\x00\x01\x02",
		"authorization: Bearer secret",
		"🍃 acacia",
		strings.Repeat("A", 1024),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		tmpDir := t.TempDir()
		lg, err := acacia.Start("fuzz_injection.log", tmpDir, acacia.Level.INFO,
			acacia.WithBufferSize(100),
			acacia.WithFlushInterval(1*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("failed to start logger: %v", err)
		}
		lg.StructuredJSON(true)

		lg.Info(map[string]interface{}{"msg": input})
		lg.Close()

		logPath := filepath.Join(tmpDir, "fuzz_injection.log")
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("failed to read log file: %v", err)
		}

		if len(data) == 0 {
			return
		}

		// Count lines: should be exactly 1 line logged
		lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
		if len(lines) > 1 {
			t.Errorf("log injection detected! Input produced %d lines: %q", len(lines), string(data))
		}
	})
}

func FuzzRedactionNoSensitiveLeak(f *testing.F) {
	seeds := []string{
		"secret_password_123",
		"token_abc_123",
		"session_cookie_xyz",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		// Clean input check: if input itself is redacted placeholder, skip leak test
		if strings.Contains(input, "[REDACTED]") || len(input) < 3 {
			return
		}

		tmpDir := t.TempDir()
		lg, err := acacia.Start("fuzz_redact.log", tmpDir, acacia.Level.INFO,
			acacia.WithBufferSize(100),
			acacia.WithFlushInterval(1*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("failed to start logger: %v", err)
		}
		lg.StructuredJSON(true)

		wrapper := logging.NewLogger(lg)
		wrapper.Info(map[string]interface{}{
			"authorization": input,
			"cookie":        input,
			"password":      input,
			"safe_field":    "not-sensitive",
		})
		lg.Close()

		logPath := filepath.Join(tmpDir, "fuzz_redact.log")
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("failed to read log: %v", err)
		}

		if len(data) == 0 {
			return
		}

		var logMap map[string]interface{}
		if err := json.Unmarshal(data, &logMap); err != nil {
			t.Fatalf("failed to unmarshal log: %v", err)
		}

		// Verify sensitive keys do not contain the raw input
		for _, key := range []string{"authorization", "cookie", "password"} {
			if val, ok := logMap[key]; ok {
				if valStr, ok := val.(string); ok {
					if strings.Contains(valStr, input) {
						t.Errorf("sensitive leak detected! key %q contained raw secret %q: %s", key, input, string(data))
					}
				}
			}
		}
	})
}
