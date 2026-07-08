package acacia_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func FuzzEventBuilderJSONValid(f *testing.F) {
	seeds := []string{
		"normal msg",
		"hello\nworld",
		"quote \" slash \\",
		"\x00\x01\x02",
		"🍃 acacia",
		strings.Repeat("A", 1024),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		tmpDir := t.TempDir()
		lg, err := acacia.Start("fuzz_builder_valid.log", tmpDir, acacia.Level.INFO,
			acacia.WithBufferSize(100),
			acacia.WithFlushInterval(1*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("failed to start logger: %v", err)
		}

		lg.InfoEvent().
			Str("fuzz_key", input).
			Msg("msg")

		lg.Close()

		logPath := filepath.Join(tmpDir, "fuzz_builder_valid.log")
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("failed to read log: %v", err)
		}

		if len(data) == 0 {
			return
		}

		line := strings.TrimSpace(string(data))
		if !json.Valid([]byte(line)) {
			t.Errorf("invalid JSON produced by builder: %q", line)
		}
	})
}

func FuzzEventBuilderNoLineInjection(f *testing.F) {
	seeds := []string{
		"normal msg",
		"hello\n[CRITICAL] forged",
		"hello\r\nforged",
		"\n\n\n",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		tmpDir := t.TempDir()
		lg, err := acacia.Start("fuzz_builder_injection.log", tmpDir, acacia.Level.INFO,
			acacia.WithBufferSize(100),
			acacia.WithFlushInterval(1*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("failed to start logger: %v", err)
		}

		lg.InfoEvent().
			Str("fuzz_key", input).
			Msg("msg")

		lg.Close()

		logPath := filepath.Join(tmpDir, "fuzz_builder_injection.log")
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("failed to read log: %v", err)
		}

		if len(data) == 0 {
			return
		}

		// Verify exactly 1 line logged
		lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
		if len(lines) > 1 {
			t.Errorf("line break injection detected in event builder! input: %q, lines: %d", input, len(lines))
		}
	})
}

func FuzzEventBuilderSecretNoLeak(f *testing.F) {
	seeds := []string{
		"secret_token_123",
		"bearer-xyz-999",
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
		lg, err := acacia.Start("fuzz_builder_secret.log", tmpDir, acacia.Level.INFO,
			acacia.WithBufferSize(100),
			acacia.WithFlushInterval(1*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("failed to start logger: %v", err)
		}

		lg.InfoEvent().
			Secret("authorization", input).
			Send()

		lg.Close()

		logPath := filepath.Join(tmpDir, "fuzz_builder_secret.log")
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

		if val, ok := logMap["authorization"]; ok {
			if valStr, ok := val.(string); ok {
				if strings.Contains(valStr, input) {
					t.Errorf("secret leaked in builder! expected redacted, got: %q", valStr)
				}
			}
		}
	})
}
