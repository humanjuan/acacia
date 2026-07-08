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

func TestJSONEncoderPrimitives(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("encoder_primitives.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(100),
		acacia.WithFlushInterval(1*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	lg.InfoEvent().
		Str("string_key", "string_val").
		Int("int_key", -123).
		Int64("int64_key", -456).
		Uint64("uint64_key", 789).
		Bool("bool_key", true).
		Float64("float_key", 123.456).
		Dur("dur_key", 5*time.Second).
		Time("time_key", time.Unix(1710000000, 0).UTC()).
		Err(errors.New("sample error")).
		Msg("primitives logged")

	lg.Close()

	logPath := filepath.Join(tmpDir, "encoder_primitives.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}

	line := strings.TrimSpace(string(data))
	if !json.Valid([]byte(line)) {
		t.Fatalf("invalid JSON output: %q", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	if m["string_key"] != "string_val" {
		t.Errorf("expected string_val, got %v", m["string_key"])
	}
	if m["int_key"] != float64(-123) {
		t.Errorf("expected -123, got %v", m["int_key"])
	}
	if m["int64_key"] != float64(-456) {
		t.Errorf("expected -456, got %v", m["int64_key"])
	}
	if m["uint64_key"] != float64(789) {
		t.Errorf("expected 789, got %v", m["uint64_key"])
	}
	if m["bool_key"] != true {
		t.Errorf("expected true, got %v", m["bool_key"])
	}
	if m["float_key"] != 123.456 {
		t.Errorf("expected 123.456, got %v", m["float_key"])
	}
	if m["dur_key"] != float64(5000000000) {
		t.Errorf("expected 5000000000, got %v", m["dur_key"])
	}
	if m["time_key"] != "2024-03-09T16:00:00Z" {
		t.Errorf("expected 2024-03-09T16:00:00Z, got %v", m["time_key"])
	}
	if m["error"] != "sample error" {
		t.Errorf("expected 'sample error', got %v", m["error"])
	}
	if m["msg"] != "primitives logged" {
		t.Errorf("expected 'primitives logged', got %v", m["msg"])
	}
}

func TestJSONEncoderEscaping(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("encoder_escaping.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(100),
		acacia.WithFlushInterval(1*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	testString := "normal quote \" backslash \\ newline \n carriage \r tab \t backspace \b formfeed \f control \x00\x01\x02 unicode 🍃"
	lg.InfoEvent().
		Str("escaped_key", testString).
		Msg("msg")

	lg.Close()

	logPath := filepath.Join(tmpDir, "encoder_escaping.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected exactly 1 line, got %d", len(lines))
	}

	line := lines[0]
	if !json.Valid([]byte(line)) {
		t.Fatalf("invalid JSON: %q", line)
	}

	var m map[string]interface{}
	json.Unmarshal([]byte(line), &m)
	expected := "normal quote \" backslash \\ newline \n carriage \r tab \t backspace \b formfeed \f control \u0000\u0001\u0002 unicode 🍃"
	if m["escaped_key"] != expected {
		t.Errorf("expected %q, got %q", expected, m["escaped_key"])
	}
}

func TestJSONEncoderSecrets(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("encoder_secrets.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(100),
		acacia.WithFlushInterval(1*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	lg.InfoEvent().
		Secret("authorization", "Bearer top-secret").
		StrRedacted("cookie", "sess=abc").
		Send()

	lg.Close()

	logPath := filepath.Join(tmpDir, "encoder_secrets.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}

	logStr := string(data)
	if strings.Contains(logStr, "top-secret") {
		t.Errorf("secret leaked: %s", logStr)
	}
	if strings.Contains(logStr, "sess=abc") {
		t.Errorf("cookie leaked: %s", logStr)
	}

	var m map[string]interface{}
	json.Unmarshal(data, &m)
	if m["authorization"] != "[REDACTED]" {
		t.Errorf("expected redacted auth, got %v", m["authorization"])
	}
	if m["cookie"] != "[REDACTED]" {
		t.Errorf("expected redacted cookie, got %v", m["cookie"])
	}
}

func TestJSONEncoderLegacyCoexistence(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("encoder_legacy.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(100),
		acacia.WithFlushInterval(1*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}
	lg.StructuredJSON(true)

	// Call legacy Map API
	lg.Info(map[string]interface{}{"service": "legacy", "code": 200})
	lg.Close()

	logPath := filepath.Join(tmpDir, "encoder_legacy.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}

	line := strings.TrimSpace(string(data))
	if !json.Valid([]byte(line)) {
		t.Fatalf("invalid JSON: %q", line)
	}

	var m map[string]interface{}
	json.Unmarshal([]byte(line), &m)
	if m["service"] != "legacy" || m["code"] != float64(200) {
		t.Errorf("unexpected legacy log payload: %v", m)
	}
}

func TestJSONEncoderLargePayloads(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("encoder_large.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(5_000_000),
		acacia.WithBufferCap(1<<20),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	str1K := strings.Repeat("A", 1024)
	str4K := strings.Repeat("B", 4096)
	str8K := strings.Repeat("C", 8192)

	lg.InfoEvent().Str("k1K", str1K).Send()
	lg.InfoEvent().Str("k4K", str4K).Send()
	lg.InfoEvent().Str("k8K", str8K).Send()

	lg.Close()

	logPath := filepath.Join(tmpDir, "encoder_large.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}

	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(lines))
	}

	for i, line := range lines {
		if !json.Valid([]byte(line)) {
			t.Errorf("line %d is not valid JSON: %q", i, line)
		}
	}
}
