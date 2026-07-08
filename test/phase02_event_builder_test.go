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

func TestEventBuilderJSONValid(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("builder_valid.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(100),
		acacia.WithFlushInterval(1*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	now := time.Now()
	lg.InfoEvent().
		Str("service", "owl").
		Int("status", 200).
		Int64("latency_ns", 123456789).
		Uint64("req_id", 9999).
		Bool("cached", true).
		Float64("pi", 3.14159).
		Err(errors.New("db error")).
		Dur("timeout", 5*time.Second).
		Time("started_at", now).
		Msg("request completed")

	lg.Close()

	logPath := filepath.Join(tmpDir, "builder_valid.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	line := strings.TrimSpace(string(data))
	if len(line) == 0 {
		t.Fatal("empty log output")
	}

	if !json.Valid([]byte(line)) {
		t.Fatalf("produced line is not valid JSON: %q", line)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(line), &parsed); err != nil {
		t.Fatalf("failed to unmarshal log line: %v", err)
	}

	// Verify values
	if parsed["service"] != "owl" {
		t.Errorf("expected 'owl', got %v", parsed["service"])
	}
	if parsed["status"] != float64(200) {
		t.Errorf("expected 200, got %v", parsed["status"])
	}
	if parsed["latency_ns"] != float64(123456789) {
		t.Errorf("expected 123456789, got %v", parsed["latency_ns"])
	}
	if parsed["req_id"] != float64(9999) {
		t.Errorf("expected 9999, got %v", parsed["req_id"])
	}
	if parsed["cached"] != true {
		t.Errorf("expected true, got %v", parsed["cached"])
	}
	if parsed["pi"] != 3.14159 {
		t.Errorf("expected 3.14159, got %v", parsed["pi"])
	}
	if parsed["error"] != "db error" {
		t.Errorf("expected 'db error', got %v", parsed["error"])
	}
	if parsed["timeout"] != float64(5000000000) {
		t.Errorf("expected 5000000000, got %v", parsed["timeout"])
	}
	if parsed["msg"] != "request completed" {
		t.Errorf("expected 'request completed', got %v", parsed["msg"])
	}
}

func TestEventBuilderEscaping(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("builder_escaping.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(100),
		acacia.WithFlushInterval(1*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	lg.InfoEvent().
		Str("special_key", "hello\nworld\r\"test\"\\backslash\traw\x00\x01\x02").
		Msg("escaped msg")

	lg.Close()

	logPath := filepath.Join(tmpDir, "builder_escaping.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected exactly 1 line, got %d lines: %q", len(lines), string(data))
	}

	line := lines[0]
	if !json.Valid([]byte(line)) {
		t.Fatalf("invalid JSON: %q", line)
	}

	var parsed map[string]interface{}
	json.Unmarshal([]byte(line), &parsed)
	expectedVal := "hello\nworld\r\"test\"\\backslash\traw\u0000\u0001\u0002"
	if parsed["special_key"] != expectedVal {
		t.Errorf("expected %q, got %q", expectedVal, parsed["special_key"])
	}
}

func TestEventBuilderNoBreakLegacy(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("legacy_coexistence.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(100),
		acacia.WithFlushInterval(1*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}
	lg.StructuredJSON(true)

	// Call legacy Info
	lg.Info(map[string]interface{}{"service": "legacy_service", "status": 404})
	lg.Close()

	logPath := filepath.Join(tmpDir, "legacy_coexistence.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}

	line := strings.TrimSpace(string(data))
	if !json.Valid([]byte(line)) {
		t.Fatalf("invalid JSON: %q", line)
	}

	var parsed map[string]interface{}
	json.Unmarshal([]byte(line), &parsed)
	if parsed["service"] != "legacy_service" || parsed["status"] != float64(404) {
		t.Errorf("legacy fields incorrect: %v", parsed)
	}
}

func TestEventBuilderSyncClose(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("sync_close.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(100),
		acacia.WithFlushInterval(10*time.Second), // Long flush to test explicit Sync
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	lg.InfoEvent().Str("evt", "first").Send()
	lg.Sync()

	logPath := filepath.Join(tmpDir, "sync_close.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log after Sync: %v", err)
	}

	if !strings.Contains(string(data), "first") {
		t.Errorf("log did not contain 'first' after Sync: %q", string(data))
	}

	lg.InfoEvent().Str("evt", "second").Send()
	lg.Close()

	data, err = os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log after Close: %v", err)
	}

	if !strings.Contains(string(data), "second") {
		t.Errorf("log did not contain 'second' after Close: %q", string(data))
	}
}

func TestEventBuilderSecrets(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("secrets.log", tmpDir, acacia.Level.INFO,
		acacia.WithBufferSize(100),
		acacia.WithFlushInterval(1*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}

	lg.InfoEvent().
		Secret("authorization", "Bearer my-secret-token").
		StrRedacted("cookie", "session=123").
		Send()

	lg.Close()

	logPath := filepath.Join(tmpDir, "secrets.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}

	logStr := string(data)
	if strings.Contains(logStr, "my-secret-token") {
		t.Errorf("leaked secret value in logs: %s", logStr)
	}
	if strings.Contains(logStr, "session=123") {
		t.Errorf("leaked session value in logs: %s", logStr)
	}

	var parsed map[string]interface{}
	json.Unmarshal([]byte(logStr), &parsed)
	if parsed["authorization"] != "[REDACTED]" {
		t.Errorf("expected [REDACTED], got %v", parsed["authorization"])
	}
	if parsed["cookie"] != "[REDACTED]" {
		t.Errorf("expected [REDACTED], got %v", parsed["cookie"])
	}
}
