package acacia_test

import (
	"bufio"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func setupTestLogger(t *testing.T) (*acacia.Log, string) {
	tmpDir, err := os.MkdirTemp("", "acacia-flat-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	logPath := filepath.Join(tmpDir, "test.log")
	logger, err := acacia.Start("test.log", tmpDir, "info", acacia.WithBufferSize(1000), acacia.WithBackpressurePolicy(acacia.DropNewest))
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to start logger: %v", err)
	}

	t.Cleanup(func() {
		logger.Close()
		os.RemoveAll(tmpDir)
	})

	return logger, logPath
}

func readLastLine(t *testing.T, filepath string) string {
	file, err := os.Open(filepath)
	if err != nil {
		t.Fatalf("failed to open log file: %v", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var lastLine string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("failed to read line: %v", err)
		}
		lastLine = line
	}
	return strings.TrimSpace(lastLine)
}

func TestPhase10FlatJSONEscapesControlCharacters(t *testing.T) {
	lg, logPath := setupTestLogger(t)
	lg.InfoEvent().Str("ctrl", "hello\x01\x1fworld").Msg("msg")
	lg.Sync()

	line := readLastLine(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["ctrl"] != "hello\u0001\u001fworld" {
		t.Errorf("control characters were not escaped correctly: got %q", m["ctrl"])
	}
}

func TestPhase10FlatJSONEscapesQuotesAndBackslashes(t *testing.T) {
	lg, logPath := setupTestLogger(t)
	lg.InfoEvent().Str(`quote"back\slash`, `val"ue\path`).Msg("msg")
	lg.Sync()

	line := readLastLine(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	val, ok := m[`quote"back\slash`]
	if !ok {
		t.Fatalf("escaped key not found in JSON map")
	}
	if val != `val"ue\path` {
		t.Errorf("quotes or backslashes not escaped correctly: got %q", val)
	}
}

func TestPhase10FlatJSONRejectsLogInjectionByEscapingCRLF(t *testing.T) {
	lg, logPath := setupTestLogger(t)
	lg.InfoEvent().Str("injection\r\nkey", "value\r\nvalue").Msg("msg\nmsg")
	lg.Sync()

	file, err := os.Open(logPath)
	if err != nil {
		t.Fatalf("failed to open log file: %v", err)
	}
	defer file.Close()

	// Check that there is only ONE newline at the end (the log has 1 event)
	scanner := bufio.NewScanner(file)
	lines := 0
	for scanner.Scan() {
		lines++
		line := scanner.Text()
		if !json.Valid([]byte(line)) {
			t.Errorf("line %d is not valid JSON: %s", lines, line)
		}
	}

	if lines != 1 {
		t.Errorf("log injection occurred: expected 1 line, got %d", lines)
	}
}

func TestPhase10FlatJSONInvalidUTF8StillValidJSON(t *testing.T) {
	lg, logPath := setupTestLogger(t)
	// \xff\xfe is invalid UTF-8 sequence
	lg.InfoEvent().Str("invalid_utf8", "hello\xff\xfeworld").Msg("msg")
	lg.Sync()

	line := readLastLine(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	val := m["invalid_utf8"].(string)
	if !strings.Contains(val, "\uFFFD") {
		t.Errorf("invalid UTF-8 was not replaced with Unicode replacement char: got %q", val)
	}
}

func TestPhase10FlatJSONDuplicateKeysAreEmitted(t *testing.T) {
	lg, logPath := setupTestLogger(t)
	lg.InfoEvent().
		Str("dup", "first").
		Str("dup", "second").
		Msg("msg")
	lg.Sync()

	line := readLastLine(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	// Verify that BOTH keys physically exist in the raw JSON string
	firstIdx := strings.Index(line, `"dup":"first"`)
	secondIdx := strings.Index(line, `"dup":"second"`)

	if firstIdx == -1 || secondIdx == -1 {
		t.Errorf("expected both duplicate keys in raw line, but got: %s", line)
	}
}

func TestPhase10FlatJSONEmptyKeyAndEmptyValue(t *testing.T) {
	lg, logPath := setupTestLogger(t)
	lg.InfoEvent().Str("", "").Msg("")
	lg.Sync()

	line := readLastLine(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	val, ok := m[""]
	if !ok {
		t.Errorf("empty key was not written")
	}
	if val != "" {
		t.Errorf("empty value was not written: got %q", val)
	}
}

func TestPhase10FlatJSONNilErrorIsOmitted(t *testing.T) {
	lg, logPath := setupTestLogger(t)
	lg.InfoEvent().Err(nil).Msg("msg")
	lg.Sync()

	line := readLastLine(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if _, ok := m["error"]; ok {
		t.Errorf("error key was written for nil error")
	}
}

func TestPhase10FlatJSONSensitiveKeysAreRedacted(t *testing.T) {
	lg, logPath := setupTestLogger(t)
	lg.InfoEvent().
		Str("password", "supersecret").
		Str("AccessToken", "token123").
		Str("api-key", "key456").
		Msg("msg")
	lg.Sync()

	line := readLastLine(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "supersecret") || strings.Contains(line, "token123") || strings.Contains(line, "key456") {
		t.Errorf("sensitive values leaked in raw line: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["password"] != "[REDACTED]" || m["AccessToken"] != "[REDACTED]" || m["api-key"] != "[REDACTED]" {
		t.Errorf("redaction failed: %s", line)
	}
}

func TestPhase10FlatJSONSecretAndStrRedacted(t *testing.T) {
	lg, logPath := setupTestLogger(t)
	lg.InfoEvent().
		Secret("custom_secret", "secret_val").
		StrRedacted("custom_redacted", "redacted_val").
		Msg("msg")
	lg.Sync()

	line := readLastLine(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "secret_val") || strings.Contains(line, "redacted_val") {
		t.Errorf("values leaked in raw line: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["custom_secret"] != "[REDACTED]" || m["custom_redacted"] != "[REDACTED]" {
		t.Errorf("explicit redaction failed: %s", line)
	}
}

func TestPhase10FlatJSONLineIsValidJSON(t *testing.T) {
	lg, logPath := setupTestLogger(t)
	lg.InfoEvent().
		Str("str", "abc").
		Int("int", 123).
		Int64("int64", 456).
		Uint64("uint64", 789).
		Bool("bool", true).
		Float64("float", 1.23).
		Dur("duration", 5*time.Second).
		Time("time", time.Now()).
		Msg("message")
	lg.Sync()

	line := readLastLine(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}
}

func TestPhase10FlatJSONLegacyMapPathStillWorks(t *testing.T) {
	lg, logPath := setupTestLogger(t)
	lg.StructuredJSON(true)
	lg.Info(map[string]interface{}{
		"legacy": "value",
		"number": 42,
	})
	lg.Sync()

	line := readLastLine(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["legacy"] != "value" || m["number"].(float64) != 42 {
		t.Errorf("legacy map path failed: got %s", line)
	}
}

func TestPhase10FlatJSONFloat64NonFiniteStillValidJSON(t *testing.T) {
	lg, logPath := setupTestLogger(t)
	lg.InfoEvent().
		Float64("nan", math.NaN()).
		Float64("pos_inf", math.Inf(1)).
		Float64("neg_inf", math.Inf(-1)).
		Msg("msg")
	lg.Sync()

	line := readLastLine(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	// Verify raw line does not contain non-quoted NaN/Inf
	if strings.Contains(line, ":NaN") || strings.Contains(line, ":+Inf") || strings.Contains(line, ":-Inf") {
		t.Errorf("raw line contains unquoted non-finite values: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["nan"] != "NaN" || m["pos_inf"] != "+Inf" || m["neg_inf"] != "-Inf" {
		t.Errorf("non-finite values were not converted to expected strings: got %s", line)
	}
}
