package acacia_test

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func setupRawJSONTestLogger(t *testing.T) (*acacia.Log, string) {
	tmpDir, err := os.MkdirTemp("", "acacia-rawjson-test-*")
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

func readLastLineRawJSON(t *testing.T, filepath string) string {
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

func TestPhase10RawJSONEventObjectPayloadValidJSON(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	lg.InfoEvent().RawJSON("payload", []byte(`{"id":123,"ok":true}`)).Msg("msg")
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	pay, ok := m["payload"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing or invalid 'payload' object: %s", line)
	}
	if pay["id"].(float64) != 123 || pay["ok"] != true {
		t.Errorf("fields inside payload object are incorrect: %+v", pay)
	}
}

func TestPhase10RawJSONEventArrayPayloadValidJSON(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	lg.InfoEvent().RawJSON("payload", []byte(`[1,2,{"a":"b"}]`)).Msg("msg")
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	arr, ok := m["payload"].([]interface{})
	if !ok {
		t.Fatalf("payload is not an array: %v", m["payload"])
	}
	if len(arr) != 3 || arr[0].(float64) != 1 || arr[1].(float64) != 2 {
		t.Errorf("incorrect array contents: %v", arr)
	}
}

func TestPhase10RawJSONEventStringPayloadValidJSON(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	lg.InfoEvent().RawJSON("payload", []byte(`"hello"`)).Msg("msg")
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["payload"] != "hello" {
		t.Errorf("expected string 'hello', got: %v", m["payload"])
	}
}

func TestPhase10RawJSONEventNumberBoolNullPayloadValidJSON(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	lg.InfoEvent().
		RawJSON("num", []byte("123.45")).
		RawJSON("bool", []byte("true")).
		RawJSON("null", []byte("null")).
		Msg("msg")
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["num"].(float64) != 123.45 {
		t.Errorf("expected 123.45, got %v", m["num"])
	}
	if m["bool"] != true {
		t.Errorf("expected true, got %v", m["bool"])
	}
	if m["null"] != nil {
		t.Errorf("expected nil, got %v", m["null"])
	}
}

func TestPhase10RawJSONObjectPayloadValidJSON(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	lg.InfoEvent().
		Object("obj", func(o *acacia.Object) {
			o.RawJSON("raw", []byte(`{"nested":true}`))
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	obj := m["obj"].(map[string]interface{})
	raw := obj["raw"].(map[string]interface{})
	if raw["nested"] != true {
		t.Errorf("expected true, got %v", raw["nested"])
	}
}

func TestPhase10RawJSONObjectFirstFieldAndAfterExistingField(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	lg.InfoEvent().
		Object("obj1", func(o *acacia.Object) {
			o.RawJSON("raw_first", []byte(`{"a":1}`)).
				Str("after", "ok")
		}).
		Object("obj2", func(o *acacia.Object) {
			o.Str("before", "ok").
				RawJSON("raw_after", []byte(`{"b":2}`))
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	obj1 := m["obj1"].(map[string]interface{})
	rawFirst := obj1["raw_first"].(map[string]interface{})
	if rawFirst["a"].(float64) != 1 || obj1["after"] != "ok" {
		t.Errorf("obj1 layout incorrect: %+v", obj1)
	}

	obj2 := m["obj2"].(map[string]interface{})
	rawAfter := obj2["raw_after"].(map[string]interface{})
	if obj2["before"] != "ok" || rawAfter["b"].(float64) != 2 {
		t.Errorf("obj2 layout incorrect: %+v", obj2)
	}
}

func TestPhase10RawJSONInvalidPayloadDegradesToEscapedString(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	lg.InfoEvent().RawJSON("payload", []byte(`{"broken":`)).Msg("msg")
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["payload"] != `{"broken":` {
		t.Errorf("expected degraded string value, got: %q", m["payload"])
	}
}

func TestPhase10RawJSONNilAndEmptyPayloadDegradeToEmptyString(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	lg.InfoEvent().
		RawJSON("nil_payload", nil).
		RawJSON("empty_payload", []byte{}).
		Msg("msg")
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["nil_payload"] != "" {
		t.Errorf("expected empty string for nil raw, got %v", m["nil_payload"])
	}
	if m["empty_payload"] != "" {
		t.Errorf("expected empty string for empty raw, got %v", m["empty_payload"])
	}
}

func TestPhase10RawJSONInvalidUTF8DegradesSafely(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	// \xff\xfe is invalid UTF-8 sequence, making the entire JSON payload technically invalid or unsafe
	lg.InfoEvent().RawJSON("payload", []byte("{\"a\":\"hello\xff\xfeworld\"}")).Msg("msg")
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// Because of invalid UTF-8 inside a JSON structure, it should be processed safely.
	// If it degrades to string, it should contain the replacement characters.
	val := m["payload"]
	if s, ok := val.(string); ok {
		if !strings.Contains(s, "\uFFFD") {
			t.Errorf("invalid UTF-8 replacement char missing in: %q", s)
		}
	} else if obj, ok := val.(map[string]interface{}); ok {
		if s2, ok2 := obj["a"].(string); ok2 {
			if !strings.Contains(s2, "\uFFFD") {
				t.Errorf("invalid UTF-8 replacement char missing in nested: %q", s2)
			}
		}
	} else {
		t.Errorf("unexpected payload type: %T", val)
	}
}

func TestPhase10RawJSONSensitiveEventKeyRedactsEntirePayload(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	lg.InfoEvent().RawJSON("password", []byte(`{"nested":"secret"}`)).Msg("msg")
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "nested") || strings.Contains(line, "secret") {
		t.Errorf("sensitive value leaked in line: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["password"] != "[REDACTED]" {
		t.Errorf("expected '[REDACTED]', got: %v", m["password"])
	}
}

func TestPhase10RawJSONSensitiveObjectKeyRedactsEntirePayload(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	lg.InfoEvent().
		Object("obj", func(o *acacia.Object) {
			o.RawJSON("client_secret", []byte(`{"token":"abc"}`))
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	obj := m["obj"].(map[string]interface{})
	if obj["client_secret"] != "[REDACTED]" {
		t.Errorf("expected redacted, got: %v", obj["client_secret"])
	}
}

func TestPhase10RawJSONTrustedInputDoesNotRedactNestedPayload(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	lg.InfoEvent().RawJSON("payload", []byte(`{"password":"visible-by-contract"}`)).Msg("msg")
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	pay := m["payload"].(map[string]interface{})
	if pay["password"] != "visible-by-contract" {
		t.Errorf("trusted input violated, password was redacted internally: %+v", pay)
	}
}

func TestPhase10RawJSONMultilineValidPayloadIsCompactedToSingleLine(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	multilineJSON := []byte("{\n  \"id\": 123,\r\n  \"ok\": true\n}")
	lg.InfoEvent().RawJSON("payload", multilineJSON).Msg("msg")
	lg.Sync()

	file, err := os.Open(logPath)
	if err != nil {
		t.Fatalf("failed to open log file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := 0
	for scanner.Scan() {
		lines++
		line := scanner.Text()
		if !json.Valid([]byte(line)) {
			t.Errorf("line %d is not valid JSON: %s", lines, line)
		}
		if strings.Contains(line, "\n") || strings.Contains(line, "\r") {
			t.Errorf("line contains raw CR or LF: %q", line)
		}
	}

	if lines != 1 {
		t.Errorf("expected exactly 1 line, got %d. Log injection/newline leak occurred.", lines)
	}
}

func TestPhase10RawJSONEscapesKeys(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	lg.InfoEvent().RawJSON("payload\nkey", []byte(`{"a":1}`)).Msg("msg")
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if _, ok := m["payload\nkey"]; !ok {
		t.Errorf("escaped key 'payload\\nkey' not found in: %s", line)
	}
}

func TestPhase10RawJSONFlatBuilderStillWorks(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	lg.InfoEvent().Str("a", "b").Int("x", 42).Msg("msg")
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if m["a"] != "b" || m["x"].(float64) != 42 {
		t.Errorf("flat builder regression: %+v", m)
	}
}

func TestPhase10RawJSONObjectBuilderStillWorks(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	lg.InfoEvent().
		Object("obj", func(o *acacia.Object) {
			o.Str("k", "v")
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	obj := m["obj"].(map[string]interface{})
	if obj["k"] != "v" {
		t.Errorf("object builder regression: %+v", obj)
	}
}

func TestPhase10RawJSONArrayBuilderStillWorks(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	lg.InfoEvent().
		Array("arr", func(a *acacia.Array) {
			a.Str("item")
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	arr := m["arr"].([]interface{})
	if len(arr) != 1 || arr[0] != "item" {
		t.Errorf("array builder regression: %+v", arr)
	}
}

func TestPhase10RawJSONGroupNamespaceStillWork(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	lg.InfoEvent().
		Group("grp", func(g *acacia.Object) {
			g.Str("k", "v")
		}).
		Namespace("ns", func(n *acacia.Namespace) {
			n.Str("k", "v")
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	grp := m["grp"].(map[string]interface{})
	if grp["k"] != "v" || m["ns.k"] != "v" {
		t.Errorf("group/namespace regression: %+v", m)
	}
}

func TestPhase10RawJSONLegacyMapPathStillWorks(t *testing.T) {
	lg, logPath := setupRawJSONTestLogger(t)
	lg.StructuredJSON(true)
	lg.Info(map[string]interface{}{"legacy": "ok"})
	lg.Sync()

	line := readLastLineRawJSON(t, logPath)
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if m["legacy"] != "ok" {
		t.Errorf("legacy map path regression: %+v", m)
	}
}
