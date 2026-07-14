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

	acacia "github.com/humanjuan/acacia/v2"
)

func setupAnyTestLogger(t *testing.T) (*acacia.Log, string) {
	tmpDir, err := os.MkdirTemp("", "acacia-any-test-*")
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

func readLastLineAny(t *testing.T, filepath string) string {
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

func TestPhase10AnyEventPrimitiveValuesValidJSON(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().
		Any("str", "hello").
		Any("int", 42).
		Any("bool", true).
		Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["str"] != "hello" || m["int"].(float64) != 42 || m["bool"] != true {
		t.Errorf("primitive values incorrect in: %s", line)
	}
}

func TestPhase10AnyEventNilValueEmitsNull(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().Any("nil_val", nil).Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["nil_val"] != nil {
		t.Errorf("expected null, got: %v", m["nil_val"])
	}
}

func TestPhase10AnyEventMapValueValidJSON(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().Any("payload", map[string]interface{}{
		"id": "u1",
		"ok": true,
	}).Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	pay := m["payload"].(map[string]interface{})
	if pay["id"] != "u1" || pay["ok"] != true {
		t.Errorf("incorrect map output: %+v", pay)
	}
}

type customStruct struct {
	Name string `json:"user_name"`
	Age  int    `json:"user_age"`
}

func TestPhase10AnyEventStructValueValidJSON(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().Any("payload", customStruct{Name: "Juan", Age: 30}).Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	pay := m["payload"].(map[string]interface{})
	if pay["user_name"] != "Juan" || pay["user_age"].(float64) != 30 {
		t.Errorf("incorrect struct output layout: %+v", pay)
	}
}

func TestPhase10AnyObjectValueValidJSON(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().
		Object("obj", func(o *acacia.Object) {
			o.Any("raw", map[string]interface{}{"nested": true})
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
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

func TestPhase10AnyObjectFirstFieldAndAfterExistingField(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().
		Object("obj1", func(o *acacia.Object) {
			o.Any("raw_first", "first").
				Str("after", "ok")
		}).
		Object("obj2", func(o *acacia.Object) {
			o.Str("before", "ok").
				Any("raw_after", "after")
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	obj1 := m["obj1"].(map[string]interface{})
	if obj1["raw_first"] != "first" || obj1["after"] != "ok" {
		t.Errorf("obj1 layout incorrect: %+v", obj1)
	}

	obj2 := m["obj2"].(map[string]interface{})
	if obj2["before"] != "ok" || obj2["raw_after"] != "after" {
		t.Errorf("obj2 layout incorrect: %+v", obj2)
	}
}

func TestPhase10AnySensitiveEventKeyRedactsEntirePayload(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().Any("password", map[string]interface{}{"nested": "secret"}).Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
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

func TestPhase10AnySensitiveObjectKeyRedactsEntirePayload(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().
		Object("obj", func(o *acacia.Object) {
			o.Any("client_secret", map[string]interface{}{"nested": "secret"})
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
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

func TestPhase10AnyBestEffortRedactsNestedMapKeys(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().Any("payload", map[string]interface{}{
		"username": "juan",
		"password": "secret_password",
		"nested": map[string]interface{}{
			"token": "secret_token",
		},
	}).Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "secret_password") || strings.Contains(line, "secret_token") {
		t.Errorf("nested secret values leaked: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	pay := m["payload"].(map[string]interface{})
	if pay["username"] != "juan" || pay["password"] != "[REDACTED]" {
		t.Errorf("failed to redact map level secret: %+v", pay)
	}

	nested := pay["nested"].(map[string]interface{})
	if nested["token"] != "[REDACTED]" {
		t.Errorf("failed to redact nested map secret: %+v", nested)
	}
}

type userCreds struct {
	User string `json:"user"`
	Pass string `json:"password"`
}

func TestPhase10AnyBestEffortRedactsNestedStructJSONKeys(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().Any("payload", userCreds{User: "juan", Pass: "secret123"}).Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "secret123") {
		t.Errorf("struct secret value leaked: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	pay := m["payload"].(map[string]interface{})
	if pay["user"] != "juan" || pay["password"] != "[REDACTED]" {
		t.Errorf("failed struct best-effort redaction: %+v", pay)
	}
}

func TestPhase10AnyBestEffortRedactsSensitiveKeysInsideSlices(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().Any("payload", []interface{}{
		map[string]interface{}{"ok": true},
		map[string]interface{}{"password": "secret"},
	}).Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "secret") {
		t.Errorf("slice secret value leaked: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	arr := m["payload"].([]interface{})
	item2 := arr[1].(map[string]interface{})
	if item2["password"] != "[REDACTED]" {
		t.Errorf("failed to redact sensitive key inside slice: %+v", item2)
	}
}

func TestPhase10AnyUnsupportedFunctionEmitsSafeError(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().Any("payload", func() {}).Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	pay := m["payload"].(map[string]interface{})
	if pay["_acacia_error"] != "any marshal error" {
		t.Errorf("expected safe marshal error sentinel, got: %+v", pay)
	}
}

func TestPhase10AnyUnsupportedChannelEmitsSafeError(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	ch := make(chan int)
	lg.InfoEvent().Any("payload", ch).Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	pay := m["payload"].(map[string]interface{})
	if pay["_acacia_error"] != "any marshal error" {
		t.Errorf("expected safe marshal error sentinel, got: %+v", pay)
	}
}

func TestPhase10AnyCyclicMapEmitsSafeError(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	m := map[string]interface{}{}
	m["self"] = m
	lg.InfoEvent().Any("payload", m).Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var unmarshaled map[string]interface{}
	if err := json.Unmarshal([]byte(line), &unmarshaled); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	pay := unmarshaled["payload"].(map[string]interface{})
	if pay["_acacia_error"] != "any marshal error" {
		t.Errorf("expected cyclic map to emit safe error sentinel, got: %+v", pay)
	}
}

func TestPhase10AnyNonFiniteFloatEmitsSafeError(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().
		Any("nan", math.NaN()).
		Any("pos_inf", math.Inf(1)).
		Any("neg_inf", math.Inf(-1)).
		Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, ":NaN") || strings.Contains(line, ":+Inf") || strings.Contains(line, ":-Inf") {
		t.Errorf("raw line contains unquoted non-finite values: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	for _, k := range []string{"nan", "pos_inf", "neg_inf"} {
		pay := m[k].(map[string]interface{})
		if pay["_acacia_error"] != "any marshal error" {
			t.Errorf("expected safe error sentinel for non-finite key %s, got: %+v", k, pay)
		}
	}
}

func TestPhase10AnyStringWithCRLFRemainsSingleLine(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().Any("payload", "hello\nworld\rtest").Msg("msg")
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

type arrayAny interface {
	Any(string, interface{}) *acacia.Array
}

type namespaceAny interface {
	Any(string, interface{}) *acacia.Namespace
}

func TestPhase10AnyDoesNotExposeMsgOrSendOnArrayOrNamespace(t *testing.T) {
	// Confirm Array does not expose Any
	if _, ok := interface{}(&acacia.Array{}).(arrayAny); ok {
		t.Fatal("Array must not expose Any in Sprint 10.6")
	}

	// Confirm Namespace does not expose Any
	if _, ok := interface{}(&acacia.Namespace{}).(namespaceAny); ok {
		t.Fatal("Namespace must not expose Any in Sprint 10.6")
	}
}

func TestPhase10AnyFlatBuilderStillWorks(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().Str("a", "b").Int("x", 42).Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if m["a"] != "b" || m["x"].(float64) != 42 {
		t.Errorf("flat builder regression: %+v", m)
	}
}

func TestPhase10AnyObjectBuilderStillWorks(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().
		Object("obj", func(o *acacia.Object) {
			o.Str("k", "v")
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	obj := m["obj"].(map[string]interface{})
	if obj["k"] != "v" {
		t.Errorf("object builder regression: %+v", obj)
	}
}

func TestPhase10AnyArrayBuilderStillWorks(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().
		Array("arr", func(a *acacia.Array) {
			a.Str("item")
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	arr := m["arr"].([]interface{})
	if len(arr) != 1 || arr[0] != "item" {
		t.Errorf("array builder regression: %+v", arr)
	}
}

func TestPhase10AnyGroupNamespaceStillWork(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().
		Group("grp", func(g *acacia.Object) {
			g.Str("k", "v")
		}).
		Namespace("ns", func(n *acacia.Namespace) {
			n.Str("k", "v")
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	grp := m["grp"].(map[string]interface{})
	if grp["k"] != "v" || m["ns.k"] != "v" {
		t.Errorf("group/namespace regression: %+v", m)
	}
}

func TestPhase10AnyRawJSONStillWorks(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.InfoEvent().RawJSON("raw", []byte(`{"k":"v"}`)).Msg("msg")
	lg.Sync()

	line := readLastLineAny(t, logPath)
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	raw := m["raw"].(map[string]interface{})
	if raw["k"] != "v" {
		t.Errorf("raw json regression: %+v", m)
	}
}

func TestPhase10AnyLegacyMapPathStillWorks(t *testing.T) {
	lg, logPath := setupAnyTestLogger(t)
	lg.StructuredJSON(true)
	lg.Info(map[string]interface{}{"legacy": "ok"})
	lg.Sync()

	line := readLastLineAny(t, logPath)
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if m["legacy"] != "ok" {
		t.Errorf("legacy map path regression: %+v", m)
	}
}
