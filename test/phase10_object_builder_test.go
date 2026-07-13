package acacia_test

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func setupObjectTestLogger(t *testing.T) (*acacia.Log, string) {
	tmpDir, err := os.MkdirTemp("", "acacia-object-test-*")
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

func readLastLineFromPath(t *testing.T, filepath string) string {
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

func TestPhase10ObjectBuilderNestedObjectValidJSON(t *testing.T) {
	lg, logPath := setupObjectTestLogger(t)
	lg.InfoEvent().
		Object("user", func(u *acacia.Object) {
			u.Str("id", "u_123").
				Object("profile", func(p *acacia.Object) {
					p.Str("country", "CL").
						Str("tier", "premium")
				})
		}).
		Msg("checkout completed")
	lg.Sync()

	line := readLastLineFromPath(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	user, ok := m["user"].(map[string]interface{})
	if !ok {
		t.Fatalf("user field missing or not object")
	}
	if user["id"] != "u_123" {
		t.Errorf("expected id u_123, got %v", user["id"])
	}

	profile, ok := user["profile"].(map[string]interface{})
	if !ok {
		t.Fatalf("profile field missing or not object")
	}
	if profile["country"] != "CL" || profile["tier"] != "premium" {
		t.Errorf("profile contents unexpected: %v", profile)
	}
}

func TestPhase10ObjectBuilderEmptyCallbackEmitsEmptyObject(t *testing.T) {
	lg, logPath := setupObjectTestLogger(t)
	lg.InfoEvent().
		Object("empty", func(o *acacia.Object) {}).
		Msg("ok")
	lg.Sync()

	line := readLastLineFromPath(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	empty, ok := m["empty"].(map[string]interface{})
	if !ok {
		t.Fatalf("empty field missing or not object")
	}
	if len(empty) != 0 {
		t.Errorf("expected empty object, got %v", empty)
	}
}

func TestPhase10ObjectBuilderNilCallbackEmitsEmptyObject(t *testing.T) {
	lg, logPath := setupObjectTestLogger(t)
	lg.InfoEvent().
		Object("nil_callback", nil).
		Msg("ok")
	lg.Sync()

	line := readLastLineFromPath(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	nilObj, ok := m["nil_callback"].(map[string]interface{})
	if !ok {
		t.Fatalf("nil_callback field missing or not object")
	}
	if len(nilObj) != 0 {
		t.Errorf("expected empty object, got %v", nilObj)
	}
}

func TestPhase10ObjectBuilderEscapesKeysAndValues(t *testing.T) {
	lg, logPath := setupObjectTestLogger(t)
	lg.InfoEvent().
		Object(`key"quote\slash`, func(o *acacia.Object) {
			o.Str(`k"v\n`, `v"al\r\n`)
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineFromPath(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	obj, ok := m[`key"quote\slash`].(map[string]interface{})
	if !ok {
		t.Fatalf("object key not found or invalid")
	}
	if obj[`k"v\n`] != `v"al\r\n` {
		t.Errorf("key/value escaping failed inside object: got %v", obj[`k"v\n`])
	}
}

func TestPhase10ObjectBuilderDuplicateKeysAreEmitted(t *testing.T) {
	lg, logPath := setupObjectTestLogger(t)
	lg.InfoEvent().
		Object("obj", func(o *acacia.Object) {
			o.Str("dup", "first").
				Str("dup", "second")
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineFromPath(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	// Verify both duplicate keys exist in the raw line
	if !strings.Contains(line, `"dup":"first"`) || !strings.Contains(line, `"dup":"second"`) {
		t.Errorf("expected duplicate keys in raw log, got: %s", line)
	}
}

func TestPhase10ObjectBuilderRecursiveSensitiveKeysAreRedacted(t *testing.T) {
	lg, logPath := setupObjectTestLogger(t)
	lg.InfoEvent().
		Object("user", func(u *acacia.Object) {
			u.Str("password", "leak_me").
				Str("AccessToken", "leak_token").
				Object("nested", func(n *acacia.Object) {
					n.Str("api-key", "nested_leak")
				})
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineFromPath(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "leak_me") || strings.Contains(line, "leak_token") || strings.Contains(line, "nested_leak") {
		t.Errorf("sensitive values leaked in raw log line: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	user := m["user"].(map[string]interface{})
	if user["password"] != "[REDACTED]" || user["AccessToken"] != "[REDACTED]" {
		t.Errorf("redaction failed at level 1: %v", user)
	}
	nested := user["nested"].(map[string]interface{})
	if nested["api-key"] != "[REDACTED]" {
		t.Errorf("redaction failed at level 2: %v", nested)
	}
}

func TestPhase10ObjectBuilderErrNilIsOmitted(t *testing.T) {
	lg, logPath := setupObjectTestLogger(t)
	lg.InfoEvent().
		Object("obj", func(o *acacia.Object) {
			o.Err(nil).
				ErrKey("cause", nil).
				Str("other", "ok")
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineFromPath(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	obj := m["obj"].(map[string]interface{})
	if _, ok := obj["error"]; ok {
		t.Errorf("error field written for nil error")
	}
	if _, ok := obj["cause"]; ok {
		t.Errorf("cause field written for nil error")
	}
	if obj["other"] != "ok" {
		t.Errorf("other field failed to write")
	}
}

func TestPhase10ObjectBuilderErrKeyEscapedAndRedacted(t *testing.T) {
	lg, logPath := setupObjectTestLogger(t)
	lg.InfoEvent().
		Object("obj", func(o *acacia.Object) {
			o.ErrKey("password", errors.New("secret_in_error")).
				ErrKey(`bad"key`, errors.New("err_msg"))
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineFromPath(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "secret_in_error") {
		t.Errorf("secret error value leaked: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	obj := m["obj"].(map[string]interface{})
	if obj["password"] != "[REDACTED]" {
		t.Errorf("sensitive error key not redacted: %v", obj)
	}
	if obj[`bad"key`] != "err_msg" {
		t.Errorf("escaped error key failed: %v", obj)
	}
}

func TestPhase10ObjectBuilderFloat64NonFiniteStillValidJSON(t *testing.T) {
	lg, logPath := setupObjectTestLogger(t)
	lg.InfoEvent().
		Object("obj", func(o *acacia.Object) {
			o.Float64("nan", math.NaN()).
				Float64("pos_inf", math.Inf(1)).
				Float64("neg_inf", math.Inf(-1)).
				Float64("normal", 3.14)
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineFromPath(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, ":NaN") || strings.Contains(line, ":+Inf") || strings.Contains(line, ":-Inf") {
		t.Errorf("raw line contains unquoted non-finite floats: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	obj := m["obj"].(map[string]interface{})
	if obj["nan"] != "NaN" || obj["pos_inf"] != "+Inf" || obj["neg_inf"] != "-Inf" {
		t.Errorf("non-finite values not stringified: %v", obj)
	}
	if obj["normal"].(float64) != 3.14 {
		t.Errorf("normal float got distorted: %v", obj["normal"])
	}
}

func TestPhase10ObjectBuilderCallbackPanicDoesNotCorruptJSON(t *testing.T) {
	lg, logPath := setupObjectTestLogger(t)
	lg.InfoEvent().
		Str("before", "ok").
		Object("panic_obj", func(o *acacia.Object) {
			o.Str("a", "b")
			panic("panic inside object")
		}).
		Str("after", "ok").
		Msg("msg")
	lg.Sync()

	line := readLastLineFromPath(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "panic_obj") || strings.Contains(line, `"a":"b"`) {
		t.Errorf("failed object details leaked in JSON after panic: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["before"] != "ok" || m["after"] != "ok" {
		t.Errorf("surrounding keys missing or modified: %v", m)
	}

	if m["_acacia_error"] != "object callback panic" {
		t.Errorf("panic indicator missing or incorrect: %v", m)
	}
}

func TestPhase10ObjectBuilderDeepNestingFiveLevels(t *testing.T) {
	lg, logPath := setupObjectTestLogger(t)
	lg.InfoEvent().
		Object("l1", func(o1 *acacia.Object) {
			o1.Object("l2", func(o2 *acacia.Object) {
				o2.Object("l3", func(o3 *acacia.Object) {
					o3.Object("l4", func(o4 *acacia.Object) {
						o4.Object("l5", func(o5 *acacia.Object) {
							o5.Str("val", "nested")
						})
					})
				})
			})
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineFromPath(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// Verify tree depth
	l1 := m["l1"].(map[string]interface{})
	l2 := l1["l2"].(map[string]interface{})
	l3 := l2["l3"].(map[string]interface{})
	l4 := l3["l4"].(map[string]interface{})
	l5 := l4["l5"].(map[string]interface{})
	if l5["val"] != "nested" {
		t.Errorf("expected nested value, got %v", l5["val"])
	}
}

func TestPhase10ObjectBuilderLegacyMapPathStillWorks(t *testing.T) {
	lg, logPath := setupObjectTestLogger(t)
	lg.StructuredJSON(true)
	lg.Info(map[string]interface{}{
		"legacy": "map",
		"child":  123,
	})
	lg.Sync()

	line := readLastLineFromPath(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["legacy"] != "map" {
		t.Errorf("legacy key not found")
	}
	if m["child"].(float64) != 123 {
		t.Errorf("child key not found or incorrect value: %v", m["child"])
	}
}

func TestPhase10ObjectBuilderZeroValueIsNoOp(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("zero-value Object methods panic'd: %v", r)
		}
	}()

	var o acacia.Object
	o.Str("k", "v")
	o.Int("n", 1)
	o.Int64("i64", 2)
	o.Uint64("u64", 3)
	o.Bool("b", true)
	o.Float64("f", math.NaN())
	o.Dur("d", 5*time.Second)
	o.Time("t", time.Now())
	o.Err(errors.New("x"))
	o.ErrKey("err", errors.New("y"))
	o.Object("child", nil)
	o.Secret("password", "secret")
	o.StrRedacted("token", "secret")
}

func TestPhase10ObjectBuilderNestedCallbackPanicAsFirstFieldDoesNotCorruptParent(t *testing.T) {
	lg, logPath := setupObjectTestLogger(t)
	lg.InfoEvent().
		Object("parent", func(p *acacia.Object) {
			p.Object("child", func(c *acacia.Object) {
				panic("oops inside first field child")
			})
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineFromPath(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "oops inside first field child") || strings.Contains(line, `"child"`) {
		t.Errorf("leaked internal panic message or child key: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	parent := m["parent"].(map[string]interface{})
	if parent["_acacia_error"] != "object callback panic" {
		t.Errorf("expected parent object to contain panic error, got %v", parent)
	}
	if len(parent) != 1 {
		t.Errorf("parent should only contain _acacia_error, got %v", parent)
	}
}

func TestPhase10ObjectBuilderNestedCallbackPanicAfterExistingFieldKeepsValidJSON(t *testing.T) {
	lg, logPath := setupObjectTestLogger(t)
	lg.InfoEvent().
		Object("parent", func(p *acacia.Object) {
			p.Str("first_field", "val").
				Object("child", func(c *acacia.Object) {
					panic("oops inside second field child")
				}).
				Str("third_field", "val3")
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineFromPath(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "oops inside second field child") || strings.Contains(line, `"child"`) {
		t.Errorf("leaked internal panic message or child key: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	parent := m["parent"].(map[string]interface{})
	if parent["first_field"] != "val" {
		t.Errorf("first field lost: %v", parent)
	}
	if parent["_acacia_error"] != "object callback panic" {
		t.Errorf("expected error field, got %v", parent)
	}
	if parent["third_field"] != "val3" {
		t.Errorf("third field lost: %v", parent)
	}
}
