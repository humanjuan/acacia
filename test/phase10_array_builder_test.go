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

func setupArrayTestLogger(t *testing.T) (*acacia.Log, string) {
	tmpDir, err := os.MkdirTemp("", "acacia-array-test-*")
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

func readLastLineArray(t *testing.T, filepath string) string {
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

func TestPhase10ArrayBuilderPrimitiveArrayValidJSON(t *testing.T) {
	lg, logPath := setupArrayTestLogger(t)
	lg.InfoEvent().
		Array("items", func(a *acacia.Array) {
			a.Str("sku-1").
				Int(42).
				Bool(true)
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineArray(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	items, ok := m["items"].([]interface{})
	if !ok {
		t.Fatalf("expected items to be array")
	}
	if len(items) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(items))
	}
	if items[0] != "sku-1" || items[1].(float64) != 42 || items[2] != true {
		t.Errorf("unexpected array contents: %v", items)
	}
}

func TestPhase10ArrayBuilderEmptyCallbackEmitsEmptyArray(t *testing.T) {
	lg, logPath := setupArrayTestLogger(t)
	lg.InfoEvent().
		Array("empty", func(a *acacia.Array) {}).
		Msg("ok")
	lg.Sync()

	line := readLastLineArray(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	empty, ok := m["empty"].([]interface{})
	if !ok {
		t.Fatalf("expected empty array")
	}
	if len(empty) != 0 {
		t.Errorf("expected empty array, got %v", empty)
	}
}

func TestPhase10ArrayBuilderNilCallbackEmitsEmptyArray(t *testing.T) {
	lg, logPath := setupArrayTestLogger(t)
	lg.InfoEvent().
		Array("nil_callback", nil).
		Msg("ok")
	lg.Sync()

	line := readLastLineArray(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	arr, ok := m["nil_callback"].([]interface{})
	if !ok {
		t.Fatalf("expected array")
	}
	if len(arr) != 0 {
		t.Errorf("expected empty array, got %v", arr)
	}
}

func TestPhase10ArrayBuilderArrayInsideObjectValidJSON(t *testing.T) {
	lg, logPath := setupArrayTestLogger(t)
	lg.InfoEvent().
		Object("cart", func(o *acacia.Object) {
			o.Array("items", func(a *acacia.Array) {
				a.Str("sku-1").
					Str("sku-2")
			})
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineArray(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	cart := m["cart"].(map[string]interface{})
	items := cart["items"].([]interface{})
	if len(items) != 2 || items[0] != "sku-1" || items[1] != "sku-2" {
		t.Errorf("unexpected items inside cart: %v", items)
	}
}

func TestPhase10ArrayBuilderObjectElementsValidJSON(t *testing.T) {
	lg, logPath := setupArrayTestLogger(t)
	lg.InfoEvent().
		Array("items", func(a *acacia.Array) {
			a.Object(func(o *acacia.Object) {
				o.Str("sku", "ABC").
					Int("qty", 2)
			}).
				Object(func(o *acacia.Object) {
					o.Str("sku", "DEF").
						Int("qty", 1)
				})
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineArray(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	items := m["items"].([]interface{})
	if len(items) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(items))
	}

	item1 := items[0].(map[string]interface{})
	if item1["sku"] != "ABC" || item1["qty"].(float64) != 2 {
		t.Errorf("item 1 values unexpected: %v", item1)
	}

	item2 := items[1].(map[string]interface{})
	if item2["sku"] != "DEF" || item2["qty"].(float64) != 1 {
		t.Errorf("item 2 values unexpected: %v", item2)
	}
}

func TestPhase10ArrayBuilderEscapesStringValues(t *testing.T) {
	lg, logPath := setupArrayTestLogger(t)
	lg.InfoEvent().
		Array("escaped", func(a *acacia.Array) {
			a.Str("val\"with\nquotes\\slash")
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineArray(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	arr := m["escaped"].([]interface{})
	if arr[0] != "val\"with\nquotes\\slash" {
		t.Errorf("escaping failed inside array: got %v", arr[0])
	}
}

func TestPhase10ArrayBuilderFloat64NonFiniteStillValidJSON(t *testing.T) {
	lg, logPath := setupArrayTestLogger(t)
	lg.InfoEvent().
		Array("floats", func(a *acacia.Array) {
			a.Float64(math.NaN()).
				Float64(math.Inf(1)).
				Float64(math.Inf(-1)).
				Float64(3.14)
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineArray(t, logPath)
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

	arr := m["floats"].([]interface{})
	if arr[0] != "NaN" || arr[1] != "+Inf" || arr[2] != "-Inf" || arr[3].(float64) != 3.14 {
		t.Errorf("unexpected floats array contents: %v", arr)
	}
}

func TestPhase10ArrayBuilderErrNilIsNoOp(t *testing.T) {
	lg, logPath := setupArrayTestLogger(t)
	lg.InfoEvent().
		Array("errors", func(a *acacia.Array) {
			a.Err(nil).
				Str("other")
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineArray(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	errors := m["errors"].([]interface{})
	if len(errors) != 1 || errors[0] != "other" {
		t.Errorf("nil error not ignored correctly: %v", errors)
	}
}

func TestPhase10ArrayBuilderErrValueIsString(t *testing.T) {
	lg, logPath := setupArrayTestLogger(t)
	lg.InfoEvent().
		Array("errors", func(a *acacia.Array) {
			a.Err(errors.New("db connection failed"))
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineArray(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	errors := m["errors"].([]interface{})
	if len(errors) != 1 || errors[0] != "db connection failed" {
		t.Errorf("error not emitted as string inside array: %v", errors)
	}
}

func TestPhase10ArrayBuilderObjectElementRedactsSensitiveKeys(t *testing.T) {
	lg, logPath := setupArrayTestLogger(t)
	lg.InfoEvent().
		Array("items", func(a *acacia.Array) {
			a.Object(func(o *acacia.Object) {
				o.Str("password", "highly_secret").
					Str("token", "secret_token")
			})
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineArray(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "highly_secret") || strings.Contains(line, "secret_token") {
		t.Errorf("sensitive credentials leaked inside array element: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	items := m["items"].([]interface{})
	item := items[0].(map[string]interface{})
	if item["password"] != "[REDACTED]" || item["token"] != "[REDACTED]" {
		t.Errorf("redaction failed inside array object: %v", item)
	}
}

func TestPhase10ArrayBuilderEventArrayCallbackPanicDoesNotCorruptJSON(t *testing.T) {
	lg, logPath := setupArrayTestLogger(t)
	lg.InfoEvent().
		Str("before", "ok").
		Array("panic_arr", func(a *acacia.Array) {
			a.Str("first_item")
			panic("panic inside event array callback")
		}).
		Str("after", "ok").
		Msg("msg")
	lg.Sync()

	line := readLastLineArray(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "panic_arr") || strings.Contains(line, "first_item") {
		t.Errorf("details of panicky array leaked: %s", line)
	}

	if strings.Contains(line, "panic inside event array callback") {
		t.Errorf("raw panic message leaked in log: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["before"] != "ok" || m["after"] != "ok" {
		t.Errorf("surrounding fields affected: %v", m)
	}
	if m["_acacia_error"] != "array callback panic" {
		t.Errorf("expected error field in parent, got %v", m)
	}
}

func TestPhase10ArrayBuilderObjectArrayCallbackPanicAsFirstFieldDoesNotCorruptParent(t *testing.T) {
	lg, logPath := setupArrayTestLogger(t)
	lg.InfoEvent().
		Object("parent", func(p *acacia.Object) {
			p.Array("panic_arr", func(a *acacia.Array) {
				a.Int(123)
				panic("oops inside first field array")
			})
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineArray(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "panic_arr") || strings.Contains(line, "123") {
		t.Errorf("panicky array details leaked: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	parent := m["parent"].(map[string]interface{})
	if parent["_acacia_error"] != "array callback panic" {
		t.Errorf("expected array callback panic field inside parent, got %v", parent)
	}
	if len(parent) != 1 {
		t.Errorf("parent should only contain _acacia_error, got %v", parent)
	}
}

func TestPhase10ArrayBuilderObjectArrayCallbackPanicAfterExistingFieldKeepsValidJSON(t *testing.T) {
	lg, logPath := setupArrayTestLogger(t)
	lg.InfoEvent().
		Object("parent", func(p *acacia.Object) {
			p.Str("first_field", "val").
				Array("panic_arr", func(a *acacia.Array) {
					a.Str("first_item")
					panic("oops inside second field array")
				}).
				Str("third_field", "val3")
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineArray(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "panic_arr") || strings.Contains(line, "first_item") {
		t.Errorf("panicky array details leaked: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	parent := m["parent"].(map[string]interface{})
	if parent["first_field"] != "val" || parent["_acacia_error"] != "array callback panic" || parent["third_field"] != "val3" {
		t.Errorf("parent details incorrect after rollback: %v", parent)
	}
}

func TestPhase10ArrayBuilderObjectElementPanicDoesNotLeakRawPanic(t *testing.T) {
	lg, logPath := setupArrayTestLogger(t)
	lg.InfoEvent().
		Array("items", func(a *acacia.Array) {
			a.Str("first").
				Object(func(o *acacia.Object) {
					o.Str("partial", "x")
					panic("secret panic inside element")
				}).
				Str("last")
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineArray(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "secret panic inside element") || strings.Contains(line, "partial") {
		t.Errorf("details of panicky object element leaked: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	items := m["items"].([]interface{})
	if len(items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(items))
	}
	if items[0] != "first" || items[2] != "last" {
		t.Errorf("elements before/after panic element lost: %v", items)
	}

	itemObj := items[1].(map[string]interface{})
	if itemObj["_acacia_error"] != "object callback panic" {
		t.Errorf("unexpected panic indicator: %v", itemObj)
	}
}

func TestPhase10ArrayBuilderZeroValueIsNoOp(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("zero-value Array methods panic'd: %v", r)
		}
	}()

	var a acacia.Array
	a.Str("x")
	a.Int(1)
	a.Int64(2)
	a.Uint64(3)
	a.Bool(true)
	a.Float64(math.NaN())
	a.Dur(5 * time.Second)
	a.Time(time.Now())
	a.Err(errors.New("x"))
	a.Object(nil)
}

func TestPhase10ArrayBuilderObjectBuilderStillWorks(t *testing.T) {
	lg, logPath := setupArrayTestLogger(t)
	lg.InfoEvent().
		Object("obj", func(o *acacia.Object) {
			o.Str("k", "v").
				Object("inner", func(inner *acacia.Object) {
					inner.Int("qty", 10)
				})
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineArray(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	obj := m["obj"].(map[string]interface{})
	inner := obj["inner"].(map[string]interface{})
	if obj["k"] != "v" || inner["qty"].(float64) != 10 {
		t.Errorf("object builder failed: %v", m)
	}
}

func TestPhase10ArrayBuilderLegacyMapPathStillWorks(t *testing.T) {
	lg, logPath := setupArrayTestLogger(t)
	lg.StructuredJSON(true)
	lg.Info(map[string]interface{}{
		"legacy": "map",
		"child":  123,
	})
	lg.Sync()

	line := readLastLineArray(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["legacy"] != "map" || m["child"].(float64) != 123 {
		t.Errorf("legacy map path failed: %v", m)
	}
}
