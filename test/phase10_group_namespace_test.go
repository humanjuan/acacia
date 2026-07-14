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

func setupGroupNamespaceTestLogger(t *testing.T) (*acacia.Log, string) {
	tmpDir, err := os.MkdirTemp("", "acacia-group-ns-test-*")
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

func readLastLineGroupNS(t *testing.T, filepath string) string {
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

// --- Group Tests ---

func TestPhase10GroupEventEmitsNestedObject(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Group("http", func(g *acacia.Object) {
			g.Str("method", "GET")
			g.Int("status", 200)
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	httpObj, ok := m["http"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing or invalid 'http' object: %s", line)
	}

	if httpObj["method"] != "GET" || httpObj["status"].(float64) != 200 {
		t.Errorf("incorrect fields inside http object: %+v", httpObj)
	}
}

func TestPhase10GroupObjectEmitsNestedObject(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Object("request", func(o *acacia.Object) {
			o.Group("http", func(g *acacia.Object) {
				g.Str("method", "GET")
			})
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	reqObj, ok := m["request"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing request: %s", line)
	}

	httpObj, ok := reqObj["http"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing http under request: %s", line)
	}

	if httpObj["method"] != "GET" {
		t.Errorf("expected GET, got %+v", httpObj["method"])
	}
}

func TestPhase10GroupNilCallbackEmitsEmptyObject(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Group("empty", nil).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	emptyObj, ok := m["empty"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing empty group: %s", line)
	}

	if len(emptyObj) != 0 {
		t.Errorf("expected empty group, got %+v", emptyObj)
	}
}

func TestPhase10GroupCallbackPanicDoesNotLeakRawPanic(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Group("danger", func(g *acacia.Object) {
			g.Str("a", "b")
			panic("something went wrong and this contains credential secret123")
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "secret123") {
		t.Errorf("raw panic message leaked: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["_acacia_error"] != "object callback panic" {
		t.Errorf("expected panic indicator field, got: %s", line)
	}
}

func TestPhase10GroupObjectBuilderStillWorks(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Object("obj", func(o *acacia.Object) {
			o.Str("foo", "bar")
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	obj, ok := m["obj"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing object: %s", line)
	}
	if obj["foo"] != "bar" {
		t.Errorf("expected bar, got %v", obj["foo"])
	}
}

// --- Namespace Tests ---

func TestPhase10NamespaceEventEmitsFlatPrefixedKeys(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Namespace("http", func(n *acacia.Namespace) {
			n.Str("method", "GET").
				Int("status", 200).
				Bool("ok", true)
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["http.method"] != "GET" || m["http.status"].(float64) != 200 || m["http.ok"] != true {
		t.Errorf("fields not flattened correctly: %s", line)
	}
}

func TestPhase10NamespaceObjectEmitsFlatPrefixedKeys(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Object("request", func(o *acacia.Object) {
			o.Namespace("http", func(n *acacia.Namespace) {
				n.Str("method", "GET")
			})
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	req, ok := m["request"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing request: %s", line)
	}

	if req["http.method"] != "GET" {
		t.Errorf("prefixed field inside object was incorrect: %+v", req)
	}
}

func TestPhase10NamespaceNilCallbackIsNoOp(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Namespace("http", nil).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	for k := range m {
		if strings.HasPrefix(k, "http") {
			t.Errorf("unexpected namespace key in map: %s", k)
		}
	}
}

func TestPhase10NamespaceEmptyCallbackIsNoOp(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Namespace("http", func(n *acacia.Namespace) {}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	for k := range m {
		if strings.HasPrefix(k, "http") {
			t.Errorf("unexpected namespace key in map: %s", k)
		}
	}
}

func TestPhase10NamespaceEscapesPrefixAndKeys(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Namespace("http\nctx", func(n *acacia.Namespace) {
			n.Str("method\rname", "GET")
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	val, ok := m["http\nctx.method\rname"]
	if !ok {
		t.Fatalf("missing escaped prefixed key in: %s", line)
	}

	if val != "GET" {
		t.Errorf("incorrect value: %v", val)
	}
}

func TestPhase10NamespacePrefixEmptyUsesRawKey(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Namespace("", func(n *acacia.Namespace) {
			n.Str("key", "val")
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["key"] != "val" {
		t.Errorf("expected raw key with no prefix dot: %s", line)
	}
}

func TestPhase10NamespaceZeroValueIsNoOp(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("zero-value Namespace call panicked: %v", r)
		}
	}()

	var n acacia.Namespace
	n.Str("key", "val").
		Int("int", 1).
		Int64("int64", 2).
		Uint64("uint64", 3).
		Bool("bool", true).
		Float64("float", math.NaN()).
		Dur("dur", time.Second).
		Time("time", time.Now()).
		Err(errors.New("err")).
		ErrKey("cause", errors.New("err")).
		Secret("secret", "val").
		StrRedacted("redacted", "val")
}

func TestPhase10NamespaceSensitiveLeafKeyIsRedacted(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Namespace("auth", func(n *acacia.Namespace) {
			n.Str("token", "secret123")
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "secret123") {
		t.Errorf("sensitive value leaked: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["auth.token"] != "[REDACTED]" {
		t.Errorf("expected REDACTED, got: %s", line)
	}
}

func TestPhase10NamespaceSensitivePrefixIsRedacted(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Namespace("token", func(n *acacia.Namespace) {
			n.Str("value", "secret456")
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "secret456") {
		t.Errorf("sensitive value leaked: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["token.value"] != "[REDACTED]" {
		t.Errorf("expected REDACTED, got: %s", line)
	}
}

func TestPhase10NamespaceSensitiveCompoundKeyIsRedacted(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Namespace("client", func(n *acacia.Namespace) {
			n.Str("secret", "secret789")
		}).
		Namespace("api", func(n *acacia.Namespace) {
			n.Str("key", "secret111")
		}).
		Namespace("client_secret", func(n *acacia.Namespace) {
			n.Str("value", "secret222")
		}).
		Namespace("private", func(n *acacia.Namespace) {
			n.Str("key", "secret333")
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	for _, s := range []string{"secret789", "secret111", "secret222", "secret333"} {
		if strings.Contains(line, s) {
			t.Errorf("compound sensitive value leaked: %s", line)
		}
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["client.secret"] != "[REDACTED]" || m["api.key"] != "[REDACTED]" ||
		m["client_secret.value"] != "[REDACTED]" || m["private.key"] != "[REDACTED]" {
		t.Errorf("redaction of compound keys failed: %s", line)
	}
}

func TestPhase10NamespaceSecretAndStrRedacted(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Namespace("http", func(n *acacia.Namespace) {
			n.Secret("passwd", "sensitive").
				StrRedacted("cookie", "sensitive")
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "sensitive") {
		t.Errorf("explicit redacted values leaked: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["http.passwd"] != "[REDACTED]" || m["http.cookie"] != "[REDACTED]" {
		t.Errorf("redaction failed: %s", line)
	}
}

func TestPhase10NamespaceErrNilIsNoOp(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Namespace("http", func(n *acacia.Namespace) {
			n.Err(nil)
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	for k := range m {
		if strings.Contains(k, "error") {
			t.Errorf("unexpected error field: %s", k)
		}
	}
}

func TestPhase10NamespaceErrKeyEmitsPrefixedError(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Namespace("http", func(n *acacia.Namespace) {
			n.ErrKey("cause", errors.New("conn reset"))
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["http.cause"] != "conn reset" {
		t.Errorf("expected conn reset error, got: %v", m["http.cause"])
	}
}

func TestPhase10NamespaceFloat64NonFiniteStillValidJSON(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Namespace("http", func(n *acacia.Namespace) {
			n.Float64("nan", math.NaN()).
				Float64("pos_inf", math.Inf(1)).
				Float64("neg_inf", math.Inf(-1))
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
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

	if m["http.nan"] != "NaN" || m["http.pos_inf"] != "+Inf" || m["http.neg_inf"] != "-Inf" {
		t.Errorf("non-finite values were not converted to expected strings: got %s", line)
	}
}

func TestPhase10NamespaceEventCallbackPanicDoesNotCorruptJSON(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Namespace("http", func(n *acacia.Namespace) {
			n.Str("method", "GET")
			panic("panic event")
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if strings.Contains(line, "http.method") {
		t.Errorf("expected rolled back namespace, but got method field in: %s", line)
	}

	if m["_acacia_error"] != "namespace callback panic" {
		t.Errorf("expected _acacia_error field, got: %s", line)
	}
}

func TestPhase10NamespaceObjectCallbackPanicAsFirstFieldDoesNotCorruptParent(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Object("request", func(o *acacia.Object) {
			o.Namespace("http", func(n *acacia.Namespace) {
				n.Str("method", "GET")
				panic("panic object first")
			})
			o.Str("extra", "value")
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	req := m["request"].(map[string]interface{})
	if strings.Contains(line, "http.method") {
		t.Errorf("expected rolled back namespace, got method in: %s", line)
	}

	if req["_acacia_error"] != "namespace callback panic" {
		t.Errorf("expected _acacia_error in object, got: %+v", req)
	}

	if req["extra"] != "value" {
		t.Errorf("expected extra to survive, got: %+v", req)
	}
}

func TestPhase10NamespaceObjectCallbackPanicAfterExistingFieldKeepsValidJSON(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Object("request", func(o *acacia.Object) {
			o.Str("initial", "yes")
			o.Namespace("http", func(n *acacia.Namespace) {
				n.Str("method", "GET")
				panic("panic object after")
			})
			o.Str("extra", "value")
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	req := m["request"].(map[string]interface{})
	if strings.Contains(line, "http.method") {
		t.Errorf("expected rolled back namespace, got method in: %s", line)
	}

	if req["initial"] != "yes" || req["_acacia_error"] != "namespace callback panic" || req["extra"] != "value" {
		t.Errorf("expected parent object to be valid and contain error, got: %+v", req)
	}
}

// --- Regression Tests ---

func TestPhase10NamespaceFlatBuilderStillWorks(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Str("foo", "bar").
		Int("num", 10).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["foo"] != "bar" || m["num"].(float64) != 10 {
		t.Errorf("flat builder regression: %+v", m)
	}
}

func TestPhase10NamespaceObjectBuilderStillWorks(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Object("obj", func(o *acacia.Object) {
			o.Str("k", "v")
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	obj := m["obj"].(map[string]interface{})
	if obj["k"] != "v" {
		t.Errorf("object builder regression: %+v", obj)
	}
}

func TestPhase10NamespaceArrayBuilderStillWorks(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.InfoEvent().
		Array("arr", func(a *acacia.Array) {
			a.Str("item1").Int(2)
		}).
		Msg("ok")
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	arr := m["arr"].([]interface{})
	if len(arr) != 2 || arr[0] != "item1" || arr[1].(float64) != 2 {
		t.Errorf("array builder regression: %+v", arr)
	}
}

func TestPhase10NamespaceLegacyMapPathStillWorks(t *testing.T) {
	lg, logPath := setupGroupNamespaceTestLogger(t)
	lg.StructuredJSON(true)
	lg.Info(map[string]interface{}{
		"map_key": "map_val",
	})
	lg.Sync()

	line := readLastLineGroupNS(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["map_key"] != "map_val" {
		t.Errorf("legacy map path regression: %+v", m)
	}
}
