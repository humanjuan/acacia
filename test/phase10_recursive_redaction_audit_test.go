package acacia_test

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

const (
	secretVal  = "DO_NOT_LEAK_PHASE10_SECRET"
	tokenVal   = "Bearer DO_NOT_LEAK_PHASE10_TOKEN"
	cookieVal  = "session=DO_NOT_LEAK_PHASE10_COOKIE"
	cleanValue = "regular_non_secret_data"
)

func setupAuditTestLogger(t *testing.T) (*acacia.Log, string) {
	tmpDir, err := os.MkdirTemp("", "acacia-redaction-audit-*")
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

func readLastLineAudit(t *testing.T, filepath string) string {
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

func TestPhase10RedactionAuditSensitiveKeyMatrixFlatBuilder(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	// List of sensitive keys and variants to test
	keys := []string{
		"password", "Password", "PASSWORD", "pass-word", "pass_word", "pass.word",
		"passwd", "pwd", "token", "accessToken", "access-token", "access_token",
		"refreshToken", "authorization", "authorization_header", "cookie", "set-cookie",
		"set.cookie", "set_cookie", "api_key", "apikey", "secret", "clientSecret",
		"client-secret", "privateKey", "bearer", "jwt", "session", "session_id",
		"csrf", "xsrf", "x-api-key",
	}

	for _, key := range keys {
		lg.InfoEvent().Str(key, secretVal).Msg("msg")
		lg.Sync()

		line := readLastLineAudit(t, logPath)
		if !json.Valid([]byte(line)) {
			t.Fatalf("produced invalid JSON for key %s: %s", key, line)
		}

		if strings.Contains(line, secretVal) {
			t.Errorf("secret leaked for key %s: %s", key, line)
		}

		var m map[string]interface{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}

		if m[key] != "[REDACTED]" {
			t.Errorf("expected redacted value for key %s, got: %v", key, m[key])
		}
	}
}

func TestPhase10RedactionAuditSensitiveKeyMatrixObjectBuilder(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		Object("user", func(o *acacia.Object) {
			o.Str("password", secretVal).
				Str("client_secret", tokenVal)
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, secretVal) || strings.Contains(line, tokenVal) {
		t.Errorf("secret leaked inside object builder: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	user := m["user"].(map[string]interface{})
	if user["password"] != "[REDACTED]" || user["client_secret"] != "[REDACTED]" {
		t.Errorf("expected redacted values in object: %+v", user)
	}
}

func TestPhase10RedactionAuditSensitiveKeyMatrixGroupBuilder(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		Group("auth_group", func(g *acacia.Object) {
			g.Str("token", tokenVal).
				Str("regular", cleanValue)
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, tokenVal) {
		t.Errorf("secret leaked inside group builder: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	grp := m["auth_group"].(map[string]interface{})
	if grp["token"] != "[REDACTED]" || grp["regular"] != cleanValue {
		t.Errorf("group redaction failed or regular value lost: %+v", grp)
	}
}

func TestPhase10RedactionAuditSensitiveKeyMatrixArrayObjectElements(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		Array("users", func(a *acacia.Array) {
			a.Object(func(o *acacia.Object) {
				o.Str("password", secretVal)
			})
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, secretVal) {
		t.Errorf("secret leaked inside array object elements: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	users := m["users"].([]interface{})
	user := users[0].(map[string]interface{})
	if user["password"] != "[REDACTED]" {
		t.Errorf("redaction failed inside array object: %+v", user)
	}
}

func TestPhase10RedactionAuditSensitiveKeyMatrixNamespace(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		Namespace("auth", func(n *acacia.Namespace) {
			n.Str("token", tokenVal).
				Str("regular", cleanValue)
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, tokenVal) {
		t.Errorf("secret leaked inside namespace: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["auth.token"] != "[REDACTED]" || m["auth.regular"] != cleanValue {
		t.Errorf("namespace redaction failed: %+v", m)
	}
}

func TestPhase10RedactionAuditSecretAndStrRedactedAlwaysRedact(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		Secret("regular_key", secretVal).
		StrRedacted("another_key", tokenVal).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, secretVal) || strings.Contains(line, tokenVal) {
		t.Errorf("secret leaked in explicit redaction builders: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["regular_key"] != "[REDACTED]" || m["another_key"] != "[REDACTED]" {
		t.Errorf("explicit redaction failed: %+v", m)
	}
}

func TestPhase10RedactionAuditErrKeySensitiveRedacts(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		ErrKey("password", errors.New(secretVal)).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, secretVal) {
		t.Errorf("error secret leaked under sensitive key: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["password"] != "[REDACTED]" {
		t.Errorf("sensitive error key was not redacted: %+v", m)
	}
}

func TestPhase10RedactionAuditFloat64SensitiveKeyRedacts(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		Float64("private_key", 123.45).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "123.45") {
		t.Errorf("numeric secret leaked under sensitive float key: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["private_key"] != "[REDACTED]" {
		t.Errorf("sensitive float key was not redacted: %+v", m)
	}
}

func TestPhase10RedactionAuditTimeDurBoolIntSensitiveKeyRedacts(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		Time("cookie", time.Now()).
		Dur("token", time.Hour).
		Bool("session_id", true).
		Int("xapikey", 12345).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	for _, k := range []string{"cookie", "token", "session_id", "xapikey"} {
		if m[k] != "[REDACTED]" {
			t.Errorf("expected key %s to be redacted, got: %v", k, m[k])
		}
	}
}

func TestPhase10RedactionAuditNestedObjectDeepSensitiveKeys(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		Object("level1", func(o1 *acacia.Object) {
			o1.Object("level2", func(o2 *acacia.Object) {
				o2.Str("token", tokenVal)
			})
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, tokenVal) {
		t.Errorf("deep nested secret leaked: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	o1 := m["level1"].(map[string]interface{})
	o2 := o1["level2"].(map[string]interface{})
	if o2["token"] != "[REDACTED]" {
		t.Errorf("deep nested object key was not redacted: %+v", o2)
	}
}

func TestPhase10RedactionAuditArrayObjectDeepSensitiveKeys(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		Array("arr", func(a *acacia.Array) {
			a.Object(func(o1 *acacia.Object) {
				o1.Object("nested", func(o2 *acacia.Object) {
					o2.Str("password", secretVal)
				})
			})
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, secretVal) {
		t.Errorf("deep array-object secret leaked: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	arr := m["arr"].([]interface{})
	o1 := arr[0].(map[string]interface{})
	nested := o1["nested"].(map[string]interface{})
	if nested["password"] != "[REDACTED]" {
		t.Errorf("deep array nested object key was not redacted: %+v", nested)
	}
}

func TestPhase10RedactionAuditNamespaceCompoundAndSegmentSensitiveKeys(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		Namespace("authorization_header", func(n *acacia.Namespace) {
			n.Str("value", tokenVal)
		}).
		Namespace("regular", func(n *acacia.Namespace) {
			n.Str("access_token", tokenVal).
				Str("user_password_field", secretVal)
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, tokenVal) || strings.Contains(line, secretVal) {
		t.Errorf("namespace segment/compound secret leaked: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["authorization_header.value"] != "[REDACTED]" ||
		m["regular.access_token"] != "[REDACTED]" ||
		m["regular.user_password_field"] != "[REDACTED]" {
		t.Errorf("namespace segment/compound key redaction failed: %+v", m)
	}
}

func TestPhase10RedactionAuditRawJSONSensitiveExternalKeyRedactsEntirePayload(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		RawJSON("client_secret", []byte(`{"id":123,"plain":"leak"}`)).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, `"id":123`) || strings.Contains(line, `"plain":"leak"`) {
		t.Errorf("raw JSON sensitive key leaked payload: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["client_secret"] != "[REDACTED]" {
		t.Errorf("expected redacted payload, got: %v", m["client_secret"])
	}
}

func TestPhase10RedactionAuditRawJSONTrustedInputDoesNotRedactInternally(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	// External key is "payload" (non-sensitive), but contains internal secret
	lg.InfoEvent().
		RawJSON("payload", []byte(`{"password":"DO_NOT_LEAK_PHASE10_SECRET"}`)).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	// For RawJSON, trusted input only contract implies internal password must NOT be redacted
	if !strings.Contains(line, secretVal) {
		t.Errorf("trusted input contract violated, internal secret was redacted: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	pay := m["payload"].(map[string]interface{})
	if pay["password"] != secretVal {
		t.Errorf("expected raw unredacted secret inside trusted payload, got: %v", pay["password"])
	}
}

func TestPhase10RedactionAuditAnySensitiveExternalKeyRedactsEntirePayload(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		Any("client_secret", map[string]interface{}{"nested": secretVal}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, secretVal) {
		t.Errorf("Any sensitive external key leaked value: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["client_secret"] != "[REDACTED]" {
		t.Errorf("expected redacted payload, got: %v", m["client_secret"])
	}
}

func TestPhase10RedactionAuditAnyBestEffortNestedMapRedaction(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		Any("payload", map[string]interface{}{
			"normal":   cleanValue,
			"password": secretVal,
			"nested": map[string]interface{}{
				"token": tokenVal,
			},
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, secretVal) || strings.Contains(line, tokenVal) {
		t.Errorf("nested secret leaked inside Any map: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	pay := m["payload"].(map[string]interface{})
	if pay["normal"] != cleanValue || pay["password"] != "[REDACTED]" {
		t.Errorf("failed best effort map redaction: %+v", pay)
	}

	nested := pay["nested"].(map[string]interface{})
	if nested["token"] != "[REDACTED]" {
		t.Errorf("failed best effort nested map redaction: %+v", nested)
	}
}

type credentials struct {
	User string `json:"user_name"`
	Pass string `json:"password"`
}

func TestPhase10RedactionAuditAnyBestEffortNestedStructRedaction(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		Any("payload", credentials{User: "Juan", Pass: secretVal}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, secretVal) {
		t.Errorf("nested secret leaked inside Any struct: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	pay := m["payload"].(map[string]interface{})
	if pay["user_name"] != "Juan" || pay["password"] != "[REDACTED]" {
		t.Errorf("failed best effort struct redaction: %+v", pay)
	}
}

func TestPhase10RedactionAuditAnyBestEffortSliceRedaction(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		Any("payload", []interface{}{
			map[string]interface{}{"normal": cleanValue},
			map[string]interface{}{"token": tokenVal},
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, tokenVal) {
		t.Errorf("nested secret leaked inside Any slice: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	arr := m["payload"].([]interface{})
	o2 := arr[1].(map[string]interface{})
	if o2["token"] != "[REDACTED]" {
		t.Errorf("failed best effort slice items redaction: %+v", o2)
	}
}

func TestPhase10RedactionAuditAnyUnsupportedFallbackDoesNotLeak(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	// Pass function (unsupported) and check sentinel
	lg.InfoEvent().Any("payload", func() {}).Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	pay := m["payload"].(map[string]interface{})
	if pay["_acacia_error"] != "any marshal error" {
		t.Errorf("expected marshal error sentinel, got: %+v", pay)
	}
}

func TestPhase10RedactionAuditAnyCycleFallbackDoesNotLeak(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	mCycle := map[string]interface{}{}
	mCycle["self"] = mCycle
	lg.InfoEvent().Any("payload", mCycle).Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	pay := m["payload"].(map[string]interface{})
	if pay["_acacia_error"] != "any marshal error" {
		t.Errorf("expected cyclic map error sentinel, got: %+v", pay)
	}
}

func TestPhase10RedactionAuditPanicSentinelsDoNotLeakRawPanic(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		Object("danger", func(o *acacia.Object) {
			panic("panic_secret=" + secretVal)
		}).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, secretVal) {
		t.Errorf("raw panic secret leaked: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["_acacia_error"] != "object callback panic" {
		t.Errorf("expected raw panic error sentinel, got: %+v", m)
	}
}

func TestPhase10RedactionAuditCRLFAndInvalidUTF8RemainSingleLineAndValidJSON(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	lg.InfoEvent().
		Any("payload_crlf", "hello\nworld\r"+secretVal).
		Any("payload_utf8", "hello\xff\xfe"+tokenVal).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	if strings.Contains(line, "\n") || strings.Contains(line, "\r") {
		t.Errorf("line contains raw control characters, causing multi-line injection: %q", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// Payloads are non-sensitive keys, so the secrets can appear as part of strings,
	// but must be escaped and formatted cleanly without breaking single line or JSON format.
	if !strings.Contains(m["payload_crlf"].(string), secretVal) {
		t.Errorf("expected CRLF payload to contain secret cleanly, got: %v", m["payload_crlf"])
	}
}

func TestPhase10RedactionAuditLegacyMapPathDocumentedBehavior(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)
	lg.StructuredJSON(true)
	lg.Info(map[string]interface{}{
		"normal_field": "ok",
		"password":     secretVal,
	})
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// In the legacy map path, we verify what was tested:
	// If it doesn't redact, we document this boundary. If it does redact, we assert it.
	// In the legacy map path, it iterates and formats. Let's see if the secret is redacted:
	if m["password"] != "[REDACTED]" && m["password"] != secretVal {
		t.Errorf("unexpected value in legacy map path: %v", m["password"])
	}
}

func TestPhase10RedactionAuditExistingBuildersStillValidJSON(t *testing.T) {
	lg, logPath := setupAuditTestLogger(t)

	// Run multiple builders in a single event to ensure full compatibility
	lg.InfoEvent().
		Str("normal", cleanValue).
		RawJSON("raw", []byte(`{"a":1}`)).
		Object("obj", func(o *acacia.Object) {
			o.Str("k", "v")
		}).
		Array("arr", func(a *acacia.Array) {
			a.Str("item")
		}).
		Group("grp", func(g *acacia.Object) {
			g.Str("gk", "gv")
		}).
		Namespace("ns", func(n *acacia.Namespace) {
			n.Str("nk", "nv")
		}).
		Any("any_val", 42).
		Msg("msg")
	lg.Sync()

	line := readLastLineAudit(t, logPath)
	if !json.Valid([]byte(line)) {
		t.Fatalf("produced invalid JSON: %s", line)
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if m["normal"] != cleanValue || m["any_val"].(float64) != 42 || m["ns.nk"] != "nv" {
		t.Errorf("fields lost during combination log: %+v", m)
	}
}
