package acacia_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func FuzzPhase10ArrayBuilderEscapingAndObjectRedaction(f *testing.F) {
	// Seed inputs
	f.Add("items_key", "string_item", "object_key", "object_value")
	f.Add("password", "secret", "token", "my_token")
	f.Add("control\nchars", "val\r\ncrfl", "inner\"quote", "inner\\slash")
	f.Add("invalid_\xff\xfe_utf8", "val_\xff\xfe_utf8", "key_\xff\xfe_utf8", "val2_\xff\xfe_utf8")

	tmpDir, err := os.MkdirTemp("", "acacia-array-fuzz-*")
	if err != nil {
		return
	}
	defer os.RemoveAll(tmpDir)

	logPath := filepath.Join(tmpDir, "fuzz.log")
	logger, err := acacia.Start("fuzz.log", tmpDir, "info", acacia.WithBufferSize(1000), acacia.WithBackpressurePolicy(acacia.DropNewest))
	if err != nil {
		return
	}
	defer logger.Close()

	f.Fuzz(func(t *testing.T, arrKey string, strVal string, objKey string, objVal string) {
		os.WriteFile(logPath, []byte{}, 0644)

		event := logger.InfoEvent()
		if event == nil {
			return
		}

		event.Array(arrKey, func(a *acacia.Array) {
			a.Str(strVal).
				Object(func(o *acacia.Object) {
					o.Str(objKey, objVal)
				})
		}).Msg("fuzz")
		logger.Sync()

		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("failed to read log: %v", err)
		}

		line := strings.TrimSpace(string(data))
		if len(line) == 0 {
			return
		}

		// Verify syntax correctness
		if !json.Valid([]byte(line)) {
			t.Fatalf("produced invalid JSON for arrKey=%q, strVal=%q, objKey=%q, objVal=%q: %s", arrKey, strVal, objKey, objVal, line)
		}

		// Verify that no raw carriage return or newline exists inside the JSON structure
		if strings.Contains(line, "\n") || strings.Contains(line, "\r") {
			t.Fatalf("raw CRLF detected inside log line: %q", line)
		}

		var m map[string]interface{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("produced invalid JSON: %v", err)
		}

		// Verify redaction inside object elements
		normalizedObjKey := normalizeArrayKey(objKey)
		isObjKeySensitive := isSensitiveKeyStringArray(normalizedObjKey)

		normalizedArrKey := normalizeArrayKey(arrKey)
		isArrKeySensitive := isSensitiveKeyStringArray(normalizedArrKey)

		if isArrKeySensitive {
			found := false
			for k, val := range m {
				if normalizeArrayKey(k) == normalizedArrKey {
					found = true
					if val != "[REDACTED]" {
						t.Fatalf("sensitive array key %q was not redacted: got %v", k, val)
					}
				}
			}
			_ = found
		}

		if isObjKeySensitive {
			checkSensitiveKeys(t, m, normalizedObjKey, objVal)
		}
	})
}

func checkSensitiveKeys(t *testing.T, data interface{}, targetNormalizedKey string, secretVal string) {
	if len(secretVal) == 0 || secretVal == "[REDACTED]" {
		return
	}
	switch v := data.(type) {
	case map[string]interface{}:
		for key, val := range v {
			if normalizeArrayKey(key) == targetNormalizedKey {
				if s, ok := val.(string); ok && s == secretVal {
					t.Fatalf("sensitive value leaked under key %q: %q", key, val)
				}
			}
			checkSensitiveKeys(t, val, targetNormalizedKey, secretVal)
		}
	case []interface{}:
		for _, val := range v {
			checkSensitiveKeys(t, val, targetNormalizedKey, secretVal)
		}
	}
}

func normalizeArrayKey(key string) string {
	var buf [64]byte
	w := 0
	for i := 0; i < len(key); i++ {
		c := key[i]
		if c == '-' || c == '_' || c == '.' {
			continue
		}
		if w >= 64 {
			return ""
		}
		if c >= 'A' && c <= 'Z' {
			buf[w] = c + ('a' - 'A')
		} else {
			buf[w] = c
		}
		w++
	}
	return string(buf[:w])
}

func isSensitiveKeyStringArray(normalized string) bool {
	switch normalized {
	case "authorization", "cookie", "setcookie", "token", "accesstoken", "refreshtoken",
		"password", "passwd", "pwd", "apikey", "privatekey", "clientsecret", "secret",
		"bearer", "jwt", "session", "sessionid", "csrf", "xsrf", "xapikey":
		return true
	default:
		return false
	}
}
