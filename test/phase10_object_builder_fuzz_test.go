package acacia_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func FuzzPhase10ObjectBuilderEscapingAndRedaction(f *testing.F) {
	// Seed inputs
	f.Add("user_key", "nested_key", "nested_val")
	f.Add("password", "nested_passwd", "secret")
	f.Add("key\nwith\nnewline", "inner\r\nkey", "inner\nval")
	f.Add(`key"quote`, `inner\slash`, `val"quote\n`)
	f.Add("invalid_\xff\xfe_utf8", "inner_\xff\xfe_utf8", "val_\xff\xfe_utf8")

	tmpDir, err := os.MkdirTemp("", "acacia-object-fuzz-*")
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

	f.Fuzz(func(t *testing.T, objKey string, innerKey string, innerVal string) {
		// Clean the log file
		os.WriteFile(logPath, []byte{}, 0644)

		normalizedInner := normalizeObjectKey(innerKey)
		isInnerSensitive := isSensitiveKeyString(normalizedInner)

		normalizedObj := normalizeObjectKey(objKey)
		isObjSensitive := isSensitiveKeyString(normalizedObj)

		valToLog := innerVal
		secretProbe := ""
		shouldRedact := isObjSensitive || isInnerSensitive
		if shouldRedact {
			secretProbe = "FUZZ_SECRET_MARKER_" + innerVal
			valToLog = secretProbe
		}

		event := logger.InfoEvent()
		if event == nil {
			return
		}

		event.Object(objKey, func(o *acacia.Object) {
			o.Str(innerKey, valToLog)
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
			t.Fatalf("produced invalid JSON for objKey=%q, innerKey=%q, innerVal=%q: %s", objKey, innerKey, innerVal, line)
		}

		// Verify that no raw carriage return or newline exists inside the JSON structure
		if strings.Contains(line, "\n") || strings.Contains(line, "\r") {
			t.Fatalf("raw CRLF detected inside log line: %q", line)
		}

		if shouldRedact {
			if strings.Contains(line, secretProbe) {
				t.Fatalf("sensitive value leaked: objKey=%q, innerKey=%q, secretProbe=%q, line=%s", objKey, innerKey, secretProbe, line)
			}
		}
	})
}

func normalizeObjectKey(key string) string {
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

func isSensitiveKeyString(normalized string) bool {
	switch normalized {
	case "authorization", "authorizationheader", "cookie", "setcookie", "token", "accesstoken", "refreshtoken",
		"password", "passwd", "pwd", "apikey", "privatekey", "clientsecret", "secret",
		"bearer", "jwt", "session", "sessionid", "csrf", "xsrf", "xapikey":
		return true
	default:
		return false
	}
}
