package acacia_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func FuzzPhase10RawJSONValidityAndSingleLineSafety(f *testing.F) {
	f.Add("payload", `{"id":123,"ok":true}`, false)
	f.Add("password", `{"token":"secret"}`, true)
	f.Add("key\nwith\nnewline", "{\n  \"multiline\": true\n}", false)
	f.Add("payload", "invalid{json", false)
	f.Add("payload", "invalid\xff\xfeutf8", true)

	f.Fuzz(func(t *testing.T, key, raw string, useObject bool) {
		tmpDir, err := os.MkdirTemp("", "acacia-rawjson-fuzz-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		logPath := filepath.Join(tmpDir, "fuzz.log")
		logger, err := acacia.Start("fuzz.log", tmpDir, "info", acacia.WithBufferSize(1000), acacia.WithBackpressurePolicy(acacia.DropNewest))
		if err != nil {
			t.Fatalf("failed to start logger: %v", err)
		}
		defer logger.Close()

		if useObject {
			logger.InfoEvent().Object("obj", func(o *acacia.Object) {
				o.RawJSON(key, []byte(raw))
			}).Msg("msg")
		} else {
			logger.InfoEvent().RawJSON(key, []byte(raw)).Msg("msg")
		}

		logger.Sync()

		data, err := os.ReadFile(logPath)
		if err != nil {
			return
		}
		lines := strings.Split(string(data), "\n")
		if len(lines) == 0 || lines[0] == "" {
			return
		}
		line := lines[0]

		// 1. Valid JSON
		if !json.Valid([]byte(line)) {
			t.Fatalf("produced invalid JSON: %q", line)
		}

		// 2. Single line validation (no raw newline leaked)
		nonEmptyLines := 0
		for _, l := range lines {
			if strings.TrimSpace(l) != "" {
				nonEmptyLines++
			}
		}
		if nonEmptyLines > 1 {
			t.Fatalf("raw newline leaked: expected 1 log event line, got %d lines", nonEmptyLines)
		}

		// 3. No raw carriage return \r in the output line
		if strings.Contains(line, "\r") {
			t.Fatalf("raw carriage return found in log line: %q", line)
		}

		var m map[string]interface{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}

		// 4. Sensitive key redaction check
		isSensitive := isSensitiveKeyRawJSONFuzz(key)
		if isSensitive {
			// If useObject is true, key is nested inside "obj"
			var val interface{}
			var found bool
			if useObject {
				if obj, ok := m["obj"].(map[string]interface{}); ok {
					val, found = obj[key]
				}
			} else {
				val, found = m[key]
			}

			if found {
				if val != "[REDACTED]" && raw != "" && raw != "[REDACTED]" {
					t.Fatalf("sensitive key %q was not fully redacted (got value %q): %s", key, val, line)
				}
			}
		}
	})
}

func isSensitiveKeyRawJSONFuzz(key string) bool {
	var buf [64]byte
	w := 0
	for i := 0; i < len(key); i++ {
		c := key[i]
		if c == '-' || c == '_' || c == '.' {
			continue
		}
		if w >= 64 {
			return false
		}
		if c >= 'A' && c <= 'Z' {
			buf[w] = c + ('a' - 'A')
		} else {
			buf[w] = c
		}
		w++
	}
	normalized := buf[:w]
	switch string(normalized) {
	case "authorization", "cookie", "setcookie", "token", "accesstoken", "refreshtoken",
		"password", "passwd", "pwd", "apikey", "privatekey", "clientsecret", "secret",
		"bearer", "jwt", "session", "sessionid", "csrf", "xsrf", "xapikey":
		return true
	default:
		return false
	}
}
