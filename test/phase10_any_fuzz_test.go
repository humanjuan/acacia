package acacia_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func FuzzPhase10AnyValidityRedactionAndSingleLineSafety(f *testing.F) {
	f.Add("payload", "hello", "password", false)
	f.Add("password", "secret", "nestedKey", true)
	f.Add("payload", "val\nwith\rnewline", "token", false)
	f.Add("payload", "invalid\xff\xfeutf8", "apikey", false)

	f.Fuzz(func(t *testing.T, key, value, nestedKey string, useObject bool) {
		tmpDir, err := os.MkdirTemp("", "acacia-any-fuzz-*")
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

		payload := map[string]interface{}{
			nestedKey: value,
			"nested": map[string]interface{}{
				nestedKey: value,
			},
			"items": []interface{}{
				map[string]interface{}{nestedKey: value},
			},
		}

		if useObject {
			logger.InfoEvent().Object("obj", func(o *acacia.Object) {
				o.Any(key, payload)
			}).Msg("msg")
		} else {
			logger.InfoEvent().Any(key, payload).Msg("msg")
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

		// 4. Sensitive key redaction checks
		isExternalSensitive := isSensitiveKeyAnyFuzz(key)
		isNestedSensitive := isSensitiveKeyAnyFuzz(nestedKey)

		if isExternalSensitive {
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
				if val != "[REDACTED]" && value != "" && value != "[REDACTED]" {
					t.Fatalf("sensitive external key %q was not redacted (got value %q): %s", key, val, line)
				}
			}
		}

		if isNestedSensitive && !isExternalSensitive {
			// If external was not redacted, check nested fields
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
				pay, ok := val.(map[string]interface{})
				if ok {
					if pay[nestedKey] != "[REDACTED]" && value != "" && value != "[REDACTED]" {
						t.Fatalf("nested key %q was not redacted inside map (got value %q): %s", nestedKey, pay[nestedKey], line)
					}
					nested, ok2 := pay["nested"].(map[string]interface{})
					if ok2 {
						if nested[nestedKey] != "[REDACTED]" && value != "" && value != "[REDACTED]" {
							t.Fatalf("nested key %q was not redacted inside nested map (got value %q): %s", nestedKey, nested[nestedKey], line)
						}
					}
					items, ok3 := pay["items"].([]interface{})
					if ok3 && len(items) > 0 {
						item, ok4 := items[0].(map[string]interface{})
						if ok4 {
							if item[nestedKey] != "[REDACTED]" && value != "" && value != "[REDACTED]" {
								t.Fatalf("nested key %q was not redacted inside slice item (got value %q): %s", nestedKey, item[nestedKey], line)
							}
						}
					}
				}
			}
		}
	})
}

func isSensitiveKeyAnyFuzz(key string) bool {
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
