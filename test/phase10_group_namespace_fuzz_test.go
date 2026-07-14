package acacia_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func FuzzPhase10NamespaceEscapingAndRedaction(f *testing.F) {
	f.Add("http", "method", "GET", false)
	f.Add("auth", "token", "supersecret", false)
	f.Add("prefix\nwith\nnewlines", "key\rwith\rreturns", "val\"ue\\with\\escapes", false)
	f.Add("invalid\xff\xfeutf8", "invalid\xff\xfeutf8key", "invalid\xff\xfeutf8val", true)
	f.Add("client_secret", "value", "secret123", true)

	f.Fuzz(func(t *testing.T, prefix, key, value string, shouldPanic bool) {
		tmpDir, err := os.MkdirTemp("", "acacia-fuzz-*")
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

		func() {
			logger.InfoEvent().Namespace(prefix, func(n *acacia.Namespace) {
				n.Str(key, value)
				if shouldPanic {
					panic("fuzz_panic_credential_secret_999")
				}
			}).Msg("msg")
		}()

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

		if !json.Valid([]byte(line)) {
			t.Fatalf("produced invalid JSON: %q", line)
		}

		nonEmptyLines := 0
		for _, l := range lines {
			if strings.TrimSpace(l) != "" {
				nonEmptyLines++
			}
		}
		if nonEmptyLines > 1 {
			t.Fatalf("raw newline leaked, multiple lines produced: %d lines", nonEmptyLines)
		}

		if strings.Contains(line, "\r") {
			t.Fatalf("raw carriage return found in log line: %q", line)
		}

		if strings.Contains(line, "fuzz_panic_credential_secret_999") {
			t.Fatalf("raw panic message leaked: %s", line)
		}

		var m map[string]interface{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}

		if !shouldPanic {
			isSensitive := isSensitiveNamespaceKeyForFuzzTest(prefix, key)
			if isSensitive {
				var fullKey string
				if prefix == "" {
					fullKey = key
				} else if key == "" {
					fullKey = prefix + "."
				} else {
					fullKey = prefix + "." + key
				}

				if val, ok := m[fullKey]; ok {
					if val != "[REDACTED]" && value != "" && value != "[REDACTED]" {
						t.Fatalf("sensitive key %q was not redacted (got value %q): %s", fullKey, val, line)
					}
				}
			}
		}
	})
}

func isSensitiveNamespaceKeyForFuzzTest(prefix, key string) bool {
	if isSensitiveKeyFuzz(prefix) || isSensitiveKeyFuzz(key) {
		return true
	}
	if prefix != "" && key != "" {
		if isSensitiveKeyFuzz(prefix + "." + key) {
			return true
		}
	}
	if hasSensitiveSegmentFuzz(prefix) || hasSensitiveSegmentFuzz(key) {
		return true
	}
	return false
}

func hasSensitiveSegmentFuzz(s string) bool {
	start := -1
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '.' || c == '_' || c == '-' {
			if start != -1 {
				if isSensitiveKeyFuzz(s[start:i]) {
					return true
				}
				start = -1
			}
		} else {
			if start == -1 {
				start = i
			}
		}
	}
	if start != -1 {
		if isSensitiveKeyFuzz(s[start:]) {
			return true
		}
	}
	return false
}

func isSensitiveKeyFuzz(key string) bool {
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
