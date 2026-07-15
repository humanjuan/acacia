package acacia_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func FuzzPhase10RecursiveRedactionAuditSensitiveKeys(f *testing.F) {
	f.Add("password", "DO_NOT_LEAK_PHASE10_SECRET", "nestedKey", "prefix", false, false, false)
	f.Add("payload", "secret", "token", "auth", true, true, true)
	f.Add("token", "val\nwith\rnewline", "pwd", "user", false, true, false)

	f.Fuzz(func(t *testing.T, key, value, nestedKey, prefix string, useObject, useNamespace, useAny bool) {
		tmpDir, err := os.MkdirTemp("", "acacia-redact-fuzz-*")
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

		isExternalSensitive := isSensitiveKeyAuditFuzz(key)
		isNestedSensitive := isSensitiveKeyAuditFuzz(nestedKey)
		isPrefixSensitive := isSensitiveKeyAuditFuzz(prefix)

		shouldRedact := isExternalSensitive || (useNamespace && isPrefixSensitive) || (useNamespace && isNestedSensitive) || (!useNamespace && !useAny && isExternalSensitive) || (useAny && isNestedSensitive)

		valToLog := value
		secretProbe := ""
		if shouldRedact {
			secretProbe = "FUZZ_SECRET_MARKER_" + value
			valToLog = secretProbe
		}

		event := logger.InfoEvent()

		if useAny {
			payload := map[string]interface{}{
				nestedKey: valToLog,
				"nested": map[string]interface{}{
					nestedKey: valToLog,
				},
			}
			if useObject {
				event.Object("obj", func(o *acacia.Object) {
					o.Any(key, payload)
				})
			} else {
				event.Any(key, payload)
			}
		} else if useNamespace {
			if useObject {
				event.Object("obj", func(o *acacia.Object) {
					o.Namespace(prefix, func(n *acacia.Namespace) {
						n.Str(nestedKey, valToLog)
					})
				})
			} else {
				event.Namespace(prefix, func(n *acacia.Namespace) {
					n.Str(nestedKey, valToLog)
				})
			}
		} else if useObject {
			event.Object("obj", func(o *acacia.Object) {
				o.Str(key, valToLog)
			})
		} else {
			event.Str(key, valToLog)
		}

		event.Msg("msg")
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

		if shouldRedact {
			if strings.Contains(line, secretProbe) {
				t.Fatalf("leak detected for sensitive input: secretProbe=%q, line=%s", secretProbe, line)
			}
		}
	})
}

func isSensitiveKeyAuditFuzz(key string) bool {
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
	case "authorization", "authorizationheader", "cookie", "setcookie", "token", "accesstoken", "refreshtoken",
		"password", "passwd", "pwd", "apikey", "privatekey", "clientsecret", "secret",
		"bearer", "jwt", "session", "sessionid", "csrf", "xsrf", "xapikey":
		return true
	default:
		return false
	}
}
