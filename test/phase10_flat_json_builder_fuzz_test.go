package acacia_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func FuzzPhase10FlatJSONStringEscaping(f *testing.F) {
	// Seed inputs
	f.Add("normal_key", "normal_value")
	f.Add("key_with_\n_newline", "value_with_\r\n_newline")
	f.Add("sensitive_password_key", "secret123")
	f.Add("invalid_\xff\xfe_utf8", "val_\xff\xfe_utf8")
	f.Add(`key"quote\slash`, `val"quote\slash`)

	tmpDir, err := os.MkdirTemp("", "acacia-flat-fuzz-*")
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

	f.Fuzz(func(t *testing.T, key string, val string) {
		// Clean the log file between fuzz iterations
		os.WriteFile(logPath, []byte{}, 0644)

		event := logger.InfoEvent()
		if event == nil {
			return
		}

		event.Str(key, val).Msg("fuzz")
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
			t.Fatalf("produced invalid JSON for key=%q, val=%q: %s", key, val, line)
		}

		// Verify that no raw carriage return or newline exists inside the JSON structure
		// (except for the final newline at the end of the log line, which we trimmed anyway)
		if strings.Contains(line, "\n") || strings.Contains(line, "\r") {
			t.Fatalf("raw CRLF detected inside log line: %q", line)
		}

		// Check redaction if key is sensitive
		// We normalize key to see if it should have been redacted
		normalized := normalizeKey(key)
		isSensitive := false
		switch normalized {
		case "authorization", "cookie", "setcookie", "token", "accesstoken", "refreshtoken",
			"password", "passwd", "pwd", "apikey", "privatekey", "clientsecret", "secret",
			"bearer", "jwt", "session", "sessionid", "csrf", "xsrf", "xapikey":
			isSensitive = true
		}

		if isSensitive && len(val) > 0 {
			// If it's sensitive, the original value should not be in the output
			if strings.Contains(line, val) && val != "[REDACTED]" {
				t.Fatalf("sensitive value leaked in JSON output: key=%q, val=%q, line=%s", key, val, line)
			}
		}
	})
}

func normalizeKey(key string) string {
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
