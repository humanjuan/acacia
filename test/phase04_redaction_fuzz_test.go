package acacia_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	acacia "github.com/humanjuan/acacia/v2"
)

func hasControlBytes(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 32 {
			return true
		}
	}
	return false
}

func FuzzRedactionSensitiveKeysNoLeak(f *testing.F) {
	seeds := []string{
		"password",
		"Authorization",
		"set_cookie",
		"X-API-Key",
		"refresh-token",
	}
	for _, seed := range seeds {
		f.Add(seed, "secret-leak-value")
	}

	f.Fuzz(func(t *testing.T, key, val string) {
		if len(key) == 0 || len(key) > 60 || len(val) == 0 {
			return
		}
		if !utf8.ValidString(key) || !utf8.ValidString(val) {
			return
		}
		if strings.ContainsAny(key, `"\`) || hasControlBytes(key) {
			return
		}

		tmpDir := t.TempDir()
		lg, err := acacia.Start("fuzz_redact_leak.log", tmpDir, acacia.Level.INFO,
			acacia.WithBufferSize(100),
			acacia.WithFlushInterval(10*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("failed to start logger: %v", err)
		}

		lg.InfoEvent().Str(key, val).Send()
		lg.Close()

		logPath := filepath.Join(tmpDir, "fuzz_redact_leak.log")
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("failed to read log: %v", err)
		}

		if len(data) == 0 {
			return
		}

		line := strings.TrimSpace(string(data))
		if !json.Valid([]byte(line)) {
			t.Fatalf("invalid JSON: %q", line)
		}

		// Normalize key comparison
		var buf [64]byte
		w := 0
		for i := 0; i < len(key); i++ {
			c := key[i]
			if c == '-' || c == '_' || c == '.' {
				continue
			}
			if w >= 64 {
				return
			}
			if c >= 'A' && c <= 'Z' {
				buf[w] = c + ('a' - 'A')
			} else {
				buf[w] = c
			}
			w++
		}
		normKey := string(buf[:w])
		isSensitive := false
		switch normKey {
		case "authorization", "cookie", "setcookie", "token", "accesstoken", "refreshtoken",
			"password", "passwd", "pwd", "apikey", "privatekey", "clientsecret", "secret",
			"bearer", "jwt", "session", "sessionid", "csrf", "xsrf", "xapikey":
			isSensitive = true
		}

		if isSensitive {
			if strings.Contains(line, val) && !strings.Contains(val, "[REDACTED]") {
				t.Errorf("sensitive value leak for key %q (normalized %q): %s", key, normKey, line)
			}
			var m map[string]interface{}
			json.Unmarshal([]byte(line), &m)
			for k, v := range m {
				if strings.EqualFold(k, key) {
					if v != "[REDACTED]" {
						t.Errorf("sensitive key %q did not have [REDACTED] value: %v", key, v)
					}
				}
			}
		}
	})
}

func FuzzRedactionKeyVariantsNoBypass(f *testing.F) {
	seeds := []string{
		"a-p-i-k-e-y",
		"___password___",
		"aPiKeY",
		"Client_Secret",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, rawKey string) {
		if len(rawKey) == 0 || len(rawKey) > 60 {
			return
		}
		if !utf8.ValidString(rawKey) {
			return
		}
		if strings.ContainsAny(rawKey, `"\`) || hasControlBytes(rawKey) {
			return
		}

		tmpDir := t.TempDir()
		lg, err := acacia.Start("fuzz_bypass.log", tmpDir, acacia.Level.INFO,
			acacia.WithBufferSize(100),
			acacia.WithFlushInterval(10*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("failed to start logger: %v", err)
		}

		lg.InfoEvent().Str(rawKey, "secret-val").Send()
		lg.Close()

		logPath := filepath.Join(tmpDir, "fuzz_bypass.log")
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("failed to read log: %v", err)
		}

		if len(data) == 0 {
			return
		}

		line := strings.TrimSpace(string(data))
		var m map[string]interface{}
		json.Unmarshal([]byte(line), &m)

		var buf [64]byte
		w := 0
		for i := 0; i < len(rawKey); i++ {
			c := rawKey[i]
			if c == '-' || c == '_' || c == '.' {
				continue
			}
			if w >= 64 {
				return
			}
			if c >= 'A' && c <= 'Z' {
				buf[w] = c + ('a' - 'A')
			} else {
				buf[w] = c
			}
			w++
		}
		normKey := string(buf[:w])
		isSensitive := false
		switch normKey {
		case "authorization", "cookie", "setcookie", "token", "accesstoken", "refreshtoken",
			"password", "passwd", "pwd", "apikey", "privatekey", "clientsecret", "secret",
			"bearer", "jwt", "session", "sessionid", "csrf", "xsrf", "xapikey":
			isSensitive = true
		}

		if isSensitive {
			for k, v := range m {
				if strings.EqualFold(k, rawKey) {
					if v != "[REDACTED]" {
						t.Errorf("key variant bypass detected for raw key %q (norm %q): %v", rawKey, normKey, v)
					}
				}
			}
		}
	})
}

func FuzzRedactionMixedTypesNoLeak(f *testing.F) {
	f.Add("password", int(123), true)
	f.Add("token", int(456), false)
	f.Add("api_key", int(789), true)

	f.Fuzz(func(t *testing.T, key string, intVal int, boolVal bool) {
		if len(key) == 0 || len(key) > 60 {
			return
		}
		if !utf8.ValidString(key) {
			return
		}
		if strings.ContainsAny(key, `"\`) || hasControlBytes(key) {
			return
		}

		tmpDir := t.TempDir()
		lg, err := acacia.Start("fuzz_mixed_leak.log", tmpDir, acacia.Level.INFO,
			acacia.WithBufferSize(100),
			acacia.WithFlushInterval(10*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("failed to start logger: %v", err)
		}

		lg.InfoEvent().
			Int(key, intVal).
			Bool(key+"_bool", boolVal).
			Float64(key+"_float", float64(intVal)/10.0).
			Time(key+"_time", time.Unix(1710000000, 0).UTC()).
			Dur(key+"_dur", time.Second).
			ErrKey(key+"_err", errors.New("my sensitive error")).
			Send()

		lg.Close()

		logPath := filepath.Join(tmpDir, "fuzz_mixed_leak.log")
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("failed to read log: %v", err)
		}

		if len(data) == 0 {
			return
		}

		line := strings.TrimSpace(string(data))
		var m map[string]interface{}
		json.Unmarshal([]byte(line), &m)

		for k, v := range m {
			// Normalize
			var buf [64]byte
			w := 0
			for i := 0; i < len(k); i++ {
				c := k[i]
				if c == '-' || c == '_' || c == '.' {
					continue
				}
				if w >= 64 {
					return
				}
				if c >= 'A' && c <= 'Z' {
					buf[w] = c + ('a' - 'A')
				} else {
					buf[w] = c
				}
				w++
			}
			normKey := string(buf[:w])
			isSensitive := false
			switch normKey {
			case "authorization", "cookie", "setcookie", "token", "accesstoken", "refreshtoken",
				"password", "passwd", "pwd", "apikey", "privatekey", "clientsecret", "secret",
				"bearer", "jwt", "session", "sessionid", "csrf", "xsrf", "xapikey":
				isSensitive = true
			}

			if isSensitive {
				if v != "[REDACTED]" {
					t.Errorf("type redaction bypass for key %q (norm %q) under value: %v", k, normKey, v)
				}
			}
		}
	})
}

func FuzzRedactionNonSensitivePreserved(f *testing.F) {
	f.Add("username", "juan")
	f.Add("service", "billing")

	f.Fuzz(func(t *testing.T, key, val string) {
		if len(key) == 0 || len(key) > 60 || len(val) == 0 || len(val) > 200 {
			return
		}
		if !utf8.ValidString(key) || !utf8.ValidString(val) {
			return
		}
		if strings.ContainsAny(key, `"\`) || hasControlBytes(key) {
			return
		}

		// Normalize key comparison
		var buf [64]byte
		w := 0
		for i := 0; i < len(key); i++ {
			c := key[i]
			if c == '-' || c == '_' || c == '.' {
				continue
			}
			if w >= 64 {
				return
			}
			if c >= 'A' && c <= 'Z' {
				buf[w] = c + ('a' - 'A')
			} else {
				buf[w] = c
			}
			w++
		}
		normKey := string(buf[:w])
		isSensitive := false
		switch normKey {
		case "authorization", "cookie", "setcookie", "token", "accesstoken", "refreshtoken",
			"password", "passwd", "pwd", "apikey", "privatekey", "clientsecret", "secret",
			"bearer", "jwt", "session", "sessionid", "csrf", "xsrf", "xapikey":
			isSensitive = true
		}

		if isSensitive {
			return // Skip sensitive keys
		}

		tmpDir := t.TempDir()
		lg, err := acacia.Start("fuzz_preserve.log", tmpDir, acacia.Level.INFO,
			acacia.WithBufferSize(100),
			acacia.WithFlushInterval(10*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("failed to start logger: %v", err)
		}

		lg.InfoEvent().Str(key, val).Send()
		lg.Close()

		logPath := filepath.Join(tmpDir, "fuzz_preserve.log")
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("failed to read log: %v", err)
		}

		if len(data) == 0 {
			return
		}

		line := strings.TrimSpace(string(data))
		var m map[string]interface{}
		json.Unmarshal([]byte(line), &m)

		for k, v := range m {
			if k == key {
				if v != val {
					t.Errorf("preserved key %q value changed from %q to %q", key, val, v)
				}
			}
		}
	})
}
