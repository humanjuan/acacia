package acacia_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func FuzzJSONEncoderStringEscaping(f *testing.F) {
	seeds := []string{
		"normal text",
		"newline\nhere",
		"quote\"and\\backslash",
		"\x00\x01\x1f\b\f",
		"🍃 unicode emoji",
		strings.Repeat("S", 1024),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		tmpDir := t.TempDir()
		lg, err := acacia.Start("fuzz_esc.log", tmpDir, acacia.Level.INFO,
			acacia.WithBufferSize(100),
			acacia.WithFlushInterval(1*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("failed to start logger: %v", err)
		}

		lg.InfoEvent().Str("fuzz_val", input).Msg("fuzzed msg")
		lg.Close()

		logPath := filepath.Join(tmpDir, "fuzz_esc.log")
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("failed to read log: %v", err)
		}

		if len(data) == 0 {
			return
		}

		line := strings.TrimSpace(string(data))
		if !json.Valid([]byte(line)) {
			t.Errorf("produced invalid JSON: %q", line)
		}

		var m map[string]interface{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Errorf("failed to parse JSON: %v", err)
		}
	})
}

func FuzzJSONEncoderBuilderMixedFields(f *testing.F) {
	f.Add("service_name", int(200), true, float64(1.234))
	f.Fuzz(func(t *testing.T, service string, status int, cached bool, durationMs float64) {
		tmpDir := t.TempDir()
		lg, err := acacia.Start("fuzz_mixed.log", tmpDir, acacia.Level.INFO,
			acacia.WithBufferSize(100),
			acacia.WithFlushInterval(1*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("failed to start logger: %v", err)
		}

		lg.InfoEvent().
			Str("service", service).
			Int("status", status).
			Bool("cached", cached).
			Float64("duration_ms", durationMs).
			Send()

		lg.Close()

		logPath := filepath.Join(tmpDir, "fuzz_mixed.log")
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("failed to read log: %v", err)
		}

		if len(data) == 0 {
			return
		}

		line := strings.TrimSpace(string(data))
		if !json.Valid([]byte(line)) {
			t.Errorf("produced invalid JSON: %q", line)
		}
	})
}

func FuzzJSONEncoderLargePayload(f *testing.F) {
	f.Add(1024, "A")
	f.Add(4096, "B")
	f.Add(8192, "C")

	f.Fuzz(func(t *testing.T, size int, char string) {
		if size <= 0 || size > 16384 {
			return
		}
		if len(char) != 1 {
			return
		}

		tmpDir := t.TempDir()
		lg, err := acacia.Start("fuzz_large.log", tmpDir, acacia.Level.INFO,
			acacia.WithBufferSize(5_000_000),
			acacia.WithBufferCap(1<<20),
		)
		if err != nil {
			t.Fatalf("failed to start logger: %v", err)
		}

		largeStr := strings.Repeat(char, size)
		lg.InfoEvent().Str("large_payload", largeStr).Send()
		lg.Close()

		logPath := filepath.Join(tmpDir, "fuzz_large.log")
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("failed to read log: %v", err)
		}

		if len(data) == 0 {
			return
		}

		line := strings.TrimSpace(string(data))
		if !json.Valid([]byte(line)) {
			t.Errorf("produced invalid JSON: %q", line)
		}
	})
}
