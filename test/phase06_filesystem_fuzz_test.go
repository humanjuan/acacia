package acacia_test

import (
	"strings"
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func FuzzValidateLogFilenameNoTraversal(f *testing.F) {
	f.Add("../evil.log")
	f.Add("..\\evil.log")
	f.Add("app.log")
	f.Add("subdir/app.log")
	f.Add("/tmp/evil.log")

	f.Fuzz(func(t *testing.T, name string) {
		err := acacia.ValidateLogFilename(name)
		if err == nil {
			// If it passed validation, it MUST NOT contain traversal sequences or path separators
			if strings.Contains(name, "..") || strings.ContainsAny(name, `/\`) {
				t.Fatalf("name %q passed validation but contains traversal/separators", name)
			}
		}
	})
}

func FuzzValidateLogFilenameNoNUL(f *testing.F) {
	f.Add("app.log\x00evil")
	f.Add("app.log")

	f.Fuzz(func(t *testing.T, name string) {
		err := acacia.ValidateLogFilename(name)
		if err == nil {
			if strings.Contains(name, "\x00") {
				t.Fatalf("name %q passed validation but contains NUL byte", name)
			}
		}
	})
}

func FuzzValidateLogFilenameAcceptsSafeNames(f *testing.F) {
	f.Add("app.log")
	f.Add("audit.jsonl")
	f.Add("service-2026.log")

	f.Fuzz(func(t *testing.T, name string) {
		_ = acacia.ValidateLogFilename(name)
	})
}
