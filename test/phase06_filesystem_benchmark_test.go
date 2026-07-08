package acacia_test

import (
	"path/filepath"
	"strings"
	"testing"

	acacia "github.com/humanjuan/acacia/v2"
)

func BenchmarkValidateLogFilenameSafe(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = acacia.ValidateLogFilename("app.log")
	}
}

func BenchmarkValidateLogFilenameTraversal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = acacia.ValidateLogFilename("../evil.log")
	}
}

func BenchmarkSafeJoinLogPath(b *testing.B) {
	dir := "/var/log"
	name := "app.log"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = filepath.Join(dir, name)
	}
}

func BenchmarkRotationCheckNoRotate(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_check.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		b.Fatalf("failed to start logger: %v", err)
	}
	lg.Rotation(10, 3) // 10MB
	defer lg.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info("small log message line that does not trigger rotation")
	}
}

func BenchmarkRotationSizeSmall(b *testing.B) {
	tmpDir := b.TempDir()
	lg, err := acacia.Start("bench_rot.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		b.Fatalf("failed to start logger: %v", err)
	}
	lg.Rotation(1, 2)
	defer lg.Close()

	largeMsg := strings.Repeat("X", 1100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lg.Info(largeMsg)
		lg.Sync()
	}
}
