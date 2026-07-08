package acacia_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

// A. Filename validation tests
func TestFilenameValidation(t *testing.T) {
	tmpDir := t.TempDir()

	invalidNames := []string{
		"../evil.log",
		"..\\evil.log",
		"/tmp/evil.log",
		"subdir/app.log",
		"subdir\\app.log",
		".",
		"..",
		"",
		"app.log\x00evil",
		"app/../evil.log",
		"C:\\temp\\evil.log",
	}

	for _, name := range invalidNames {
		t.Run("invalid_"+name, func(t *testing.T) {
			_, err := acacia.Start(name, tmpDir, acacia.Level.INFO)
			if !errors.Is(err, acacia.ErrInvalidLogFilename) {
				t.Errorf("expected ErrInvalidLogFilename for name %q, got %v", name, err)
			}
		})
	}

	validNames := []string{
		"app.log",
		"audit.jsonl",
		"service-2026.log",
	}

	for _, name := range validNames {
		t.Run("valid_"+name, func(t *testing.T) {
			lg, err := acacia.Start(name, tmpDir, acacia.Level.INFO)
			if err != nil {
				t.Fatalf("unexpected error for valid name %q: %v", name, err)
			}
			lg.Close()
		})
	}
}

// B. Directory validation tests
func TestDirectoryValidation(t *testing.T) {
	tmpDir := t.TempDir()

	// Test non-existent directory (should return not exist error)
	nonExistent := filepath.Join(tmpDir, "does-not-exist")
	_, err := acacia.Start("app.log", nonExistent, acacia.Level.INFO)
	if err == nil || !os.IsNotExist(err) {
		t.Errorf("expected path not exist error for %q, got %v", nonExistent, err)
	}

	// Test base path is a file (should return ErrInvalidLogDirectory)
	filePath := filepath.Join(tmpDir, "file.log")
	if err := os.WriteFile(filePath, []byte("test"), 0600); err != nil {
		t.Fatalf("failed to create dummy file: %v", err)
	}
	_, err = acacia.Start("app.log", filePath, acacia.Level.INFO)
	if !errors.Is(err, acacia.ErrInvalidLogDirectory) {
		t.Errorf("expected ErrInvalidLogDirectory for filepath base, got %v", err)
	}

	// Test base path is a symlink (should return ErrUnsafeSymlink)
	symlinkPath := filepath.Join(tmpDir, "symlinked_dir")
	if err := os.Symlink(tmpDir, symlinkPath); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}
	_, err = acacia.Start("app.log", symlinkPath, acacia.Level.INFO)
	if !errors.Is(err, acacia.ErrUnsafeSymlink) {
		t.Errorf("expected ErrUnsafeSymlink for symlinked base path, got %v", err)
	}
}

// C. Permissions tests
func TestPermissionsSecurity(t *testing.T) {
	tmpDir := t.TempDir()

	// Insecure mode WithFileMode(0666) -> should strip world-writable (0002) and group-writable (0020)
	lg, err := acacia.Start("perminsecure.log", tmpDir, acacia.Level.INFO,
		acacia.WithFileMode(0666),
	)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}
	lg.Info("test")
	lg.Close()

	fullPath := filepath.Join(tmpDir, "perminsecure.log")
	info, err := os.Stat(fullPath)
	if err != nil {
		t.Fatalf("failed to stat log file: %v", err)
	}
	perm := info.Mode().Perm()
	if perm&0002 != 0 || perm&0020 != 0 {
		t.Errorf("expected file mode to strip group/world writable bits, got %o", perm)
	}

	// Default file mode should be 0600
	lgDefault, err := acacia.Start("permdefault.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}
	lgDefault.Info("test")
	lgDefault.Close()

	infoDefault, err := os.Stat(filepath.Join(tmpDir, "permdefault.log"))
	if err != nil {
		t.Fatalf("failed to stat default log: %v", err)
	}
	permDefault := infoDefault.Mode().Perm()
	if permDefault&^0600 != 0 {
		t.Errorf("expected default permissions to be at most 0600, got %o", permDefault)
	}
}

// D. Symlink active file tests
func TestSymlinkActiveFile(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "active.log")
	targetFile := filepath.Join(tmpDir, "target.log")

	if err := os.WriteFile(targetFile, []byte("untouched"), 0600); err != nil {
		t.Fatalf("failed to create target file: %v", err)
	}

	// Symlink active.log -> target.log
	if err := os.Symlink(targetFile, logFile); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	// Try starting logger pointing to symlink
	_, err := acacia.Start("active.log", tmpDir, acacia.Level.INFO)
	if !errors.Is(err, acacia.ErrUnsafeSymlink) {
		t.Errorf("expected ErrUnsafeSymlink, got %v", err)
	}

	// Target file must remain untouched
	content, err := os.ReadFile(targetFile)
	if err != nil {
		t.Fatalf("failed to read target: %v", err)
	}
	if string(content) != "untouched" {
		t.Errorf("expected target file content to be untouched, got %q", string(content))
	}
}

// E. Symlink backup/rotation tests
func TestSymlinkBackupRotation(t *testing.T) {
	tmpDir := t.TempDir()

	lg, err := acacia.Start("app.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	lg.Rotation(1, 2) // 1MB, 2 backups

	// Create symlink at target backup file: app.log.0 -> target.log
	targetFile := filepath.Join(tmpDir, "target.log")
	if err := os.WriteFile(targetFile, []byte("untouched"), 0600); err != nil {
		t.Fatalf("failed to create target file: %v", err)
	}
	backupFile := filepath.Join(tmpDir, "app.log.0")
	if err := os.Symlink(targetFile, backupFile); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	// Write enough (1.1MB) to trigger size rotation
	lg.Info(strings.Repeat("X", 1100000))
	lg.Sync()
	lg.Close()

	// rotationErrorsSeq should be incremented or target should remain untouched
	stats := lg.Stats()
	if stats.RotationErrorsTotal == 0 {
		t.Error("expected rotation errors due to backup symlink, got 0")
	}

	content, err := os.ReadFile(targetFile)
	if err != nil {
		t.Fatalf("failed to read target: %v", err)
	}
	if string(content) != "untouched" {
		t.Errorf("expected target file content to be untouched, got %q", string(content))
	}
}

// F & G. Size rotation normal and backup limits
func TestSizeRotationNormal(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("rot.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}
	lg.Rotation(1, 2)

	// Trigger rotation multiple times by sending 1.1MB logs
	largeMsg := strings.Repeat("X", 1100000)
	for i := 0; i < 3; i++ {
		lg.Info(largeMsg)
		lg.Sync()
		time.Sleep(10 * time.Millisecond)
	}
	lg.Close()

	// Verify that rot.log.0, rot.log.1, and rot.log.2 exist, but rot.log.3 does NOT exist
	if _, err := os.Stat(filepath.Join(tmpDir, "rot.log.0")); err != nil {
		t.Errorf("expected rot.log.0 to exist: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tmpDir, "rot.log.1")); err != nil {
		t.Errorf("expected rot.log.1 to exist: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tmpDir, "rot.log.2")); err != nil {
		t.Errorf("expected rot.log.2 to exist: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tmpDir, "rot.log.3")); err == nil || !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("expected rot.log.3 to not exist, got err: %v", err)
	}
}

// I. No regression Sync/Close
func TestNoRegressionSyncClose(t *testing.T) {
	tmpDir := t.TempDir()
	lg, err := acacia.Start("sync_close_ok.log", tmpDir, acacia.Level.INFO)
	if err != nil {
		t.Fatalf("failed to start logger: %v", err)
	}
	lg.Info("normal msg")
	lg.Sync()
	lg.Close()
}
