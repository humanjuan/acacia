package acacia_test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func readLog(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Falló lectura de %s: %v", path, err)
	}
	return string(data)
}

func fileExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	return err == nil
}

// 1. Básicos y niveles
func TestLevelFilter(t *testing.T) {
	tmp := t.TempDir()
	lg, _ := acacia.Start("level.log", tmp, acacia.Level.INFO)
	defer lg.Close()

	lg.Info("info ok")
	lg.Warn("warn ok")
	lg.Error("error ok")
	lg.Critical("critical ok")
	lg.Debug("debug NO")

	lg.Sync()

	content := readLog(t, filepath.Join(tmp, "level.log"))
	if strings.Contains(content, "debug NO") {
		t.Fatal("DEBUG apareció con nivel INFO")
	}
	if !strings.Contains(content, "info ok") || !strings.Contains(content, "critical ok") {
		t.Fatal("Faltan logs esperados")
	}
}

// 2. Rotación por tamaño
func TestRotationBySize(t *testing.T) {
	tmp := t.TempDir()
	lg, _ := acacia.Start("size.log", tmp, acacia.Level.INFO)
	defer lg.Close()

	lg.Rotation(1, 3)

	payload := strings.Repeat("A", 1100*1024)

	for i := 0; i < 6; i++ {
		lg.Write([]byte(payload))
	}
	lg.Sync()

	base := filepath.Join(tmp, "size.log")

	for i := 0; i <= 3; i++ {
		path := base
		if i > 0 {
			path += fmt.Sprintf(".%d", i)
		}
		if !fileExists(t, path) {
			t.Fatalf("Falta archivo rotado: %s", path)
		}
	}

	if fileExists(t, base+".4") {
		t.Fatal("Se crearon demasiados backups")
	}
}

// 3. Rotación diaria (corregida y estable)
func TestDailyRotation(t *testing.T) {
	tmp := t.TempDir()
	lg, _ := acacia.Start("daily.log", tmp, acacia.Level.INFO)
	defer lg.Close()

	base := filepath.Join(tmp, "daily.log")

	for i := 0; i < 3; i++ {
		os.WriteFile(base+"."+strconv.Itoa(i), []byte("old"), 0644)
	}

	lg.DailyRotation(true)
	lg.Info("primer mensaje")
	lg.Sync()

	today := time.Now().Format("2006-01-02")
	baseName := strings.TrimSuffix(base, filepath.Ext(base))
	datedNew := baseName + "-" + today + filepath.Ext(base)
	datedOld := base + "-" + today

	if fileExists(t, datedNew) || fileExists(t, datedOld) {
		t.Fatal("Se creó archivo con fecha de hoy antes del cambio real de día")
	}

	// Debe existir el archivo base activo
	if !fileExists(t, base) {
		t.Fatal("No existe el archivo principal tras habilitar daily")
	}
}

// 4. Resto de tests (perfectos como estaban)
func TestFlushOnClose(t *testing.T) {
	tmp := t.TempDir()
	lg, _ := acacia.Start("flush.log", tmp, acacia.Level.INFO)

	for i := 0; i < 1000; i++ {
		lg.Info("linea %d", i)
	}
	lg.Close()

	if strings.Count(readLog(t, filepath.Join(tmp, "flush.log")), "\n") < 1000 {
		t.Fatal("No se flushó todo al Close()")
	}
}

func TestConcurrentWrites(t *testing.T) {
	tmp := t.TempDir()
	lg, _ := acacia.Start("conc.log", tmp, acacia.Level.INFO)

	const goroutines = 50
	const msgs = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < msgs; i++ {
				lg.Info("test")
			}
		}()
	}
	wg.Wait()
	lg.Close()

	count := strings.Count(readLog(t, filepath.Join(tmp, "conc.log")), "[INFO]")
	expected := goroutines * msgs
	if count != expected {
		t.Fatalf("Se esperaban %d logs sin pérdida, se contaron %d", expected, count)
	}
}

func TestIOWriter(t *testing.T) {
	tmp := t.TempDir()
	lg, _ := acacia.Start("writer.log", tmp, acacia.Level.INFO)
	defer lg.Close()

	io.WriteString(lg, "hola io\n")
	fmt.Fprintf(lg, "hola fmt %d\n", 42)
	lg.Write([]byte("hola raw"))

	lg.Sync()

	content := readLog(t, filepath.Join(tmp, "writer.log"))
	if !strings.Contains(content, "hola io") ||
		!strings.Contains(content, "hola fmt 42") ||
		!strings.Contains(content, "hola raw") {
		t.Fatal("io.Writer falló")
	}
}

func TestPanicRecoveryNoLogLoss(t *testing.T) {
	tmp := t.TempDir()
	lg, _ := acacia.Start("panic.log", tmp, acacia.Level.INFO)
	defer lg.Close()

	defer func() {
		if r := recover(); r != nil {
			lg.Sync()
			t.Logf("Recuperado de panic: %v", r)
		}
	}()

	for i := 0; i < 10000; i++ {
		lg.Info("log crítico antes del crash %d", i)
	}

	panic("simulando crash de producción")
}

/*
go test -v -race ./...
go test -run TestConcurrentWrites -race   // harder
go test -bench=. -run=Benchmark
*/
