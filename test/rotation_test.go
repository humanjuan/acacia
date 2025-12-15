package acacia_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func readAndCleanDir(t *testing.T, dir string, baseName string) []string {
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Fallo al leer directorio de prueba %s: %v", dir, err)
	}

	var rotatedFiles []string
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if file.Name() != baseName {
			rotatedFiles = append(rotatedFiles, file.Name())
		}
	}

	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Logf("Advertencia: No se pudo limpiar el directorio temporal %s: %v", dir, err)
		}
	})
	return rotatedFiles
}

func TestIntensiveRotation(t *testing.T) {
	const logName = "intensive.log"
	const maxRot = 3
	const logLine = "Log Line to force rotation. ABCDEFGHIJKLMNOPQRSTUVWXYZ\n"

	dir := t.TempDir()
	log, err := acacia.Start(logName, dir, acacia.Level.DEBUG)
	if err != nil {
		t.Fatalf("Fallo Start: %v", err)
	}
	defer log.Close()

	t.Log("--- 1. Test de Rotación por Tamaño ---")

	// Configurar rotación por tamaño: 1 MB y 3 backups
	log.Rotation(1, maxRot)
	big := strings.Repeat("X", 2*1024*1024)
	for rotationCount := 0; rotationCount <= maxRot; rotationCount++ {
		t.Logf("Iniciando ciclo %d de rotación por tamaño.", rotationCount)
		log.Info(big)
		log.Sync()
		rotatedFiles := readAndCleanDir(t, dir, logName)
		if rotationCount == 0 {
			if len(rotatedFiles) == 0 {
				t.Errorf("FAIL: No se detectaron backups tras la primera rotación. Archivos: %v", rotatedFiles)
			}
		}
		if len(rotatedFiles) > maxRot+1 {
			t.Errorf("FAIL: Se excedió el número máximo de backups (%d): %v", maxRot+1, rotatedFiles)
		}
	}

	t.Log("--- 2. Test de Rotación Diaria ---")
	log.DailyRotation(true)

	log.Info("DailyLog: verificando que NO se cree archivo fechado inmediatamente")
	log.Sync()
	today := time.Now().Format("2006-01-02")
	todayName := fmt.Sprintf("%s-%s", strings.TrimSuffix(logName, filepath.Ext(logName)), today)
	expectedToday := todayName + filepath.Ext(logName)

	// Con el nuevo comportamiento, al habilitar daily no debe aparecer inmediatamente el archivo con fecha de hoy
	if _, err := os.Stat(filepath.Join(dir, expectedToday)); err == nil {
		t.Errorf("FAIL: Se encontró el archivo fechado de hoy antes del cambio real de día: %s", expectedToday)
	}

	// Debe seguir existiendo el archivo base activo
	if _, err := os.Stat(filepath.Join(dir, logName)); err != nil {
		t.Errorf("FAIL: No existe el archivo base activo tras habilitar daily: %s", logName)
	}
}

/*
# run
go test -run TestIntensiveRotation -v
*/
