package main

import (
	"log"
	"os"
	"time"

	acacia "github.com/humanjuan/acacia/v2"
)

func main() {
	// Create local directory for logging safely
	_ = os.MkdirAll("./logs", 0700)

	// Start the logger
	lg, err := acacia.Start("structured-basic.log", "./logs", acacia.Level.INFO)
	if err != nil {
		log.Fatalf("failed to start logger: %v", err)
	}
	defer lg.Close()

	// Log flat structured fields
	lg.InfoEvent().
		Str("service", "payments").
		Int("status", 200).
		Bool("cache_hit", true).
		Dur("latency", 25*time.Millisecond).
		Msg("request completed")

	// Flush buffers before termination
	lg.Sync()
}
