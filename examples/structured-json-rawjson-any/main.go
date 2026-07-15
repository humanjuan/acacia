package main

import (
	"log"
	"os"

	acacia "github.com/humanjuan/acacia/v2"
)

type userAccount struct {
	Name   string `json:"name"`
	Active bool   `json:"active"`
}

func main() {
	_ = os.MkdirAll("./logs", 0700)

	lg, err := acacia.Start("structured-rawjson-any.log", "./logs", acacia.Level.INFO)
	if err != nil {
		log.Fatalf("failed to start logger: %v", err)
	}
	defer lg.Close()

	// 1. RawJSON with valid compacted output
	lg.InfoEvent().
		RawJSON("client_data", []byte(`{
			"ip": "10.0.0.1",
			"status": "connected"
		}`)).
		Msg("raw JSON compacted and emitted")

	// 2. RawJSON with invalid JSON input (degrades to escaped string)
	lg.InfoEvent().
		RawJSON("invalid_payload", []byte(`{"missing_brace":`)).
		Msg("invalid JSON safely escaped")

	// 3. Any with a struct (fallback path)
	lg.InfoEvent().
		Any("account", userAccount{Name: "Juan", Active: true}).
		Msg("struct serialized dynamically")

	// 4. Any with non-serializable type (safely captured without panics)
	lg.InfoEvent().
		Any("unsupported", func() {}).
		Msg("function serialization degraded to safe error sentinel")

	lg.Sync()
}
