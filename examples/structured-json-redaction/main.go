package main

import (
	"log"
	"os"

	acacia "github.com/humanjuan/acacia/v2"
)

func main() {
	_ = os.MkdirAll("./logs", 0700)

	lg, err := acacia.Start("structured-redaction.log", "./logs", acacia.Level.INFO)
	if err != nil {
		log.Fatalf("failed to start logger: %v", err)
	}
	defer lg.Close()

	// 1. Automatic Key Redaction
	lg.InfoEvent().
		Str("password", "raw_password_value").
		Str("authorization_header", "Bearer raw_token").
		Msg("user credentials verified")

	// 2. Explicit Secret Redaction
	lg.InfoEvent().
		Secret("custom_field_key", "secret_value").
		StrRedacted("auth_token", "jwt_token_val").
		Msg("explicit redaction used")

	// 3. Namespace Segment Redaction
	lg.InfoEvent().
		Namespace("regular", func(n *acacia.Namespace) {
			n.Str("user_password_field", "secret123")
		}).
		Msg("prefixed sensitive keys redacted")

	// 4. Any Best-Effort Redaction
	lg.InfoEvent().
		Any("payload", map[string]interface{}{
			"normal_field": "ok",
			"password":     "inside_secret",
		}).
		Msg("any fallback best-effort redaction")

	// 5. RawJSON External Key Redaction Boundary
	lg.InfoEvent().
		RawJSON("client_secret", []byte(`{"ip":"127.0.0.1"}`)).
		Msg("raw JSON redacted because key name is sensitive")

	// 6. RawJSON Non-Sensitive External Key (Trusted Input Only)
	lg.InfoEvent().
		RawJSON("payload_raw", []byte(`{"password":"will_not_be_redacted"}`)).
		Msg("trusted raw JSON (no internal parsing for performance)")

	lg.Sync()
}
