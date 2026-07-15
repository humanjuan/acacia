package main

import (
	"log"
	"os"

	acacia "github.com/humanjuan/acacia/v2"
)

func main() {
	_ = os.MkdirAll("./logs", 0700)

	lg, err := acacia.Start("structured-nested.log", "./logs", acacia.Level.INFO)
	if err != nil {
		log.Fatalf("failed to start logger: %v", err)
	}
	defer lg.Close()

	// 1. Nested Objects
	lg.InfoEvent().
		Object("user", func(u *acacia.Object) {
			u.Str("id", "u_123").
				Str("country", "CL")
		}).
		Msg("user metadata loaded")

	// 2. Arrays
	lg.InfoEvent().
		Array("items", func(a *acacia.Array) {
			a.Object(func(i *acacia.Object) {
				i.Str("sku", "ABC").
					Int("qty", 2)
			})
			a.Str("manual-note")
		}).
		Msg("cart processed")

	// 3. Group Scope (nested scope)
	lg.InfoEvent().
		Group("network", func(g *acacia.Object) {
			g.Str("interface", "eth0").
				Int("port", 443)
		}).
		Msg("network socket open")

	// 4. Namespace Scope (flat prefixed key scope)
	lg.InfoEvent().
		Namespace("db", func(n *acacia.Namespace) {
			n.Str("query", "SELECT 1").
				Int("connections", 12)
		}).
		Msg("db check done")

	lg.Sync()
}
