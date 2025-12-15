.PHONY: test race bench bench-gmp1 fuzz audit

test:
	go test ./...

race:
	go test -race ./...

bench:
	go test ./... -bench=. -benchmem -run '^$$' -count=10

bench-gmp1:
	GOMAXPROCS=1 go test ./... -bench=. -benchmem -run '^$$' -count=10

fuzz:
	go test ./... -run Fuzz -fuzz=Fuzz -fuzztime=60s

audit: test race bench-gmp1
