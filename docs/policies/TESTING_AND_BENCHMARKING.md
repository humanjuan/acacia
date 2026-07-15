# Testing and Benchmarking Guide

This guide details the commands and guidelines to validate performance, security, and concurrency in Acacia.

---

## 1. Unit Tests

### Running Unit Tests
To run the standard test suite:
```bash
go test ./...
```

### Race Conditions Detection
All changes must pass the Go race detector:
```bash
go test -race ./...
```

---

## 2. Benchmarks

To ensure consistent performance analysis:
* **Test Filter (`-run '^$'`):** Prevents unit tests from running during the benchmark.
* **Repetitions (`-count=10`):** Run at least 10 iterations to mitigate system scheduler variance.
* **Memory Metrics (`-benchmem`):** Reports bytes allocated and allocations per operation.

### Multi-threaded Benchmarks (Full CPU utilization)
```bash
go test ./... -bench=. -benchmem -run '^$' -count=10
```

### Single-Core Benchmarks (`GOMAXPROCS=1`)
Mandatory test to measure channel and scheduling contention without hardware parallelism:
```bash
GOMAXPROCS=1 go test ./... -bench=. -benchmem -run '^$' -count=10
```

### Dual-Core Benchmarks (`GOMAXPROCS=2`)
```bash
GOMAXPROCS=2 go test ./... -bench=. -benchmem -run '^$' -count=10
```

---

## 3. Security and Fuzzing

Fuzzing is used to check the JSON encoder and redaction logic for escaping or indexing errors:
```bash
go test ./... -run Fuzz -fuzz=Fuzz -fuzztime=60s
```

---

## 4. Stress Testing

Verify drop policies and queue limits under saturation:
```bash
go test ./... -run TestStress -count=1
```

---

## 5. Makefile Automation

The root `Makefile` provides shorthand commands:

| Command | Shorthand for | Purpose |
| :--- | :--- | :--- |
| `make test` | `go test ./...` | Unit tests |
| `make race` | `go test -race ./...` | Concurrency check |
| `make bench` | `go test ./... -bench=. -benchmem -run '^$$' -count=10` | Run standard benchmark |
| `make bench-gmp1` | `GOMAXPROCS=1 ...` | Run single-core benchmark |
| `make fuzz` | `go test ./... -run Fuzz -fuzz=Fuzz -fuzztime=60s` | Run all fuzz targets |
| `make audit` | Runs `test`, `race`, and `bench-gmp1` | Local pre-commit audit |

---

## 6. Capturing Profiles (pprof)

When profiling code for performance analysis:
1. Profiles must be saved to `.private/pprof/` and kept out of Git.
2. Run and save CPU/Memory profiles:
   ```bash
   go test -cpuprofile=.private/pprof/cpu.out -memprofile=.private/pprof/mem.out -bench=. -run=^$ ./...
   ```
3. To view profiles locally via web browser:
   ```bash
   go tool pprof -http=:8080 .private/pprof/cpu.out
   ```

---

## 7. Hard Testing Protocols

For release candidates and regression audits, we apply a strict testing protocol.

### Statistical Comparison with Benchstat
To verify performance changes statistically:
1. Install the official tool:
   ```bash
   go install golang.org/x/perf/cmd/benchstat@latest
   ```
2. Capture baseline benchmarks (`old`):
   ```bash
   git checkout v2.3.0
   go test ./... -bench=. -benchmem -run '^$' -count=20 > .private/bench/old.txt
   ```
3. Capture new branch benchmarks (`new`):
   ```bash
   git checkout dev
   go test ./... -bench=. -benchmem -run '^$' -count=20 > .private/bench/new.txt
   ```
4. Run benchstat comparison:
   ```bash
   benchstat .private/bench/old.txt .private/bench/new.txt
   ```

### Soak Testing
To check for memory leaks or scheduling degradation over long periods, run soak tests under `ACACIA_HARD_SOAK=1`:
```bash
ACACIA_HARD_SOAK=1 go test ./test -run TestPhase09Soak -v -timeout=15m
```

### Validation Matrix
Profiles and traces must cover these capability areas:
* **Flat Writing:** raw string and byte paths.
* **Buffer I/O:** isolated `io.Writer` overhead.
* **JSON Serialization:** comparison of map serialization vs. the event builder.
* **Redaction Safety:** redaction performance.
* **Concurrency Flow:** backpressure drops and Sync/Close lifecycles.
* **Rotation Safety:** size-based, daily, and backup rotation.
* **Integration:** overhead of future OTel integrations (optional, not in this candidate).
