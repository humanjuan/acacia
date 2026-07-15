# Local Audit Runbook

This runbook describes the steps to audit the code locally before committing to the repository.

---

## Step 1: Unit and Concurrency Tests
Run the unit tests and the Go race detector to check the buffer pools and asynchronous queue:
```bash
go test ./...
go test -race ./...
```

---

## Step 2: Single-Core Contention Benchmarks
Run benchmarks forcing a single OS thread to check goroutine scheduling contention:
```bash
GOMAXPROCS=1 go test ./... -bench=. -benchmem -run '^$' -count=10 > .private/bench/bench_gmp1.txt
```
Review `.private/bench/bench_gmp1.txt` to ensure allocations and latencies have not regressed.

---

## Step 3: Local Profiling
If you detect performance regressions, collect CPU and Heap profiles directly in the private directory:
```bash
go test -cpuprofile=.private/pprof/cpu.out -memprofile=.private/pprof/mem.out -bench=. -run=^$ ./...
```

---

## Step 4: Versioning Leak Checks
Before committing, verify that no private files are staged:

1. **Verify Ignored Status:**
   ```bash
   git check-ignore -v .private/README.md
   git check-ignore -v .private/reports/private-evidence.md
   ```
   *These commands must output the matching ignore rule from your `.gitignore`.*

2. **Verify Untracked Status:**
   ```bash
   git ls-files .private docs/private reports/private
   ```
   *This command must output nothing.*

3. **Verify Git Status:**
   ```bash
   git status --short
   ```
   *Ensure no files with `.pprof`, `.prof`, `.out`, or `.trace` extensions are listed.*
