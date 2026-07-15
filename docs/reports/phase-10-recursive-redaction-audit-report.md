# Phase 10 — Recursive Redaction Audit Report

*This is a historical engineering report. The filename is kept for audit continuity.*

## 1. Objective and Scope
The primary objective of Sprint 10.7 is to perform a comprehensive audit, hardening, and documentation of redaction policies and consistency across all structured builders of Acacia Logger:
* **Validation of Sensitive Families**: Complete mapping of the 21 forbidden key families and their variants using case-insensitive normalization and removal of separators (`-`, `_`, `.`).
* **Cross-Builder Verification**: Confirming that Flat, Object, Array, Group, and Namespace builders apply redaction consistently.
* **Boundaries and Limits**:
  * **RawJSON**: Confirming that it redacts only the external key (trusted input only).
  * **Any**: Verifying that it performs external redaction and best-effort internal recursive redaction via `encoding/json`.
  * **Legacy Map**: Documenting compatibility features of v2.x without extending promises of optimization or exact recursive redaction.
* **Security and Robustness**: Validating leak control against encoding failures, cycles, invalid UTF-8 characters, line-break injections (CR/LF), and panics in callbacks.

No new public APIs, OpenTelemetry support, or comparative absolute performance validations on local SSDs are introduced.

---

## 2. Modified and Created Files
* **Modified**:
  * `acacia.go`: Hardened compliance matching for the `authorization_header` / `authorizationheader` key in global selectors.
  * `docs/ACACIA_PROJECT_CONTEXT.md`: Updated status and next action of Sprint 10.7.
  * `docs/PHASE_10_STRUCTURED_JSON_API_CONTRACT.md`: Incorporated the structured redaction matrix.
  * `docs/adr/0004-structured-json-maturity.md`: Logged the audit design decision.
  * `docs/reports/README.md`: Added the link to the public report.
* **Created**:
  * `test/phase10_recursive_redaction_audit_test.go`: Suite with 24 correctness scenarios.
  * `test/phase10_recursive_redaction_audit_fuzz_test.go`: Fuzz target to evaluate behavior with arbitrary keys, values, and prefixes.
  * `test/phase10_recursive_redaction_audit_benchmark_test.go`: Benchmarks characterizing redaction impact.

---

## 3. Redaction Matrix by Builder

| Builder / Flow | Redaction Criterion | Internal Redaction | Redaction Matcher Behavior |
| :--- | :--- | :---: | :---: |
| **Flat Builder** | Sensitive external key in `isSensitiveKey` | N/A | No intentional dynamic concatenation of composite key; the full event may show allocs/op according to benchmark |
| **Object Builder** | Recursive key at typed field level | Yes (child fields) | No intentional dynamic concatenation of composite key; the full event may show allocs/op according to benchmark |
| **Array Builder** | Internal keys of objects added to the array | Yes (in callback) | No intentional dynamic concatenation of composite key; the full event may show allocs/op according to benchmark |
| **Group Builder** | Reuses Object Builder recursive logic | Yes (child fields) | No intentional dynamic concatenation of composite key; the full event may show allocs/op according to benchmark |
| **Namespace** | Composite keys, sensitive prefixes, or segments | Yes | No intentional dynamic concatenation of composite key; the full event may show allocs/op according to benchmark |
| **RawJSON** | External key only (complete replacement) | No (Trusted Input) | No intentional dynamic concatenation of composite key; the full event may show allocs/op according to benchmark |
| **Any Fallback** | Sensitive external and best-effort internal key | Yes (recursive) | Slow path; diagnostic benchmarks observe ~20-45 allocs/op depending on case |
| **Legacy Map** | Based on classic Acacia traversal | Partial (reflective) | Variable |

---

## 4. Sensitive Key Families and Normalization
The following key families were audited and validated (case-insensitive and omitting separators `-`, `_`, `.`):
* `password`, `passwd`, `pwd`
* `token`, `access_token`, `refresh_token`
* `authorization`, `authorization_header`
* `cookie`, `set-cookie`
* `api_key`, `apikey`, `client_secret`, `secret`, `private_key`
* `bearer`, `jwt`, `session`, `session_id`, `csrf`, `xsrf`, `x-api-key`

### Hardening of `authorization_header`
It was detected that the `authorization_header` variant normalized as `authorizationheader` did not exactly match the `"authorization"` selector. The global selectors in `acacia.go` were hardened by explicitly adding the `"authorizationheader"` case, covering that variant under the audited suite.

---

## 5. Test Results and Robustness

### Unit Tests and Race Detector
All suites passed successfully without observing leaks of the secret values used by the executed suite or concurrent conflicts:
```text
ok  	github.com/humanjuan/acacia/v2/test	17.822s
```
* **Panic Sentinels**: Panics triggered within constructor callbacks (`Object`, `Namespace`) are captured by Acacia. They clean the buffer and cleanly emit the label `"_acacia_error":"object callback panic"` (or `"namespace callback panic"`), without observing leaks of raw panic messages to the output file under the executed tests.
* **Line Break Control (CR/LF)**: The executed cases validate that the audited inputs do not generate physical line breaks in the final event, mitigating log injection risks.

### Fuzzing Validation
The fuzz target `FuzzPhase10RecursiveRedactionAuditSensitiveKeys` was executed for 62 seconds, validating random key and builder permutations without observing syntax corruptions or unexpected panics:
```text
PASS
ok  	github.com/humanjuan/acacia/v2/test	62.650s
```

---

## 6. Diagnostic Benchmark Summary
Performance characterization collected with 10 repetitions in an external disk environment:

### Redaction Performance in Structured Builders
| Benchmark | Observed Range (ns/op) | Allocations (allocs/op) |
| :--- | :---: | :---: |
| **Flat Builder (Str Redactado)** | ~500 - 700 ns/op | 1 alloc/op |
| **Object Builder (Redactado)** | ~630 - 810 ns/op | 2 allocs/op |
| **Array Builder (Elemento Redactado)** | ~600 - 820 ns/op | 3 allocs/op |
| **Namespace (Redactado)** | ~520 - 660 ns/op | 2 allocs/op |
| **RawJSON (Externa Redactada)** | ~460 - 640 ns/op | 1 alloc/op |
| **Any Fallback (Mapa Redactado)** | ~6100 - 9450 ns/op | 45 allocs/op |

*Note: Diagnostic benchmark results confirm the cost of Any due to the recursive decoding and re-marshaling flow, whereas direct structured builders operate with highly optimized allocations.*

---

## 7. Diagnostic Regression of Builders
Behavior was compared with consolidated builders (sprints 10.1 to 10.6). Functional suites passed; external disk benchmarks remain as diagnostic evidence, not as a final performance conclusion for previous flows.

---

## 8. Risks and Decisions
* **No SSD Hard Validation**: Characterizations were performed on external development storage. Final release metrics must be validated on local production SSD hardware.
* **No New APIs**: The rule of not modifying API signatures or adding public methods was respected.
* **RawJSON Trusted Input**: The design boundary is formally documented: payloads processed via RawJSON are not internally inspected for performance reasons.
