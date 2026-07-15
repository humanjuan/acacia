# Documentation and Examples Report

## 1. Goal and Scope
The goal of this review is to provide reference guides and runnable code examples to help developers adopt the structured JSON features of Acacia Logger.
* **Guides Created:**
  * `docs/guides/structured-json-builder.md`: Practical reference for the builder APIs.
  * `docs/guides/structured-json-redaction.md`: Redaction keyword logic, normalization rules, and safety limits.
  * `docs/guides/structured-json-migration.md`: Steps to transition legacy dynamic maps to typed event builders.
* **Code Examples:** Created four runnable code examples under `examples/`.
* **Claims Sanitization:** Performance claims in public files were reviewed. Metrics are described as diagnostic observations from local benchmark runs rather than absolute guarantees.

No functional changes or signature modifications were made to the logger core (`acacia.go`). No OpenTelemetry adapter is added to the core module.

---

## 2. Modified and Created Files
* **Modified:**
  * `README.md`: Added structured JSON quick-start section and technical guide links.
  * `CHANGELOG.md`: Added structured JSON and redaction updates under the `[Unreleased]` section.
  * `RELEASE_NOTES.md`: Documented structured JSON features for the upcoming release candidate.
  * `docs/README.md`: Indexed the new guides and updated the current status.
  * `docs/MIGRATION_STRATEGY.md`: Added sections on migrating legacy maps to structured builders.
  * `docs/plans/structured-json-api-contract.md`: Documented the current API surface (implemented vs deferred).
  * `docs/reports/README.md`: Added link to this report.
* **Created:**
  * `docs/guides/structured-json-builder.md`
  * `docs/guides/structured-json-redaction.md`
  * `docs/guides/structured-json-migration.md`
  * `examples/structured-json-basic/main.go`
  * `examples/structured-json-nested/main.go`
  * `examples/structured-json-redaction/main.go`
  * `examples/structured-json-rawjson-any/main.go`
  * `docs/reports/structured-json-documentation-examples-report.md`

---

## 3. Guide Summaries
* **Structured JSON Builder Guide:** Reference for event builders (`InfoEvent()`), flat fields (`Str`, `Int`), nested structures (`Object`, `Array`), and prefixes/scopes (`Group`, `Namespace`).
* **Redaction Policy Guide:** Specifies the sensitive keys matrix. Details the rules for raw bytes (`RawJSON`, trusted input only) and dynamic fallback (`Any`, recursive best-effort).
* **Migration Strategy Guide:** Step-by-step instructions to replace reflective map logging with event builders.

---

## 4. Runnable Examples
Four self-contained examples were added under `examples/`:
1. `structured-json-basic`: Shows setup, flat typed builder fields, and safe logger close.
2. `structured-json-nested`: Shows nested objects (`Object`), array items (`Array`), nested groups (`Group`), and flat prefixed keys (`Namespace`).
3. `structured-json-redaction`: Practical guide on auto-redaction key names, explicit redaction (`Secret`, `StrRedacted`), and RawJSON/Any boundaries.
4. `structured-json-rawjson-any`: Shows how to inject pre-serialized JSON (`RawJSON`), carriage return escaping, and the dynamic fallback (`Any`) for un-structured types.

---

## 5. Verification Results

### Running Unit and Concurrency Tests
The full test suite was run under the Go race detector:
```text
$ go test ./... && go test -race ./...
ok  	github.com/humanjuan/acacia/v2/test	16.829s
ok  	github.com/humanjuan/acacia/v2/test	32.875s
```
All correctness, safety, and regression tests passed successfully.

### Compiling and Running Examples
Each code example was run locally to verify compilation:
```bash
go run ./examples/structured-json-basic
go run ./examples/structured-json-nested
go run ./examples/structured-json-redaction
go run ./examples/structured-json-rawjson-any
```
*Workspace Cleanup:* All temporary log files generated during local execution were deleted.

---

## 6. Risks and Decisions
* **Diagnostic Performance:** Performance metrics are explicitly marked as local observations on development hardware. They do not replace release candidate validation on SSD drives under production-like workloads.
* **No New Core APIs:** Confirmed that no core API signatures or public methods were altered in `acacia.go`.
