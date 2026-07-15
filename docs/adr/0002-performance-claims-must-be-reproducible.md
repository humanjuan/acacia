# ADR 0002: Reproducible Performance Claims

## Status
Accepted

## Context
Acacia's performance is one of its primary attributes. However, in software development, it is easy to publish inaccurate performance statements that erode user trust. In structured logging, builder APIs can generate dynamic allocations depending on nesting, which conflicts with general "zero allocation" claims.

## Decision
To ensure the accuracy of all publicly reported performance metrics:
1. **Reproducible Commands:** Any performance claim in the documentation or README must include a reproducible `go test` command.
2. **Contention Testing with `GOMAXPROCS=1`:** Benchmarks must be validated under `GOMAXPROCS=1` limits to reveal synchronization bottlenecks and garbage collection pressure.
3. **Write Path Classification:** General "zero allocation" claims that cover the entire library are not allowed. Benchmarks and reports must separate results for:
   * Direct paths (String / []byte)
   * Builder paths (JSON Builders)
   * Legacy paths (Map-based compatibility)
   * Redaction paths (Sanitization overhead)
4. **Transparent Comparisons:** Comparisons against other loggers (e.g. Zap, Zerolog) will not be published unless the exact test methodology and benchmark source code are provided.

## Consequences
* **Positive:** Improved technical transparency, elimination of false performance assumptions, and data-driven development.
* **Negative:** More developer effort is needed to document and run multiple test configurations and benchmark variations.
