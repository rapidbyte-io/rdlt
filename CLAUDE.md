<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan:
`specs/006-postgres-completeness/plan.md` (feature: Postgres completeness —
TLS full sslmode matrix via a shared rdlt-pg-tls crate for BOTH postgres
connectors, per-column type hints, describe-schema query streams, merge for
keyed structured streams via the recorded B4 amendment, lossy-mapping
visibility, generated config schemas, test-integrity closures; contracts:
tls-policy, type-hints, query-streams, merge-structured). Feature 005's
connector and its contracts (`specs/005-postgres-source/`) are the base
being extended. Feature
004's benchmark governance (`specs/004-close-perf-misses/` — gated vs
scoreboard rows, measurement-first bars, version-policy entries) and feature
003's hardening nets (`specs/003-hardening-performance/` — crash sweep,
mutation/fuzz/property suites, blocking perf gate) remain in force. The
established architecture is feature 001:
`specs/001-rdlt-ingestion-engine/plan.md` and its contracts (as amended by
feature 002) remain authoritative; the approved technical design is
`2026-07-18-rdlt-engine-design.md` at the repo root. Run tests with
`cargo nextest run` (doc-tests: `cargo test --doc`).
<!-- SPECKIT END -->
