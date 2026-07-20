<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan:
`specs/007-postgres-source-completion/plan.md` (feature: Postgres source
completion, pre-CDC — mutual TLS client credentials through the shared
tls module for BOTH postgres connectors, cursor lag/attribution window
riding keyed Merge for exact totals, libpq conn-string TLS-parameter
translation with named rejections, NULL-cursor error policy, inclusive
end bound, default application_name, pg_inherits discovery filter with
an explicit-listing override; contracts: tls-client-auth, cursor-lag,
connstring-portability; two recorded spec amendments in research.md
R4/R7). Feature 006 (`specs/006-postgres-completeness/` — merged
rdlt-postgres crate, sslmode matrix, type hints, query streams, keyed
structured merge via the B4 amendment, lossy tracing, generated config
schemas) and feature 005 (`specs/005-postgres-source/`) are the base
being extended. Feature 004's benchmark governance
(`specs/004-close-perf-misses/` — gated vs scoreboard rows,
measurement-first bars, version-policy entries) and feature 003's
hardening nets (`specs/003-hardening-performance/` — crash sweep,
mutation/fuzz/property suites, blocking perf gate) remain in force. The
established architecture is feature 001:
`specs/001-rdlt-ingestion-engine/plan.md` and its contracts (as amended
by features 002 and 006) remain authoritative; the approved technical
design is `2026-07-18-rdlt-engine-design.md` at the repo root. Run
tests with `cargo nextest run` (doc-tests: `cargo test --doc`).
<!-- SPECKIT END -->
