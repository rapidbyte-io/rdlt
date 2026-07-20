<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan:
`specs/005-postgres-source/plan.md` (feature: Postgres SQL source connector —
snapshot + cursor-column incremental streamed as typed Arrow batches via
binary COPY decoding, postgres→duckdb/postgres benchmark cells with
measurement-first bars, crash-sweep robustness; spec, research, data model,
and the source-config + type-mapping contracts live alongside it). Feature
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
