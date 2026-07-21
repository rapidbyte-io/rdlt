<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan:
`specs/008-postgres-dest-completion/plan.md` (feature: Postgres
destination completion — pure-relocation modularization of dest/ into
config/ddl/encode/commit modules FIRST; native NUMERIC(p,s)/JSONB/UUID
+ NOT NULL type fidelity via hand-rolled wire encoders mirroring the
source's decoders and capability flips (decimal, json_type — engine
lowering is capability-driven, zero engine changes); destination-side
merge strategies: upsert with auto-ensured unique index, hard-delete
column, SCD2 with IS-DISTINCT-FROM change detection and D3-receipt
redelivery stability; supporting indexes for merge identities with a
measured scoreboard entry; review-F6 error chains (server message +
SQLSTATE); contracts: dest-types, merge-strategies, scd2. Zero
rdlt-core/rdlt-connector changes — WriteMode frozen). Features 005/006/
007 (`specs/00{5,6,7}-*/`) are the merged base being extended. Feature
004's benchmark governance (`specs/004-close-perf-misses/` — gated vs
scoreboard rows, measurement-first bars, version-policy entries) and
feature 003's hardening nets (`specs/003-hardening-performance/` —
crash sweep, mutation/fuzz/property suites, blocking perf gate) remain
in force. The established architecture is feature 001:
`specs/001-rdlt-ingestion-engine/plan.md` and its contracts (as amended
by features 002 and 006) remain authoritative; the approved technical
design is `2026-07-18-rdlt-engine-design.md` at the repo root. Run
tests with `cargo nextest run` (doc-tests: `cargo test --doc`).
<!-- SPECKIT END -->
