<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan:
`specs/009-postgres-cdc/plan.md` (feature: Postgres CDC via logical
replication — SQL-level decoding over the ordinary connection path
(released tokio-postgres has NO replication protocol; verified):
peek-don't-consume passes per table over a pinned LSN range through the
frozen SPI, slot-first snapshot with a CONVERGENT overlap boundary
(recorded spec refinement 1), hand-rolled+fuzzed pgoutput parser,
ack = min committed cursor once per run AFTER destination commit,
deletes via the 008 hard-delete composition, chunked-loop tail mode
(recorded refinement 2), REPLICA-IDENTITY/TOAST/slot-lifecycle
distinguished errors, four new fail points swept; contracts:
cdc-protocol, cdc-config, cdc-operability. Zero rdlt-core/
rdlt-connector changes; zero new dependencies). Features 005–008
(`specs/00{5,6,7,8}-*/`) are the merged base being composed. Feature
004's benchmark governance (`specs/004-close-perf-misses/`) and 003's
hardening nets (`specs/003-hardening-performance/`) remain in force.
The established architecture is feature 001:
`specs/001-rdlt-ingestion-engine/plan.md` and its contracts (as amended
by features 002 and 006) remain authoritative; the approved technical
design is `2026-07-18-rdlt-engine-design.md` at the repo root. Run
tests with `cargo nextest run` (doc-tests: `cargo test --doc`).
<!-- SPECKIT END -->
