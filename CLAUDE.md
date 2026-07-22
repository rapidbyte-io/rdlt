<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan:
`specs/013-duckdb-completeness/plan.md` (feature: duckdb destination
completeness — extract the postgres dest's merge layer into a NEW
internal crate `rdlt-connector-sqlcore` (options vocabulary +
validation + MergePlan shapes + single-unit rules) behind a
MergeDialect trait that owns SQL TEXT ONLY; extraction proven by
golden-SQL pins captured BEFORE the refactor (byte-identical after) +
untouched postgres suites/sweeps/gated bars (contract SM4). DuckDB
gets the full 008/010 options vocabulary (merge_strategy delete_insert/
upsert/scd2, hard_delete, dedup_sort, merge_key, scd2 block) with
identical typed errors; probe-first dialect arms (DISTINCT ON,
ON CONFLICT vs auto unique index, tx-stable now() scd2 boundary,
bundled JSON extension) — a failed probe becomes a TYPED capability
gap, never an approximation (SM3). json_type flips to native JSON via
the stage→target SQL seam. Verification to the 011 standard: matrix.md
zero uncited rows, ≥80% measured coverage baseline-first, armed crash
sweeps over new arms, dlt-1.29.0 parity record, PLUS the
cross-destination differential oracle in crates/rdlt/tests/
(identical feeds → equivalent outcomes both dests). Two scoreboard
bench cells via the 012 harness; zero SPI change, zero new external
runtime deps, WriteMode frozen. Contract:
contracts/shared-merge-core.md SM1-SM8).
Previous feature 012 for reference: `specs/012-bench-harness/plan.md`
(benchmark framework — `crates/rdlt-bench` clap CLI runs declarative
TOML cells (benches/cells/), bars.toml enforced by `rdlt-bench gate`,
committed fingerprinted artifacts, RESULTS.md tables generated between
markers; new bench cells are declared as data, scoreboard unless the
004 governance grants a bar; contract: contracts/bench-harness.md
BH1-BH8).
Previous feature 011 for reference:
`specs/011-connector-verification/plan.md` (postgres connector
verification — traceability matrix PM1-PM8, 88.98% measured line
coverage for rdlt-connector-postgres, R5 typed rejection of explicit
merge_strategy under non-merge modes). Features 005-010
(`specs/0{05,06,07,08,09,10}-*/`) are the merged base being composed;
004's benchmark governance and 003's hardening nets remain in force. The established architecture is feature
001: `specs/001-rdlt-ingestion-engine/plan.md` and its contracts (as
amended by features 002 and 006) remain authoritative; the approved
technical design is `2026-07-18-rdlt-engine-design.md` at the repo root.
Run tests with `cargo nextest run` (doc-tests: `cargo test --doc`).
<!-- SPECKIT END -->
