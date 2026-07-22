<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan:
`specs/014-rest-completeness/plan.md` (feature: REST source
completeness — restructure rdlt-connector-rest to the family layout
(src/source/ + thin lib.rs façade w/ root re-exports, like postgres/
duckdb) holding three PUBLIC layers: client/ (auth incl. OAuth2 client-credentials w/ single-flight
refresh + Secret-redacted fields, classify, bounded Retry-After,
pacing), read/ (Paginator trait + 7 config families page/offset/
cursor/header_cursor/next_url/link_header/none with same-request +
max_pages loop guards — NO auto-detection, deliberate anti-guessing;
JSONPath-subset extraction dot+[*]+[N] hand-rolled; incremental
{cursor_field,start_param,end_param}; parent-child placeholder
resolution), config.rs (ADDITIVE — old spellings frozen as aliases).
Response actions = declared allow-lists {status/content_contains ->
ignore|end_stream|error} over the unchanged S3 typed-error posture
(engine owns retries). ZERO new external deps (OAuth2 = one POST,
hand-rolled Link-header/JSONPath subsets per 009 survey rule). Crash
points rest.request/decode/checkpoint join the engine sweep with
armed-fire pins. Existing conformance cells must stay green through
the rewrite (behavior-preservation net). Verification: matrix.md, ≥80%
coverage baseline-first, wiremock conformance per pagination×auth×
action, dlt-parity record, RDLT_NET=1-gated PokeAPI live cell
(structural asserts, 100ms pacing), gated REST→PG ≥5x bar re-measured;
composed example connector in-crate proves the US3 seam (public pieces
only, no raw reqwest). Contract: contracts/rest-source.md RS1-RS8).
Previous feature 013 for reference:
`specs/013-duckdb-completeness/plan.md` (duckdb completeness —
rdlt-connector-sqlcore shared merge core behind golden-SQL pins,
MergeDialect owns SQL text only, duckdb at full dlt parity incl. scoped
scd2 retirement + markers + extensions/settings; contract
shared-merge-core.md SM1-SM8).
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
