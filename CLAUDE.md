<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan:
`specs/017-workspace-refactoring/plan.md` (feature: workspace refactoring
program executing REFACTORING.md end-to-end — fix 12 latent defects
B1-B12 with red-before/green-after regression pins, then cross-cutting
refactors R1-R13 + delivery-surface items D1-D15 as ~12 independently
mergeable increments in value-per-risk order (Part 4 + Part 5 folded
in). Constitution v1.0.0 ratified (.specify/memory/constitution.md) —
this feature enforces Principles V (typed taxonomy, no citation IDs in
user-facing strings; substring-matching rendered errors FORBIDDEN) and
VI (self-contained comments). Key decisions (research.md): B5 duckdb
classification via structured code/extended_code (probe-pinned); B6
iceberg via status context value (probe + designed fallback);
DestError::RateLimited is ADDITIVE (#[non_exhaustive] verified); one
Secret in rdlt-connector::secret behind new SPI `schema` feature; R2
commit protocol = pure sqlcore planner commit_script->Vec<Step>,
destinations execute (golden pins prove SQL-identical); R6 shared
apply_delta/apply_batch used by Loader + two-pass WAL replay (B10);
R7 one file Location abstraction w/ read+write halves + one
keys_of_table ownership helper (closes B2/B9); D1-D5 testkit
containers module (runtime_available() superset probe, PgFixture
Option-returning skip-not-fail) + fixtures module (batch_of/
schema_for/meta_for); breaking renames = deprecated aliases or NAMED
deferrals to the recorded 0.2->0.3 window — window NOT opened here.
Behavior changes CONFINED to defect fixes + classification
corrections; persisted formats/golden pins byte-identical (WR1);
close-out matrix zero uncited dispositions (WR7); full gate green at
EVERY increment merge (WR8). Contract:
contracts/workspace-refactoring.md WR1-WR8).
Previous feature 016 for reference:
`specs/016-iceberg-dest/plan.md` (feature: provider-agnostic Iceberg
REST-catalog DESTINATION — new THIN crate rdlt-connector-iceberg
(facade rdlt::connector::iceberg, CLI destination: iceberg:) wrapping
Apache iceberg-rust at ONE boundary (errors.rs + commit.rs — library
types never cross the public surface; duckdb-rs wrapping precedent).
SURVEY RESOLVED AT PLAN TIME with registry facts: iceberg 0.10.0 +
iceberg-catalog-rest 0.10.0 + iceberg-storage-opendal 0.10.0
(opendal-s3) — arrow ^58/parquet ^58 match the workspace pin (single
arrow 58.4 tree proven by live cargo-tree probe); rustc floor 1.94 ok.
NOT taken: iceberg-catalog-glue (aws-sdk smithy tree; Glue/SigV4 is
PHASE-2, recorded); rdlt-connector-rest NOT a dep; file-crate
location/ NOT extracted (config VOCABULARY shared — family S3
spelling + Secret — plumbing not). Exactly-once = snapshot-native D3:
commit identity (rdlt.pipeline/load-id/commit-seq) in snapshot
SUMMARY properties, replay detected from snapshot history, StateDoc
in table property rdlt.state updated in the same atomic commit;
bounded conflict retry (4 attempts, refresh->rebuild->commit,
exhaustion typed naming table+competing snapshot). Closed type
mapping (Json->string documented; field IDs library-assigned only);
additive drift = UpdateSchema add-nullable-column. Write modes:
Append (fast-append) + Replace (overwrite once-per-load, durable
guard from snapshot history); T001 PROBES overwrite support in 0.10 —
fallback DESIGNED: v1 narrows to Append with Replace
typed-unsupported, recorded never silent (ID5). Auth v1:
oauth2_client_credentials + bearer (Secret-wrapped, grep-proof);
credential VENDING default (X-Iceberg-Access-Delegation, session
tokens; expiry = transient), family-S3 storage override explicit.
Tests: Polaris container + 015 RUSTFS container canonical leg
(testcontainers skip-not-fail, images/env VERIFIED at T001 like 015);
UC OSS candidate bearer leg gate-verified; pyiceberg read-back venv in
the standard gate (competitors-harness pattern), Spark read-back DEEP
tier only. Crash points ice.files.write/ice.commit/
ice.receipt.visible swept live x3 actions with duplicate-free
snapshot-history pins. Bench: iceberg-polaris-200k SCOREBOARD (never
gated). Verification: matrix zero uncited, parity vs dlt iceberg w/
deferrals named, >=80% coverage baseline-first, README, quickstart.
Contract: contracts/iceberg-dest.md ID1-ID8).
Previous feature 015 for reference:
`specs/015-file-completeness/plan.md` (file family unified —
rdlt-connector-parquet absorbed into rdlt-connector-file
(src/{source,dest}/ + shared location/ + formats/; ParquetDir frozen
alias); Location = Local | S3 via object_store; one cursor rulebook
incl. TAIL-HASH resume integrity; CSV record format w/ JOIN lattice;
gzip/zstd whole-file units; dest parquet+jsonl both kinds w/
partition_by + ownership-precise Replace truncation; RUSTFS container
cells; contract file-family.md FF1-FF8).
Previous feature 014 for reference:
`specs/014-rest-completeness/plan.md` (REST source completeness —
client/ (OAuth2 single-flight, Secret, bounded Retry-After), read/
(Paginator trait + 7 families, TYPED response-action matching,
parent-child fan-out), additive config incl. tagged-YAML compat;
contract rest-source.md RS1-RS8; 014 recorded the one-time semver
MAJOR — 0.2→0.3 at next publish, config enums #[non_exhaustive]).
Previous feature 013 for reference:
`specs/013-duckdb-completeness/plan.md` (rdlt-connector-sqlcore shared
merge core behind golden-SQL pins; duckdb full dlt parity; contract
shared-merge-core.md SM1-SM8).
Previous feature 012 for reference: `specs/012-bench-harness/plan.md`
(crates/rdlt-bench declarative TOML cells, bars.toml enforced by
rdlt-bench gate, generated RESULTS.md tables; new cells are scoreboard
unless 004 governance grants a bar; contract bench-harness.md
BH1-BH8; 015 added the generic Container fixture kind).
Features 005-011 (`specs/0{05,06,07,08,09,10,11}-*/`) are the merged
base being composed; 004's benchmark governance and 003's hardening
nets remain in force. The established architecture is feature 001:
`specs/001-rdlt-ingestion-engine/plan.md` and its contracts (as
amended by features 002 and 006) remain authoritative; the approved
technical design is `2026-07-18-rdlt-engine-design.md` at the repo root.
Run tests with `cargo nextest run` (doc-tests: `cargo test --doc`).
<!-- SPECKIT END -->
