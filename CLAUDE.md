<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan:
`specs/015-file-completeness/plan.md` (feature: filesystem/object-store
completeness — MERGE rdlt-connector-parquet INTO rdlt-connector-file
(parquet is a FORMAT; crates are named by system) with the family
layout: src/{source,dest}/ + shared crate-root location/ and formats/
modules, thin lib.rs façade (rdlt::connector::file). The weld is
moves-only behind the behavior-preservation net: ALL pre-015 cells
green unchanged, gated parquet-passthrough + jsonl-duckdb-200k bars
in-band same-session, pipeline-YAML spellings frozen (`file:` source,
`parquet:` destination), persisted cursor/staging/receipt formats
(CURSOR_FORMAT_VERSION 1, LAYOUT_FORMAT_VERSION 1, pq.* fail-point
names) byte-compatible. Then completeness: Location = Local |
S3-compatible (object_store crate `aws` feature — the ONE new external
dep, surveyed R1; csv/flate2/zstd already in-tree, promoted to direct
deps); discovery deterministic + COMPLETE across listing pagination
(complete-or-fail); per-file cursors extend to (size, etag) identity
with the same loud-failure rules; CSV = RECORD format via NDJSON
conversion (inference lattice bool→int64→float64→utf8, type_hints
override, typed errors naming file+row+column); gzip/zstd by extension
(whole-file incremental units, magic-byte mismatch typed); dest writes
parquet AND jsonl to either location kind, optional partition_by,
staged-key → COPY+DELETE finalize at commit (readers never observe a
partial object; local rename protocol byte-identical). Live cells:
RUSTFS container (Apache-2.0 S3 server; image/env verified at the
environment-gate task) via podman shim, skip-not-fail; pagination cell
seeds >1000 keys. Crash points: pq.* preserved + file.list/read/
stage.put/finalize.copy/finalize.delete, swept both location kinds.
Verification: matrix.md zero uncited, dlt-parity vs dlt filesystem
source/dest, ≥80% coverage baseline-first, file-s3-duckdb-200k
SCOREBOARD cell (never gated — that floor measures the test server),
comprehensive README, quickstart walked. Contract:
contracts/file-family.md FF1-FF8).
Previous feature 014 for reference:
`specs/014-rest-completeness/plan.md` (REST source completeness —
family layout, client/ (auth incl. OAuth2 single-flight, Secret
redaction, bounded Retry-After), read/ (Paginator trait + 7 families
w/ loop guards, JSONPath-subset extraction, TYPED response-action
matching status+content over the S3 posture, incremental block,
parent-child w/ max_concurrency fan-out), additive config incl. the
pre-014 `auth: !bearer` tagged-YAML compat; contract rest-source.md
RS1-RS8; NOTE: 014 recorded the one-time semver MAJOR — config enums
are #[non_exhaustive]; 0.2→0.3 at next publish covers 015's crate
removal too).
Previous feature 013 for reference:
`specs/013-duckdb-completeness/plan.md` (duckdb completeness —
rdlt-connector-sqlcore shared merge core behind golden-SQL pins,
MergeDialect owns SQL text only, duckdb at full dlt parity; contract
shared-merge-core.md SM1-SM8).
Previous feature 012 for reference: `specs/012-bench-harness/plan.md`
(benchmark framework — `crates/rdlt-bench` clap CLI runs declarative
TOML cells (benches/cells/), bars.toml enforced by `rdlt-bench gate`,
committed fingerprinted artifacts, RESULTS.md tables generated between
markers; new bench cells are declared as data, scoreboard unless the
004 governance grants a bar; contract: contracts/bench-harness.md
BH1-BH8).
Features 005-011 (`specs/0{05,06,07,08,09,10,11}-*/`) are the merged
base being composed; 004's benchmark governance and 003's hardening
nets remain in force. The established architecture is feature 001:
`specs/001-rdlt-ingestion-engine/plan.md` and its contracts (as
amended by features 002 and 006) remain authoritative; the approved
technical design is `2026-07-18-rdlt-engine-design.md` at the repo root.
Run tests with `cargo nextest run` (doc-tests: `cargo test --doc`).
<!-- SPECKIT END -->
