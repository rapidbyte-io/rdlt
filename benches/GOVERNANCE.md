# Benchmark governance records

Coverage, semver, classified-exclusion, and perf-gate records — relocated here
from `benches/RESULTS.md` in the feature-018 matrix rebuild (archive commit
`40841ab`) so RESULTS.md carries only the measured matrix, its caveats, trends,
and milestones. The records below are preserved verbatim under dated headings;
each is the recorded governance evidence for its feature.

## Perf-regression gate (feature 003, G1)

Instruction-count baselines for the hot paths live in
`benches/perf-baselines.json` (iai-callgrind; >3% regression blocks CI;
cross-toolchain comparisons refused — re-record deliberately). Recorded
2026-07-20: shred (tape) 362 M instructions / 10k nested rows; passthrough
602 k; identity keyed/keyless 20.5 M / 29.3 M.

## Feature 011 — connector-crate coverage (2026-07-21)

Connector-crate coverage (feature 011, contract PM5; recorded numbers
measured with `cargo llvm-cov nextest -p rdlt-connector-postgres --features
failpoints`, cargo-llvm-cov 0.8.7; floor: 80% lines for the CONNECTOR
crate; NOT a CI gate. Note: `make coverage` was widened post-merge by
the owner to the whole workspace — its total is a DIFFERENT, lower
number; re-run with `-p rdlt-connector-postgres` to reproduce the recorded
figures):

| Measurement | Lines | Functions | Date |
|---|---|---|---|
| Baseline (before feature-011 cells) | 87.69% / 87.71% (two runs, stable) | 83.17% | 2026-07-21 |
| Final (feature 011 close) | **88.98%** | 83.23% | 2026-07-21 |

Feature-011 delta: types.rs 76.88% → 91.59% (the hint-matrix cell),
encode.rs → 87.46%, cursor.rs → 85.42%; 13 new behavioral cells + the
R5 fix. Classified exclusions (verified via `--show-missing-lines`,
each a REAL uncovered cluster with a stated reason — contract PM5):

| Cluster | Lines | Reason |
|---|---|---|
| source/mod.rs 82–262 | ~168 | the `testhook` module (bench_wire/bench_decode/fuzz entries) executes only under benches and fuzz targets, outside nextest — instrumentation surface, not product paths |
| source/mod.rs scattered (368–372, 515–517, 566–568, 593–617, 652–658, …) | ~30 | defensive engine-contract guards (e.g. stream-without-reflected-table) unreachable through the engine, plus thin `PostgresSource::from_json/from_value` delegators whose shared validation path is covered at the config layer |
| dest/mod.rs (51–59, 75, 89–91) | 13 | capability/edge helper arms |
| tls_verify.rs (52–59, 116–123) | 16 | verifier trait methods for protocol variants the TLS matrix's handshakes never negotiate (TLS 1.2 signature arms under a TLS 1.3 stack) |

(Region coverage at close: 88.27%.)

## Feature 013 — DuckDB coverage (2026-07-22)

Feature-013 coverage record (011 protocol, `cargo llvm-cov nextest -p
rdlt-connector-duckdb --features failpoints`): baseline BEFORE the
feature's cells 81.95% lines (single-file crate, 6 tests); final
**87.5% lines** across the restructured crate (commit.rs 90.27%,
dialect.rs 100%, mod.rs 81.20%) — floor ≥80% met. Classified
exclusions: mod.rs helper error arms (connection-poisoned paths,
memory_limit error mapping) and rarely-hit sql_type arms (Time/Binary
in stage DDL) — defensive/administrative surface, no product path.

## Feature 014 — REST coverage (2026-07-22)

Feature-014 coverage record (011 protocol, `cargo llvm-cov nextest -p
rdlt-connector-rest`, default features — the live PokeAPI and
failpoints-sweep cells run separately and add on top): baseline BEFORE
the feature 53.92% lines; final **90.54% lines** across the rebuilt
crate (after the post-review hardening pass: typed action matching,
link-header rewrite, empty-wildcard termination, tagged-auth compat)
— floor ≥80% met.
Classified exclusions (verified via `--show-missing-lines`, each a
real uncovered cluster with a stated reason): source/mod.rs thin
`RestSource::from_json`/`from_value` delegators (the shared validation
path is covered at the config layer) and spec/type-hint wiring arms
that run only under engine discovery; config.rs 314–326 — the
`HintType → LogicalType` administrative mapping table (one arm per
variant, timestamp_tz arm covered); client/auth.rs concurrent-401
guard, an `unreachable!`, and rare token-response arms (audience push,
missing `access_token`); read/* per-family defensive error arms
(malformed header values, non-object child records, scalar rendering
variants); client/secret.rs `From` convenience conversions.

## Feature 015 — file coverage + semver (2026-07-22)

Feature-015 coverage record (011 protocol, `cargo llvm-cov nextest -p
rdlt-connector-file`, container cells running): baselines BEFORE the
feature 73.25% lines (rdlt-connector-file) / 90.80%
(rdlt-connector-parquet, its 87 instrumented lines); final **86.56%
lines** across the unified crate (dest/config 100%, dest/mod 93.12%,
location/{mod,s3,secret} 89.91/81.07/91.43%, formats/{csv,jsonl,mod,
parquet} 84.39/85.71/85.33/80.60%, source/{config,cursor,mod}
75.36/86.58/85.00%) — floor >=80% met. Classified exclusions:
source/config.rs HintType→LogicalType administrative mapping arms +
rarely-exercised validation arms of pre-015 spellings; formats/* and
location/s3 defensive IO-error arms (interrupted-read retries, codec
error paths beyond the matched cells); dest/mod count_rows helper arms
— defensive/administrative surface, no product path.
Semver (015): cargo-semver-checks vs main reports major on
rdlt-connector-file (struct fields added to the cursor data-model
types, FileProgress lost Copy, Format gained Csv) — ALL covered by the
STANDING recorded 014 major (0.2 → 0.3 at next publish; the parquet
crate deletion rides it too). The config vocabulary (Format,
FileConfig, FileStream, HintType, the dest/location/csv types) is now
#[non_exhaustive] so future growth is additive; the cursor plumbing
types stay exhaustive by choice (per-crate data model). The facade
reports "no semver update required" under default features.

## Feature 016 — Iceberg coverage + semver (2026-07-22)

Feature-016 coverage record (011 protocol, `cargo llvm-cov nextest -p
rdlt-connector-iceberg --features failpoints`, container cells running):
new crate — no prior baseline; final **85.08% lines** (config 89.47%,
schema 90.42%, errors 97.37%, commit 81.85%, dest 73.49%) — floor >=80%
met. Classified exclusions: dest.rs align/cast defensive arms and the
write-before-ensure + reserved-name guards (typed error paths whose
triggers need a hostile embedder, not the engine); commit.rs
classify-context formatting arms and the conflict-retry branches beyond
the mocked counts; connect() catalog-props escape-hatch permutations —
defensive/administrative surface, no product path.
Semver (016): rdlt-connector-iceberg is NEW this feature (no baseline);
`cargo semver-checks -p rdlt --baseline-rev main` on the facade reports
"no semver update required" (the `iceberg` feature + re-export are
additive). Bench-harness fixture fields (`reset_sh`, `teardown_sh`) are
additive serde-defaulted TOML surface. The standing 014 major
(0.2 → 0.3 at next publish) is unaffected.

## Feature 019 — performance improvements: semver outcome (2026-07-25)

Semver (019): **the 0.2 → 0.3 window recorded by feature 014 STAYS CLOSED.**
`cargo semver-checks check-release --baseline-rev main -p rdlt-core
-p rdlt-connector` reports **"no semver update required"** for both seam
crates — 196 checks pass each.

That outcome is worth stating rather than assuming, because of how much moved
underneath it. This feature replaced the entire Postgres binary-COPY encoder,
changed the publish protocol so full loads no longer stage, rewrote the WAL
segment format, and reworked the shred identity path — and none of it reached
the SPI. `LoadSession` is unchanged. The one addition, `ParquetOptions` and
`ParquetCompression` in `rdlt-connector::output`, is a new module plus
re-exports: additive, and additive is minor.

An unexercised version window is a RESULT, not an omission. The window was
available for the whole feature and nothing needed it, which is the evidence
that the SPI boundary held while the implementations behind it were replaced.

Persisted-format changes did occur and are versioned in their own right, not
through semver: the WAL segment format 1 → 2 (exact-match gate, refuses both
directions) and the bench artifact 2 → 3 (refuses v2 with its reason). Neither
is public API.

## Feature 045 — the post-split coverage baseline (2026-08-13)

The first coverage figure measured on the post-split 13-crate tree
(`make coverage` = `cargo llvm-cov nextest --features failpoints`,
workspace-wide, 625/625 tests passing): **81.50% lines** (functions
80.46%, regions 81.42%) — the recorded 80% line floor is met.

Read it against its own denominator, not the 87%-era records: the
pre-split figures (023: 87.22%, 024: 87.25%, 020: 85.64%) were
measured over a tree that still carried the first-party connector
crates and their suites, which left for the rdlt-connectors repo in
the feature-044 cut — crates and suites moved together, so the drop
is the denominator changing, not tests disappearing. This entry is
the baseline the post-split tree is read against until a later
feature re-records it.

## The 0.2 → 0.3 window — standing as of feature 020

The window recorded by feature 014 is still the next publish, and it stayed
**closed** through 019: `cargo semver-checks` reported "no semver update
required" on both seam crates, 196 checks passing each, and the one addition
(`ParquetOptions` / `ParquetCompression` in `rdlt-connector::output`) is
additive.

**Feature 020 opens it, deliberately and once.** US5 removes
`StateDoc.schema_hashes` — a public field of a public type in the
semver-sacred `rdlt-core` — because it is written at every commit, read
nowhere, and being a digest it can only ever prove inequality, which is the
false-positive trap the cross-run schema contract has to avoid. Deleting it is
what closes the contract; keeping it beside its replacement would violate the
greenfield rule.

So the bump at publish is no longer merely the standing publish-time major
from 014/015: it is **required**, and 020's close-out records it with the
local `cargo semver-checks` output that flags it. Nothing is published yet, so
no consumer is broken by the break — which is precisely why taking it now is
cheaper than taking it later.
