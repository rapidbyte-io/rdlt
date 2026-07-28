<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan:
`specs/022-snowflake-dest/plan.md` (feature: Snowflake destination
connector — PLANNED on branch `022-snowflake-dest`, spec + plan + research
committed, tasks NOT yet generated. Feature 021 is RESERVED for the publish
feature and does not exist yet. New THIN crate rdlt-connector-snowflake
(facade rdlt::connector::snowflake, feature `snowflake`, CLI
`destination: snowflake:`) — the THIRD SQL destination. Survey RESOLVED at
plan time with registry facts + a LIVE probe against the qual account
(credentials local-only: env RDLT_SNOWFLAKE_* or ~/.config/rdlt/snowflake/
incl. `passphrase` file — the key is an ENCRYPTED p8; account identity is
deliberately in NO committed file, SC-005 verifies mechanically). DRIVER:
both crates REJECTED (snowflake-api 0.14: arrow ^57 vs workspace 58;
snowflake-connector-rs 1.1: no PUT/stage API so the ingestion path is
unreachable, reqwest 0.13 second major) — HAND-ROLL a thin session-protocol
client at ONE boundary over workspace reqwest 0.12 + jsonwebtoken-over-ring
(fallback recorded: SQL API v2 + batched INSERT, typed narrowing,
escalated). PROVEN LIVE: key-pair JWT auth end-to-end (SF 10.26.101);
unquoted idents fold UPPER and `EVENTS`/`events` COEXIST → policy =
quoted-UPPERCASE everywhere; MERGE INTO + QUALIFY ROW_NUMBER() DESC=1
delivers last-wins dedup; duplicate-merge-key = STRUCTURED code 100090 (the
23505 analogue); DDL AUTO-COMMITS an open transaction (proven: INSERT
survived ROLLBACK after CREATE TABLE) → the atomic unit is PURE DML with a
code-level guard refusing DDL inside units; pure-DML BEGIN/COMMIT/ROLLBACK
atomic incl. multi-statement; PUT refused by SQL API (391911) → internal
stage upload REQUIRES the session protocol; account is AWS (EU_CENTRAL_1)
so PUT = vended-cred S3 upload + client-side AES (aes/sha2/ring in lock,
cbc new). INGESTION: parquet parts → internal named stage → COPY INTO as
the bulk path (only bucket-free live-testable one; local RUSTFS is
unreachable from SaaS), batched INSERT for small loads, crossover MEASURED
on the qual account. BOTH fired sqlcore triggers TAKEN as separate
increments BEFORE the snowflake consumer (ensure choreography + session
protocol extractions), pg/duckdb golden pins BYTE-IDENTICAL throughout.
Live legs gate skip-not-fail on credential presence (container posture);
fakesnow 0.11 server-mode = T001 fidelity probe, adopt-or-reject recorded;
recorded ingestion session UNBARRED (no SaaS bar ever). Type mapping
closed: Json→VARIANT, Decimal→NUMBER(p,s) p<=38, Uuid→VARCHAR(36),
TIMESTAMP_TZ/NTZ. Semver purely ADDITIVE. Contract:
contracts/snowflake-dest.md SD1-SD8. Research decisions D1-D10:
research.md).
Previous feature 020 for reference:
`specs/020-audit-remediation/plan.md` (feature: audit remediation —
COMPLETE on branch `020-audit-remediation`, NOT merged and NOT pushed.
Executed NEXT_STEPS.md (audited 2026-07-26 @ 634222e: 11 lenses, 175
findings, 47 defect claims adversarially verified — 29 CONFIRMED, 18
REFUTED). All 11 stories delivered; close-out CLOSED: contract matrix
AR1-AR8 all MET; all 157 ledger items carry a terminal disposition
(130 fixed, 22 deferred with named triggers, 5 rejected with the
measurement that rejected them, zero uncited); the 18 refutations
verified absent from implemented work. Gate of record: `make check`
TWICE CLEAN on a rebooted machine — 791/791 0 skipped with containers,
cold-start 25.6 ms (bar <=40), perf gate within tolerance; coverage
85.64% lines (floor 80). The 18 refutations in Appendix A REMAIN
BINDING NON-GOALS. THE 0.2->0.3 WINDOW DID NOT OPEN: US5's design was
attacked before implementation (research R0 / close-out D-10) and
scoped down to within-run enforcement + inheritance — no StateDoc
bump, no persisted-format change, no semver break; the standing
publish-time bump is still owed but nothing in 020 forces it. CI
REPAIR REMAINS OUT OF SCOPE (E1, org billing); every CI-only
verification is recorded UNPERFORMED, never green. TWO OTHER
verifications are UNPERFORMED and say so: T097's Polaris live image
probe (no container runtime at that increment) and T176's netem (no
`tc`, and the container shares the HOST netns — a qdisc on lo would
degrade the real machine; D-40's substitute measurement proved more
useful). THE DURABLE PERF FACT: pg-to-pg-dedup-1m is ~71% SERVER-side
(80.3% of wall is one INSERT..ON CONFLICT node; 4,013,669 WAL records
= 556 bytes of WAL per row against a ~121-byte source row, one index
on the table), so client-CPU wins buy headroom, not wall — read any
future perf claim on this shape against that denominator. Eight US11
measurements, four TAKEN (COPY encoder fast path -1.98% process
instructions D-35; stage sequence CACHE 32 -3.3% of the merge cell
D-37; partition ArrayFormatter hoisted out of the row loop -2.72%
D-41; S3 skip-fetch for finished etag-matched objects D-44) and four
DECLINED WITH NUMBERS (allocator 3.3% ceiling D-34; WAL residual 8.5%
but 019's D2 binds D-36; file-dest buffering constant not O(dataset)
D-38; canonical-JSON allocation 6.19% ceiling D-39). D17 taken (one
byte-budget channel; the engine's copy DELETED, AR6 verified); D18
buffering half closed on a heap profile, blocking half still open;
D19 rejected, premise changed. Contract: contracts/audit-remediation.md
AR1-AR8. Deviations, negatives and every disposition: close-out.md).
Previous feature 019 for reference:
`specs/019-performance-improvements/plan.md` (feature: performance
improvements — COMPLETE, merged @ 634222e, executing PERF_ANALYSIS.md
as nine increments. RECORDED 3-WAY SESSION 2026-07-25 on the merged
tree, all four bars PASS: pg-to-pg-1m 778.8 ms 13.2x vs dlt (bar >=4x),
pg-to-s3parquet-1m 999.4 ms 1.7x (deliberately UNBARRED — one session
on a newly-comparable cell is not a basis for a bar), s3jsonl-to-pg-200k
665.2 ms 95.0x (>=40x), s3jsonl-to-s3parquet-200k 914.1 ms 63.6x
(>=45x), pg-to-pg-dedup-1m 4.82 s 2.6x (>=2x, new bar). NO LOSSES AND NO
PARITIES REMAIN. The honest misses, recorded not buried: US2 wall
-14.3% vs the >=15% floor and RSS -7.5% vs >=8% (attributed to the
parquet destination's whole-part buffering, which US7 never
re-measured); US6 cell-CPU -4.9% vs >=10%; T047's context-switch target
4.0x vs 10x. US9 was RE-SCOPED ON EVIDENCE: T089-T095 NOT built —
single-pipeline throughput reached 1.19M rows/s (3.3x the rate the
3.5x target was derived from), 8 concurrent pipelines scale 8.43x, and
the story's lever addressed only 22.2% of the merge cell, Amdahl-bounded
at 1.29x against SC-005's required 1.5x. So the 0.2->0.3 semver window
STAYED CLOSED in 019 (feature 020 US5 reopens it). Persisted-format
bumps: WAL v1->2 (arrow IPC file segments, parquet DELETED from
rdlt-engine, exact-match refusal both ways) and bench artifact v2->3.
[profile.release] fat LTO + cgu1 (-13.2% CPU, binary -16%); [profile.dist]
strip only, NO panic=abort, NO allocator crate. COPY encoder rewritten
on ToSql::to_sql over a borrowed ColumnView (-40.3% instructions);
full-refresh publishes COPY straight into the target in one unit tx.
Snappy is now the default parquet compression. TWO ALLOCATION REMOVALS
MEASURED WORSE (D-13, D-21) — treat any counting-argument optimization
as guilty until measured. Contract: contracts/performance-improvements.md
PI1-PI8. Outcomes and every deviation: close-out.md).
Previous feature 018 for reference:
`specs/018-bench-refinement/plan.md` (feature: benchmark refinement —
COMPLETE. The benchmark is ONE e2e five-cell THREE-WAY matrix
(rdlt/dlt/Airbyte, same seeded sources, per-product destination
databases/prefixes, every arm rowcount-verified, timing boundaries in
Caveats). Constitution v1.1.0 (Principle VIII cells/bars, recorded-
session-floor requirement) amended BEFORE the vocabulary deletion
(631d9bd < 212edf5); 25 cells / 10 fixtures / all v1 artifacts / 8
bars DELETED at 212edf5, archive commit 40841ab cited everywhere
(Milestones, artifact-v1 rejection error). Artifact format_version 2
(class gone, extra{} + forced added). Cold-start on the instruments
track (benches/check-cold-start.sh <=40ms). Competitors: dlt
honest-fastest (connectorx headline, pyarrow context) + Airbyte as
driver kind (flat variants discovery, benches/competitors/airbyte/
setup.py+driver.py over abctl kind on rootless podman; pods reach host
fixtures at 169.254.1.2; ingress-nginx MUST stay scaled to 0 and node
pids-limit raised — spike/01; API via supervised port-forward :8600).
Recorded sessions 2026-07-25: 2-way then 3-way 15/15 arms — rdlt
1.95/1.62/1.15/0.99/14.60 s; vs dlt 5.3x / 1.0x / 55.3x / 60.1x / 0.9x;
Airbyte ~45-60 s job wall, floor-dominated (Caveats). **THESE 018
FIGURES ARE SUPERSEDED BY 019 — see the 019 block above for the current
standing.** The "0.9x LOSS on the dedup cell" that 018 recorded was
never real: 019 US1 found the cell delivered 3M rows against dlt's 1M
(the source discovers every table when `tables` is absent), and the
corrected cell is a 2.6x WIN carrying its own bar. Do not plan against
the numbers in this paragraph. bars.toml at 018 time: 3 bars vs
dlt (4x/40x/45x) below recorded floors, policy entries; parity+loss
cells, RSS, Airbyte ratios, Iceberg cell all deliberately unbarred/
not-taken (policy log). Close-out + deviations:
specs/018-bench-refinement/close-out.md. Contract:
contracts/bench-refinement.md BR1-BR8).
Previous feature 017 for reference:
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
arrow 58 tree proven by live cargo-tree probe — workspace pins 58.3);
toolchain pinned 1.96.0 (rust-toolchain.toml + workspace rust-version).
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
