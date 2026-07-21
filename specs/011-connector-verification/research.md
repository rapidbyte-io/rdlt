# Research: Postgres Connector Verification

Decisions for branch `011-connector-verification` (main @ 66c3003). The
parameter inventory below was verified against the config structs during
the 010 README rewrite (same session lineage); the coverage baseline was
MEASURED during planning (see R2).

## R1 — Coverage tool: cargo-llvm-cov over nextest

**Decision**: `cargo llvm-cov nextest -p rdlt-postgres` (plus
`--features failpoints` for the sweep binaries) as the measurement
command; `llvm-tools-preview` rustup component; wired into the house
Makefile vocabulary as `make coverage` (suite selection via the existing
TARGET idiom if needed). Line coverage is the recorded statistic;
region/branch numbers may be reported but the FLOOR is line coverage.

**Rationale**: llvm-cov is the only maintained option with accurate
source-based coverage for async/generic Rust; it composes with nextest
(the house runner) and instruments integration-test binaries, which is
where most connector behavior lives. Tarpaulin (ptrace-based) miscounts
async code and fights containers.

**Verification protocol (T001)**: install-or-verify the tool, run the
command twice to confirm stability, record baseline total + per-file
table in the evidence file. Subprocess coverage (the release CLI spawned
by `memory_bound`) is NOT expected to contribute — recorded as an
exclusion class, not silently lost.

## R2 — Baseline and gap strategy

**Decision**: T001 records the baseline BEFORE any new cells; every
subsequent coverage claim is a delta against it. Gap-closing follows the
per-file table, largest-uncovered-first, but ONLY through the parameter
matrix (US1) and genuine dark-corner tests — never coverage-only tests
that assert nothing (a cell that cannot state its behavioral claim in
one sentence does not get written; FR-007).

**Measured baseline (2026-07-21, `cargo llvm-cov nextest -p
rdlt-postgres --features failpoints`, cargo-llvm-cov 0.8.7)**: crate
total **87.69% lines** (87.34% regions, 83.17% functions) — the floor is
already met; the feature's value is confirmed as the matrix + gaps +
mismatches + classification, with 80% as the backstop. Per-file
outliers to investigate first:

| File | Lines | Working hypothesis (verify in audit) |
|---|---|---|
| source/mod.rs | 43.63% | the `testhook` module (bench_wire/bench_decode/fuzz entries) runs only under benches/fuzz — likely the dominant exclusion class; must verify no REAL `streams()`/`read()` branches hide behind it |
| dest/mod.rs | 74.51% | capability/open error arms |
| source/types.rs | 76.88% | hint-conversion arms + unusual oid branches — genuine matrix territory (12-hint closed table) |
| tls_verify.rs | 79.75% | verifier error branches |
| encode.rs | 84.82% | wire-encoder edge arms |
| cursor.rs | 85.16% | watermark/tracker branches |

Everything else ≥ 86%. T001 re-runs the same command to confirm
stability and records this table as the official baseline.

## R3 — The traceability matrix artifact

**Decision**: `specs/011-connector-verification/matrix.md` — one table
per config block, columns: `parameter | default | documented
values/behaviors | validation rules | citing cells | runtime class`.
Cell citations are `file::test_name` strings; runtime class ∈ {unit,
live (container), sweep (failpoints), heavy (RDLT_HEAVY)}. The matrix is
BUILT during implementation by auditing the existing suites first
(citations), then writing cells for the gaps. Zero unresolved rows is
the close-out criterion (SC-004); the matrix is reviewed like code.

**Rationale**: in-repo markdown keeps the audit greppable and reviewable
in the same PR as the cells it cites; `file::test` names are stable
under nextest filters, so spot-audits (SC-002) are one `-E` invocation.

## R4 — Parameter inventory (the audited surface)

Enumerated from the config structs (source of truth), ~60 parameters +
~40 enumerated values across:

- **Source top level** (9): `conn`, `schema`, `include_views`, `tables`,
  `queries`, `tls`, `cdc`, `batch_target_bytes`, `batch_max_rows`.
- **Table entry** (6): `name`, `cursor`, `primary_key`,
  `included_columns`, `excluded_columns`, `type_hints`.
- **Cursor block** (8): `column`, `initial_value`, `boundary` (2),
  `direction` (2), `end_value`, `end_bound` (2), `nulls` (3), `lag`
  (duration + magnitude families).
- **Query stream** (5): `name`, `sql`, `cursor`, `primary_key`,
  `type_hints`.
- **Type hints** (12 values): bool, int64, float64, decimal(p,s), utf8,
  binary, timestamp_tz, timestamp_naive, date, time, uuid, json — each
  documented (source → hint) pair plus the closed-table rejections.
- **CDC block** (7): `slot`, `publication`, `create_if_missing`, `mode`
  (2), `idle_wait`, `flag_column`, `ack` (2).
- **TLS block** (4): `mode` (5 values), `root_cert` (path/inline/
  platform-store), `client_cert`, `client_key` — both directions.
- **Conn-string surface**: sslmode spellings incl. verify-*, sslrootcert
  (+`system`), sslcert/sslkey, application_name default+override,
  rejected-parameters-by-name, percent-escapes, contradiction rules.
- **Destination connection** (3): `conn`, `dataset`, `tls`.
- **Destination options**: `merge_strategy` (3) destination-wide;
  per-table `merge_strategy`, `hard_delete`, `dedup_sort`
  ({column, order(2)}), `merge_key`, `scd2` ({valid_from, valid_to,
  absent(2)}).
- **CLI pipeline spec** (5): `pipeline`, `workdir`, `write_mode`
  (3 forms), `source` (rest/file/postgres; postgres inline XOR
  {config: path}), `destination` (3 kinds — postgres rows audited here,
  other kinds are parse-cells only per spec scope).

Interaction rows (FR-003) enumerated in the matrix design
(data-model.md): the audited cross-parameter behaviors, each its own
row.

## R5 — Resolution of the known `merge_strategy` footnote

**Decision**: typed rejection, matching the established posture (008
review F6 for inert `hard_delete` on children; 010 review F5 for inert
dedup_sort/merge_key under non-merge modes): a table-level OR
destination-wide `merge_strategy` explicitly configured while the
pipeline write mode for that table is append/replace becomes a typed
error at open naming table + mode. Nuance: the destination-wide DEFAULT
(`delete_insert`, i.e. merely unconfigured) must NOT reject append
pipelines — rejection applies to EXPLICIT configuration only. This is
why `PgDestOptions.merge_strategy` needs to distinguish "explicitly set"
from "defaulted" — an `Option` at the parse layer with the default
applied at resolution (shape decided at implementation; behavior is the
contract).

**Rationale**: silence was the recorded gap; documentation-as-designed
would enshrine an inconsistency with the F5/F6 posture for no benefit.

## R6 — Makefile wiring

**Decision**: `make coverage` runs the R1 command and prints the total +
per-file table; it is NOT part of `make check` (spec Out of Scope: no CI
gate this feature). The recorded number + command + date land in
`benches/RESULTS.md` alongside the other measurements, with the
exclusion classifications (R2).
