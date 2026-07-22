# Tasks: Filesystem/Object-Store Completeness

**Input**: Design documents from `/specs/015-file-completeness/`

**Prerequisites**: plan.md, research.md (R1–R10), data-model.md,
contracts/file-family.md (FF1–FF8), quickstart.md

**Tests**: included — the standing discipline: the EXISTING cells of
BOTH pre-015 crates are the weld's behavior-preservation net (green at
every stage, never edited beyond mechanical import paths); every new
location/format/option lands WITH its cells; container cells
skip-not-fail; the matrix commits WITH the cells that close its gaps
(011 rule).

**Organization**: tasks grouped by user story; US order is build order.
No big-bang commit — every task leaves the whole suite green.

## Phase 1: Setup

- [X] T001 Environment gate + baselines: verify the podman shim
  (`~/.local/bin/podman` → distrobox-host-exec — recreate if the
  container was rebuilt); pull and verify the RUSTFS image (confirm
  image/tag, port, access/secret-key env-var names by starting it once
  and issuing an S3 ListBuckets — record the verified values in
  `specs/015-file-completeness/research.md` R8, correcting any
  assumption); measure the coverage BASELINES
  (`cargo llvm-cov nextest -p rdlt-connector-file` and
  `-p rdlt-connector-parquet`, recorded); run both crates' full suites
  and the two touched gated bench bars (`TARGET=parquet-passthrough
  make bench`, `TARGET=jsonl-duckdb-200k make bench`, release build,
  quiet machine) recording the pre-weld medians in the task notes.
  **T001 recorded (2026-07-22)**: podman shim present+working; RUSTFS
  verified (see research R8 addendum — image/env/port assumptions all
  held). Coverage baselines: rdlt-connector-file **73.25%** lines,
  rdlt-connector-parquet **90.80%** lines. Suites: 27/27 green (both
  crates). Pre-weld gated medians: parquet-passthrough **92.3 ms**
  (3.6x vs dlt, bar >=2x PASS); jsonl-duckdb-200k **1083.2 ms**
  (13.7x vs dlt, bar >=10x PASS; RSS 1/5.5 vs bar <=1/5 PASS).

## Phase 2: Foundational — the weld (blocking all stories)

- [X] T002 Source move (moves only): restructure
  `crates/rdlt-connector-file/src/` to the family layout —
  `src/source/{mod.rs,config.rs,cursor.rs}` +
  `src/formats/{mod.rs,jsonl.rs,parquet.rs}` (jsonl/parquet readers
  move under formats; `Format` enum moves to formats/mod.rs) — with
  `src/lib.rs` a thin façade (`pub mod source; pub mod formats;` +
  root re-exports of every currently-public item). All existing tests
  compile UNCHANGED beyond import paths; suite green.
- [X] T003 Dest absorption: move the ENTIRE
  `crates/rdlt-connector-parquet/src/lib.rs` surface into
  `crates/rdlt-connector-file/src/dest/{mod.rs,config.rs}` (staging
  constants, `LAYOUT_FORMAT_VERSION`, `pq.*` FAIL_POINTS registry,
  receipt/state formats byte-identical; `ParquetDir` re-exported from
  the unified crate root); move the parquet crate's tests into
  `crates/rdlt-connector-file/tests/` (mechanical paths only); DELETE
  `crates/rdlt-connector-parquet/`; rewire workspace members, the
  façade (`crates/rdlt/`: `rdlt::connector::file` gains dest,
  `rdlt::connector::parquet` re-export removed), CLI
  (`crates/rdlt-cli/src/main.rs`: `DestSpec::Parquet { path }` parses
  UNCHANGED, constructs the unified dest), bench harness references,
  and `Makefile` sweep lines. Full workspace suite + doc-tests green.
- [X] T004 The net, pinned: persisted-format fixture cells in
  `crates/rdlt-connector-file/tests/preservation.rs` — a committed
  pre-015 cursor document parses and drives resume decisions
  identically; a committed pre-015 staging layout + receipt/state file
  set is recovered/republished identically; a pre-015 pipeline YAML
  (`source: file:` + `destination: parquet:`) parses with identical
  meaning (spelling freeze, FF1). Re-measure both gated bars
  same-session (`TARGET=parquet-passthrough`, `TARGET=jsonl-duckdb-200k`)
  — in-band vs T001's pre-weld medians (SC-001).

**Checkpoint**: one crate, zero behavior change proven.

## Phase 3: User Story 2 — Read files from object storage (P2)

**Goal**: Location abstraction + S3-compatible reading + CSV + codecs,
proven against the RUSTFS container.

**Independent test**: seeded-bucket loads land exact totals through the
engine; delta runs read only the delta; CSV/codec/error cells pass;
everything skips (not fails) without a container runtime.

- [X] T005 [US2] Dependencies + Location layer: add `object_store`
  (workspace dep, `aws` feature only) and promote `csv`, `flate2`,
  `zstd` to direct workspace deps (already in the lock — R1 survey
  verdicts recorded);
  `crates/rdlt-connector-file/src/location/{mod.rs,s3.rs}` — `Location`
  enum (Local | S3 per data-model §1, credentials as `Secret` from the
  014 pattern — add the grep-proof cell), operations: complete listing
  (continuation tokens fully drained or typed failure — FF2),
  range read, streaming get; typed open-time errors naming
  endpoint/bucket for unreachable/unauthorized (empty prefix stays
  success); unit cells in-file for config validation + error taxonomy
  (data-model §8).
- [ ] T006 [US2] RUSTFS fixture + first live cells:
  `crates/rdlt-connector-file/tests/common/s3.rs` — container
  start/health/seed/teardown helpers (seeding through the Location
  layer itself; skip-not-fail without podman, postgres-cell pattern);
  live cells in `crates/rdlt-connector-file/tests/s3_live.rs`: seeded
  jsonl bucket → engine → duckdb exact totals; listing-pagination cell
  (seed >1000 keys, complete set proven by totals); unreachable
  endpoint + wrong-credentials typed cells.
- [ ] T007 [US2] Source over Location: thread `location:` through
  `src/source/{config.rs,mod.rs}` (additive field, data-model §5;
  validation: csv block only with format csv, existing rules
  untouched) and route discovery + reads through the Location layer
  (local behavior byte-identical — the existing local cells are the
  net); per-file cursors extend to `(size, etag)` identity
  (`src/source/cursor.rs`, additive `etag` field, one-rulebook rules
  FF3: skip-completed, grown-tail, rewritten-typed); live cells in
  s3_live.rs: delta run (two files added + one grown → exactly the
  delta, proven by read accounting), etag-tripwire cell (overwrite an
  object same-size → typed error naming the key).
- [ ] T008 [P] [US2] CSV format:
  `crates/rdlt-connector-file/src/formats/csv.rs` — record-stream
  reader via NDJSON conversion (R4): options {delimiter, header,
  quote}, inference lattice bool→int64→float64→utf8 (empty = null,
  headerless = c0..cN), `type_hints` override with typed violations
  naming file+row+column, malformed-row typed naming file+row; unit
  cells in-file + local cells in
  `crates/rdlt-connector-file/tests/csv.rs` (options matrix, hints,
  empty-data-rows file, inference documentation cell).
- [ ] T009 [P] [US2] Codecs:
  gzip/zstd wrapping in `src/formats/mod.rs` by extension for
  jsonl/csv (magic-byte check → codec/extension mismatch typed naming
  the file; compressed-parquet spelling rejected typed at parse);
  whole-file incremental units (resume only at done==size; growth =
  rewrite = loud, R5); cells in tests/csv.rs +
  tests/preservation.rs-adjacent local jsonl.gz/zst cells (exact
  totals, mismatch typed, completed-skip across runs).
- [ ] T010 [US2] US2 integration cell: the quickstart shape live —
  csv.gz files in RUSTFS with hints + primary_key → engine → duckdb,
  exact totals + a second delta run (SC-002/SC-003 closing cells in
  tests/s3_live.rs).

**Checkpoint**: US2 delivers object-store + CSV reading, fully proven.

## Phase 4: User Story 3 — Write files to object storage (P3)

**Goal**: dest formats/locations/partitioning with commit-atomic
visibility on both location kinds.

**Independent test**: engine runs (incl. crash/rerun) against RUSTFS
produce exactly-once file sets; partition layout splits correctly;
readers never observe final-name partials.

- [ ] T011 [US3] Dest config evolution
  (`crates/rdlt-connector-file/src/dest/config.rs`, data-model §6):
  `location:` (shared vocabulary), `format: parquet|jsonl` (default
  parquet — frozen), `partition_by:` (column must exist at write time,
  typed; NULL values → documented `__null__` prefix); CLI
  `DestSpec::File` (new spelling, full vocabulary) beside the FROZEN
  `DestSpec::Parquet` in `crates/rdlt-cli/src/main.rs`; schema
  round-trips extended in
  `crates/rdlt-connector-file/tests/config_schema.rs`.
- [ ] T012 [US3] Dest over Location: staged-put + COPY+DELETE finalize
  for S3 (R6: deterministic final names per (load, commit, table, n),
  idempotent re-finalize, state/receipt documents written LAST, same
  order as local); jsonl writer in `src/formats/jsonl.rs` (write side);
  partition split in `src/dest/mod.rs` (one prefix per partition
  value, rows in exactly one file set); local rename protocol
  BYTE-IDENTICAL (existing dest cells are the net); live cells in
  tests/s3_live.rs: commit-atomicity probe (a concurrent lister during
  the run observes staged names only, never final-name partials),
  partition-split totals, jsonl-output parity, replace-mode cell.
- [ ] T013 [US3] Crash points both kinds: new points `file.list`,
  `file.read` (source), `file.stage.put`, `file.finalize.copy`,
  `file.finalize.delete` (dest) in the respective modules + FAIL_POINTS
  registries; `crates/rdlt-connector-file/tests/sweep.rs` — the local
  matrix (pq.* preserved + file.* points, armed-fire pins, exactly-once
  totals) always; the object matrix inside the container-gated arm;
  Makefile `TARGET=sweep` gains the file sweep binary.

**Checkpoint**: the lake story closes both directions.

## Phase 5: Polish & close-out

- [ ] T014 [P] Traceability matrix
  `specs/015-file-completeness/matrix.md`: every config row (location,
  discovery, cursor, per-format options, codecs, dest
  format/partition/location, CLI spellings) → cells, zero uncited
  (gap cells land WITH this task); dlt-parity record
  `specs/015-file-completeness/dlt-parity.md` (vs dlt's filesystem
  source + filesystem destination: mapping per capability, deliberate
  deviations named — provider-native auth deferred, no delta/iceberg,
  scoreboard-not-gated posture recorded).
- [ ] T015 Close-out: coverage re-measure to the ≥80% floor for the
  unified crate with classified exclusions recorded in
  `benches/RESULTS.md`; new bench cell `file-s3-duckdb-200k`
  (SCOREBOARD, R10) declared in `benches/cells/` with the rustfs
  fixture and recorded baseline-first; both touched gated bars
  re-verified in-band; comprehensive
  `crates/rdlt-connector-file/README.md` (013/014 standard — full
  options reference, both directions, both location kinds);
  `make check` + doc-tests + semver (no NEW break beyond the recorded
  014 major — verify the crate removal reports under it); quickstart.md
  walked verbatim.

## Dependencies

- T001 → T002 → T003 → T004 (strictly sequential: gate → move →
  absorb → pin)
- US2: T005 after T004; T006 after T005; T007 after T006; T008/T009
  [P] after T005 (formats are independent of the live fixture);
  T010 after T007+T008+T009
- US3: T011 after T004 (config can start once the weld holds);
  T012 after T011+T005 (needs Location); T013 after T012
- T014 [P] after all cells exist; T015 last
- Parallel: T008+T009 beside T006/T007; T011 beside T008/T009

## Implementation strategy

MVP = Phases 1–2 (the weld alone is shippable: one crate, nothing
changed). The non-negotiables at every stage: pre-015 cells pass
UNCHANGED (an edit beyond import paths = a behavior change — stop);
container cells skip-not-fail; the two touched gated bars re-measured
same-session as any change that could plausibly move them (T004, T015
at minimum, quiet-machine discipline). RUSTFS facts (image/env) come
from T001's verification, not assumptions — correct R8 if reality
differs.
