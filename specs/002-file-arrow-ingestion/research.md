# Phase 0 Research: File & Arrow-Native Ingestion

**Date**: 2026-07-19 · **Status**: Complete — no open unknowns. Feature 001's
research (R1–R12) stands; entries here continue the numbering for this slice's
decisions.

## R13 — One file-source crate, two formats

- **Decision**: Single `rdlt-source-file` crate handling JSONL and Parquet, selected
  per stream by config (`format: jsonl | parquet`), not per-crate.
- **Rationale**: The listing/glob/cursor/change-detection machinery is shared; the
  formats differ only in the read loop (byte slabs → `raw_json` vs row groups →
  `arrow`). Two crates would duplicate the hard part to separate the easy part.
- **Alternatives**: Separate crates per format (duplication); format auto-detection
  by extension (implicit magic — rejected, explicit config keeps errors actionable).

## R14 — File cursor: per-file progress map, size-based change detection

- **Decision**: The stream cursor is a JSON map `path → progress` where progress is
  `bytes_done` (JSONL) or `row_groups_done` (Parquet), plus the file's total size as
  recorded when last read. A file is complete when `bytes_done == recorded size`.
  Resume rules: current size == recorded → skip/continue; current > recorded →
  read the appended tail from `bytes_done`; current < recorded → **fail naming the
  file** (rewritten/rotated; spec FR-003). Files sorted lexicographically; the file
  list is snapshotted once per run (spec edge case: no mid-run pickup).
- **Rationale**: Size-based detection is cheap, deterministic, and catches the
  dangerous case (shrunk/rewritten) without hashing whole files. Byte offsets make
  JSONL resume exact; row-group indexes are Parquet's natural seek unit.
- **Alternatives**: Content hashing per file (safe but O(file) on every run — defeats
  incremental resume); mtime comparison (unreliable across filesystems/copies);
  inode tracking (not portable).

## R15 — Checkpoint granularity

- **Decision**: Checkpoint after every pushed slab/row-group batch (cursor = full
  progress map), and at each file-completion boundary.
- **Rationale**: Frequent checkpoints shrink the redelivery window (design doc §6) —
  especially important because structured streams are at-least-once in that window
  (no per-row identity to dedup with). The cursor is small (a map over matched
  files); globs matching tens of thousands of files are documented as a scaling
  limit, not silently slow.
- **Alternatives**: Checkpoint only at file boundaries (larger redelivery window);
  delta-encoded cursors (complexity before need).

## R16 — Arrow passthrough: schema mapping + policy at the same seam

- **Decision**: New `shred/passthrough.rs` maps the incoming batch's arrow schema to
  a `TableSchema` (inverse of the existing physical mapping; unmappable arrow types →
  typed error naming the column, never coercion), runs the SAME registry diff +
  `SchemaPolicy` enforcement as shredded streams, appends a constant
  `_rdlt_load_id` column, and emits the standard Delta/Batch load items. No
  semantic transformation: same-typed columns pass through as Arc'd arrays and the
  appended column is the only new allocation; a column whose registry type widened
  across batches (or arrived in a different arrow representation, e.g. LargeUtf8,
  a nanosecond timestamp) is cast LOSSLESSLY to the current type.
- **Rationale**: Policies and evolution must behave identically regardless of how a
  stream arrives — one registry seam, two producers. Batch data stays zero-copy on
  the same-type common path (Arc'd arrays pass through; WAL, lowering, and
  destinations already operate on batches).
- **Alternatives**: Bypass the registry for structured streams (policy holes);
  convert batches to JSON and re-shred (absurd cost, defeats the point).

## R17 — Structured streams declare themselves; Merge rejected at build time

- **Decision**: `StreamSpec` gains `structured: bool` (serde-default `false`,
  additive). Sources that push Arrow batches MUST set it (new clause S7). The facade
  and engine reject `Merge` for structured streams at build/plan time (clause B4);
  the engine also rejects an Arrow push on an undeclared stream at runtime (defense
  in depth, clause E7). Lineage for structured streams: `_rdlt_load_id` only.
- **Rationale**: Merge requires `_rdlt_id`; computing content hashes over arbitrary
  arrow batches would reintroduce a full data pass and defeat passthrough. Declaring
  structuredness in the spec makes the build-time rejection possible (fail before
  any I/O, feature-001 clause B2 style).
- **Alternatives**: Hash rows to synthesize identity (cost; and key-based identity
  can't be inferred); reject at first push instead of build time (violates fail-fast
  principle); allow Merge with duplicate risk (silent-corruption class — rejected).

## R18 — Parquet destination: temp-dir staging + atomic rename

- **Decision**: `rdlt-dest-parquet` writes one directory per table; staged batches go
  to `<out>/.rdlt-staging/<session>/`, `commit` renames finished files into the table
  directory and rewrites `_rdlt_state.json` / `_rdlt_commits.json` via
  write-temp-then-rename. `open` deletes any `.rdlt-staging/*` (clause D4).
  Capabilities: `merge: false`, `structs: true`, `scalar_lists: true`,
  `decimal: true` (parquet is arrow-native). Append + Replace only.
- **Rationale**: Rename-based publication gives the D1–D3 guarantees on a plain
  filesystem without a transaction log; receipts/state as JSON files reuse the
  persisted-formats discipline (format_version field).
- **Caveat recorded honestly**: multi-file rename is not atomic as a set — a crash
  mid-commit can publish some files of a span without its receipt; the recovery
  re-commit then re-publishes the span's remaining files, and duplicate FILES are
  prevented by deterministic staged file names (same name = same content = idempotent
  overwrite). This is documented in the connector contract notes.
- **Alternatives**: Single-file-per-table rewrite (unbounded rewrite cost);
  manifest-file pointer swap (closer to iceberg-lite — more moving parts than a
  benchmark/export destination warrants in v1).

## R19 — Benchmark rows

- **Decision**: Three harness rows: (1) jsonl→DuckDB via `rdlt-source-file` (replaces
  the example-binary measurement; same dataset/methodology as the existing RESULTS.md
  row), (2) parquet→parquet via file source + parquet destination (the design doc's
  ≥2× cell, engine-bound), (3) parquet→DuckDB (bonus context row). Baselines: pinned
  dlt reading the same files (dlt's parquet read path for 2/3).
- **Rationale**: Clarification Q1 answer (option C); baseline-first methodology
  unchanged.
