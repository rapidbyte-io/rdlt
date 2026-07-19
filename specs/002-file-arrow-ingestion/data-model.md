# Data Model: File & Arrow-Native Ingestion

**Date**: 2026-07-19 · Delta on [feature 001's data model](../001-rdlt-ingestion-engine/data-model.md);
everything not listed here is unchanged.

## 1. FileCursor (the file stream's `Cursor` payload)

Opaque to the engine (stored/returned like any cursor); defined by `rdlt-source-file`:

```json
{
  "format_version": 1,
  "files": {
    "<absolute path>": { "done": 12345, "size": 12345 }
  }
}
```

- `done`: bytes consumed (JSONL) or row groups consumed (Parquet).
- `size`: file length (bytes) / total row groups when last read — the change detector.
- **Invariants**: complete ⇔ `done == size`; on resume, `current_size < size` for any
  tracked file is a fatal, file-naming error (FR-003); `current_size > size` with
  `done == size` reopens the file at `done` (appended tail).
- Ordering: matched files processed in lexicographic path order; the match list is
  snapshotted once per run.

## 2. `StreamSpec.structured` (SPI amendment, additive)

- `structured: bool` (serde default `false`). `true` declares "this stream pushes
  already-structured Arrow batches".
- Consequences (validated at build time): `Merge` write mode rejected; lineage is
  `_rdlt_load_id` only; delivery in the crash-recovery redelivery window is
  at-least-once for Append (documented, clause E7).

## 3. Passthrough table schema

- Derived from the batch's arrow schema: field name → normalized column name
  (feature 001 naming rules), arrow type → logical type (inverse of the physical
  mapping; unmappable types are a typed error naming the column).
- System columns: `_rdlt_load_id` ONLY (no `_rdlt_id`/parent/pos/root — structured
  streams have no child-table splitting; nested arrow types pass through as struct/
  list columns subject to destination lowering).
- Evolution: same `SchemaRegistry` diff + `SchemaPolicy` path as shredded streams;
  arrow type changes map onto widen-or-policy exactly like inferred ones.

## 4. Parquet destination on-disk layout

```text
<out_dir>/
├── <table>/part-<load_id>-<commit_seq>-<n>.parquet   # published data files
├── _rdlt_state.json                                   # StateDoc (format_version 1)
├── _rdlt_commits.json                                 # receipt set {load_id, commit_seq}
└── .rdlt-staging/<session_id>/…                       # staged; deleted on open (D4)
```

- Publication = rename staged files into `<table>/` + rewrite the two JSON files
  (write-temp + rename). Staged file names are deterministic per
  `(load_id, commit_seq, table, n)` so a re-commit after a mid-commit crash
  overwrites rather than duplicates (see research R18 caveat).
- Replace mode: the table directory's data files are removed once per load before
  the first publish into it.

## 5. Report/event semantics (unchanged shapes, new cases)

- Structured streams emit the same `StreamStarted/BatchLoaded/SchemaEvolved/
  Committed` events; `BatchLoaded.rows/bytes` come from the batch.
- `RunReport.tables` counts passthrough rows identically; discards can only originate
  from schema policy (no value-level discards on the passthrough path).
