# Data Model: rdlt — Data Ingestion Engine Library

**Date**: 2026-07-19 · **Phase**: 1 · **Sources**: [spec.md](spec.md) Key Entities,
design doc §§4–6, [research.md](research.md) R2/R6/R7/R8.

Ownership rule (R2): everything in this document that is persisted, reported, or crosses a
wire lives in `rdlt-core`. Trait-adjacent types (`StreamSpec`, `DestCapabilities`, error
enums) live in `rdlt-connector`. Types marked *engine-internal* are `pub(crate)` in
`rdlt-engine` and may change freely.

## 1. Identifiers (`rdlt-core`)

| Type | Represents | Notes |
|---|---|---|
| `PipelineId` | A named, repeatable pipeline | Key for state lookup in the destination |
| `LoadId` | One run (execution) of a pipeline | Stamped on every row (`_rdlt_load_id`); unique per run |
| `StreamName` | A source stream | Normalized; maps 1:1 to a root `TableName` |
| `TableName` | A physical destination table | Root or child; produced by naming/normalization rules |
| `SchemaHash` | Content hash of one `TableSchema` version | Deterministic; equal schema ⇒ equal hash |
| `RowId` | Deterministic row identity (`_rdlt_id`) | Content hash (keyless) or key hash (keyed) |

All ids are newtypes (no bare `String`/`u64` crossing a seam), serde-stable, `Display`able.

**Validation**: identifier normalization (case folding, allowed charset, length caps per
destination ident rules) is a pure function in `rdlt-core::naming`; collisions after
normalization/flattening get a deterministic hash suffix — distinct source names never
silently merge.

## 2. Logical types & widening (`rdlt-core`)

`LogicalType` = `Bool | Int64 | Float64 | Decimal(p,s) | Utf8 | Binary | TimestampTz |
TimestampNaive | Date | Time | Uuid | Json`.

- Mapped to Arrow physically (engine) and to destination types via `DestCapabilities`
  (loader planning).
- `Json` is the typed escape hatch: undecomposable values are preserved verbatim, never
  dropped, never exploded into variant columns.
- `widen(a, b) -> LogicalType` is a pure join on the lattice (R7): commutative,
  idempotent, monotone — property-tested as laws. Value-checked conversions at shred time
  may escalate further (e.g. `Int64` beyond ±2^53 under a `Float64` column → `Utf8`).

**State transitions (per column)**: type moves only *upward* in the lattice, each move
recorded as a `SchemaDelta`. No downward or sideways moves, ever.

## 3. Table schema & evolution (`rdlt-core`)

- **`ColumnDef`**: name (normalized), `LogicalType`, nullability, provenance (inferred |
  hinted | system), optional hint metadata.
- **`TableSchema`**: `TableName`, ordered `ColumnDef`s, parent linkage (root table or
  child-of + depth), `SchemaHash` (content hash over the canonical form).
- **`SchemaDelta`**: `from_hash → to_hash` plus the minimal change set (add column, widen
  column, add table). Auditable and replayable; the only way schemas change.
- **`SchemaPolicy` / contract**: per table/column/type: `Evolve | Freeze | DiscardRow |
  DiscardValue`. `Freeze` turns a would-be delta into a typed `ContractViolation` **before
  any row of the violating batch is written**; discards are counted into `RunReport`.

**Invariants**:
- A `SchemaDelta` chain from the first version to the current one always exists and
  re-derives every intermediate `SchemaHash` (audit trail).
- In the WAL and in replay, a delta is always ordered **before** the first batch at its
  `to_hash` (crash-safe mid-run evolution).
- System columns (§5) are present in every `TableSchema` and are non-evolvable.

## 4. Streams, cursors & state (`rdlt-core`; `StreamSpec` in `rdlt-connector`)

- **`StreamSpec`** (connector crate): stream name, optional primary key, cursor field,
  per-column type hints, nesting hints. Declared by sources; consumed by engine planning.
- **`Cursor`**: opaque-to-engine, serde-serializable source position. Source-defined
  semantics; engine only stores, compares for equality, and returns it.
- **Checkpoint**: a `Cursor` pushed by the source asserting "all rows pushed so far are
  complete up to here". Rows between two checkpoints form one **recovery unit**.
- **`StateDoc`**: per-pipeline persisted state — per-stream committed `Cursor`s, current
  `SchemaHash` per table, last `(load_id, commit_seq)` receipt info, format version.
  Stored *in the destination*, written atomically by `commit`.

**Invariant**: a `Cursor` is committed iff every row it covers is in the same commit unit
(no cursor ever runs ahead of its data).

## 5. Lineage & identity (columns stamped by the shredder)

| Column | On | Value |
|---|---|---|
| `_rdlt_load_id` | every row | the run's `LoadId` |
| `_rdlt_id` | every row | deterministic `RowId`: keyed → hash of key fields; keyless → content hash |
| `_rdlt_parent_id` | child tables | parent row's `_rdlt_id` |
| `_rdlt_pos` | child tables | ordinal within the parent's list |
| `_rdlt_root_id` | child tables (every depth) | root row's `_rdlt_id` |

**Invariants**:
- `_rdlt_id` is deterministic across runs for identical input (property-tested).
- `_rdlt_root_id` at every depth makes merge subtree-replacement two flat operations
  (delete by root id, insert) — no per-level parent joins.
- Keyless `Merge`: byte-identical rows collapse to one (documented dedup semantics).
- Nested content changes propagate into child `_rdlt_id`s, so merge sees changed children.

## 6. Write modes & commit protocol (`rdlt-core`)

- **`WriteMode`**: `Append | Replace | Merge { key: Vec<String> }` — per stream. `Merge`
  requires destination `merge` capability (fail-fast at build time otherwise) and replaces
  the updated record's entire subtree (§5).
- **`CommitMeta`**: `LoadId`, `commit_seq`, per-stream `Cursor`s covered, `SchemaHash` per
  touched table, counters (rows/bytes/discards for the unit).
- **`CommitReceipt`**: destination acknowledgment keyed `(load_id, commit_seq)`;
  re-committing the same key returns the prior receipt (idempotence).
- **`CommitPolicy`** (engine config): group checkpoint spans into commit units by N
  checkpoints | bytes | seconds.

**Commit-unit state machine** (engine-internal driver, contract-visible states):

```
accumulating ──(policy trigger)──► committing ──(receipt)──► committed ──► WAL segments GC'd
      ▲                                 │(crash)
      └────────── restart: replay WAL ──┴─► re-commit (idempotent) or re-extract
```

Staged writes are invisible until `committed`; `open` after a crash tears down anything
left in `committing`/staged.

## 7. Observability & report (`rdlt-core`)

- **`PipelineEvent`** (`#[non_exhaustive]`, serde-stable): `StreamStarted`, `BatchLoaded`,
  `SchemaEvolved { delta }`, `Committed { cursors }`, retry/discard notices.
- **`RunReport`** (`#[non_exhaustive]`, serde-stable): per-table rows/bytes, schema
  migrations applied, discard counts by cause, committed cursor positions, `resumed_from`,
  elapsed. **Accounting invariant**: report totals equal destination-visible reality; every
  retry, widening, and discard appears (no silent failures — spec FR-012, SC-008).

## 8. Capabilities & errors (`rdlt-connector`)

- **`DestCapabilities`** (data, not traits): merge support, logical→destination type
  matrix, identifier rules (length/charset/case), nesting support (struct-native |
  flatten), list support. The engine *plans around* capabilities: lowering strategy,
  merge validation, ident normalization — all decided at build time where possible.
- **`SourceError`**: `Transient` (engine retries, backoff+jitter) | `RateLimited
  { retry_after }` | `Fatal`. **`DestError`**: analogous taxonomy.
- **`RdltError`** (embedder layer, `rdlt-core`): `Config | Schema(ContractViolation) |
  Source { stream, source } | Destination { .. } | Wal( .. )` — each variant maps to one
  operator action.

## 9. Engine-internal formats (stable *format*, private *type*)

- **WAL segment**: Arrow IPC file keyed `(load_id, table, seq)`.
- **WAL manifest**: append-only records `segment → SchemaHash → covering checkpoint`;
  deltas manifested before any batch at the new version; fsync at commit boundaries only.
- These are *persisted formats* documented in
  [contracts/persisted-formats.md](contracts/persisted-formats.md) with a format-version
  field, but their Rust types are `pub(crate)` — only the bytes are contractual (a
  workdir may be read by a newer engine, never by connectors).

## 10. Relationship map

```
Pipeline 1─* Run(LoadId) 1─* CommitUnit(commit_seq) 1─* RecoveryUnit(checkpoint span)
Pipeline 1─1 StateDoc (in destination; updated per commit)
Source 1─* StreamSpec; Stream 1─1 root TableSchema 1─* child TableSchema (via nesting)
TableSchema 1─* SchemaDelta (version chain by SchemaHash)
Row *─1 LoadId; child Row *─1 parent Row; child Row *─1 root Row (_rdlt_root_id)
CommitMeta *─* Cursor (per covered stream); CommitReceipt 1─1 (LoadId, commit_seq)
```
