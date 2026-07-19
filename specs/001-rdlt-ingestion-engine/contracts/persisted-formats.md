# Contract: Persisted Formats

**Status**: v1 target · **Stability**: forward-compatible by format version field;
breaking a persisted format is a major-version event for `rdlt-core` regardless of Rust
API compatibility.
**Audience**: engine (reader/writer), rapidbyte platform (reader), destination
implementors (StateDoc storage), operators (debugging).

Principle (design doc §6): **the destination is the sole source of truth; the WAL is a
replayable buffer.** Every format below carries an explicit `format_version`.

## 1. StateDoc (in the destination — the correctness-critical one)

- One document per `PipelineId`, persisted by `LoadSession::commit` **atomically with the
  data it covers** (D2 in [connector-spi.md](connector-spi.md)).
- Content: format version; per-stream committed `Cursor`s (opaque, serde); current
  `SchemaHash` per table; last `(load_id, commit_seq)`; engine version that wrote it.
- Serialization: canonical JSON (stable field order) — destinations store it as an opaque
  document (e.g. a `_rdlt_state` table row); they MUST NOT interpret its interior.
- Compatibility: engines MUST read all prior format versions (migrate-on-read); a newer
  format version than the engine knows is a typed `RdltError::Config`-class failure, never
  a silent reset (a silent reset would re-extract from zero and duplicate under Append).

## 2. WAL segment files (workdir — the disposable one)

*(Amended 2026-07-19 to match the implementation; the correctness properties are
unchanged, the byte layout is now stated as built.)*

- Layout: `<workdir>/wal/<load_id>-<seq>.parquet`, standard parquet, schema equal to
  the current table schema's Arrow projection (system columns included); the owning
  table is recorded in the manifest's `segment` record.
- Loss/corruption of any segment is **recoverable by design**: recovery degrades to
  re-extraction from the last committed cursor (crash-matrix row 4) — slower, never wrong.
  A damaged span never aborts recovery.
- fsync policy: segments are fsynced at commit boundaries only; between commits, loss on
  power failure is expected and covered by the degradation path.

## 3. WAL manifest (workdir)

*(Amended 2026-07-19 to the implemented record shapes.)*

- ONE append-only JSON-lines file (`manifest.jsonl`) shared across runs; each run
  begins with a `run` header record. Records (serde-tagged with `rec`):
  - `run { format_version, load_id, pipeline }` — starts a run; `format_version`
    governs the whole manifest (a newer-than-supported version degrades recovery to
    cursor re-extraction, never a misread),
  - `delta { schema, delta, mode }` — the FULL `TableSchema` is embedded (recovery
    must `ensure_table` on a fresh session even when the delta committed in an
    earlier span; hashes alone could not reconstruct it),
  - `segment { table, file, rows }`,
  - `checkpoint { stream, cursor }` — coverage is positional: a checkpoint covers
    every segment recorded before it within the run,
  - `committed { commit_seq }` — the receipt identity is `(run.load_id, commit_seq)`.
- Ordering invariant on disk = replay invariant: a `delta` precedes the first
  `segment` at its version; a `checkpoint` follows every segment it covers;
  `committed` marks segments GC-eligible. Replay applies only up to the LAST
  checkpoint of the uncommitted span — segments beyond it are not cursor-covered and
  are re-extracted instead (never double-applied).
- Recovery scan is a single forward pass; a torn final line (crash mid-append) is
  truncated and ignored (append-only ⇒ prefix is always valid).

## 4. RunReport / PipelineEvent (emitted, persisted by platforms)

- Serde-stable, `#[non_exhaustive]`, format-versioned. Consumers MUST ignore unknown
  fields/variants (that is what non_exhaustive buys); producers MUST only add, never
  rename/retype, within a major version.

## 5. Schema hashing (cross-cutting)

- `SchemaHash` = content hash over the canonical serialized `TableSchema` (normalized
  names, ordered columns, logical types, nullability; system columns included; hint
  provenance excluded).
- The hash algorithm and canonical form are frozen per `rdlt-core` major version — hashes
  appear in StateDoc, WAL manifest, and `SchemaDelta` audit chains, so instability here
  would sever every audit trail at once.
