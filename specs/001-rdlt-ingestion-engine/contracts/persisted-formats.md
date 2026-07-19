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

- Layout: `<workdir>/wal/<load_id>/<table>/<seq>.parquet`, standard parquet, schema equal
  to the manifested `SchemaHash`'s Arrow projection (system columns included).
- Loss/corruption of any segment is **recoverable by design**: recovery degrades to
  re-extraction from the last committed cursor (crash-matrix row 4) — slower, never wrong.
  A corrupt segment therefore quarantines (renamed aside, warned, counted), never aborts
  recovery.
- fsync policy: segments are fsynced at commit boundaries only; between commits, loss on
  power failure is expected and covered by the degradation path.

## 3. WAL manifest (workdir)

- Append-only JSON-lines file per run: each record is one of
  `segment { load_id, table, seq, schema_hash, bytes }`,
  `delta { from_hash, to_hash, delta }`,
  `checkpoint { stream, cursor, covers_up_to_seq }`,
  `committed { commit_seq, receipt }`.
- Ordering invariant on disk = replay invariant: a `delta` record precedes the first
  `segment` at its `to_hash`; a `checkpoint` follows every segment it covers; `committed`
  marks segments GC-eligible.
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
