# Contract: Connector SPI (`rdlt-connector`)

**Status**: v1 target · **Stability**: SEMVER-SACRED (CI: `cargo semver-checks`)
**Audience**: connector authors (bundled and third-party) · **Enforced by**: public
conformance suite in `rdlt-testkit` — every clause below has a conformance test.

Vocabulary types (`Cursor`, `TableSchema`, `CommitMeta`, ids…) come from `rdlt-core` and
are re-exported here. Traits are object-safe; all exchange types are serde-serializable
(a future process/WASM host adapts this SPI over a wire without engine changes).

## Source

```rust
#[async_trait]
pub trait Source: Send + Sync + 'static {
    fn spec(&self) -> ConnectorSpec;                       // name, version, config JSON-schema
    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError>;
    /// Read ONE stream, pushing through `req.out`. Engine schedules streams concurrently.
    async fn read(&self, req: ReadRequest<'_>) -> Result<(), SourceError>;
}

pub struct ReadRequest<'a> {
    pub stream: &'a StreamSpec,
    pub since:  Option<&'a Cursor>,   // last committed cursor; source MUST resume from it
    pub out:    RecordsOut,           // push handle
}

impl RecordsOut {
    /// Perf path: raw JSON bytes (one document, an array, or NDJSON). Parsed straight
    /// into Arrow builders — sources that already hold bytes MUST prefer this.
    pub async fn raw_json(&mut self, bytes: Bytes) -> Result<(), ChannelClosed>;
    /// Convenience path for programmatically constructed rows.
    pub async fn rows(&mut self, rows: impl IntoIterator<Item = serde_json::Value>) -> Result<(), ChannelClosed>;
    pub async fn arrow(&mut self, batch: RecordBatch) -> Result<(), ChannelClosed>;
    /// "All rows pushed so far are complete up to `cursor`."
    pub async fn checkpoint(&mut self, cursor: Cursor) -> Result<(), ChannelClosed>;
}

#[non_exhaustive]
pub enum SourceError {
    Transient(BoxError),                                        // engine retries (backoff + jitter)
    RateLimited { retry_after: Option<Duration>, source: BoxError },
    Fatal(BoxError),                                            // run aborts
}
```

### Source obligations (conformance-tested)

| # | Clause |
|---|---|
| S1 | Given `since: Some(c)`, emit no row already covered by `c` (resume-from-cursor). |
| S2 | `checkpoint(c)` only after every row covered by `c` has been pushed. |
| S3 | Never retry internally — classify and return (`Transient`/`RateLimited`/`Fatal`); retries are engine-owned. |
| S4 | Treat `ChannelClosed` as cancellation: return promptly without error escalation. |
| S5 | Awaiting a push **is** flow control — no internal unbounded buffering. |
| S6 | Non-rewindable sources (queues/webhooks): checkpoint frequently; redelivered rows must be byte-stable so `_rdlt_id` dedup holds under `Merge`. |
| S7 | *(feature 002)* A source that pushes `arrow(batch)` on a stream MUST declare that stream `structured: true` in its `StreamSpec`. Arrow pushes on undeclared streams are rejected at runtime. Mixed raw_json+arrow pushes on one stream are unsupported in v1. |

## Destination

```rust
#[async_trait]
pub trait Destination: Send + Sync + 'static {
    fn spec(&self) -> ConnectorSpec;
    fn capabilities(&self) -> DestCapabilities; // merge, type matrix, ident rules, nesting
    async fn open(&self, ctx: OpenCtx<'_>) -> Result<Box<dyn LoadSession>, DestError>;
}

#[async_trait]
pub trait LoadSession: Send {
    /// Create/migrate physical table AND record its write disposition (`mode` is the
    /// root stream's mode for child tables — merge needs it at commit time).
    /// Idempotent; precedes first write at each schema version.
    async fn ensure_table(&mut self, schema: &TableSchema, mode: &WriteMode) -> Result<(), DestError>;
    /// Engine guarantees `batch` conforms to the last ensured schema — no drift ever.
    async fn write(&mut self, table: &TableName, batch: RecordBatch) -> Result<(), DestError>;
    /// Atomically publish all writes since last commit AND persist `meta`.
    /// Idempotent per (load_id, commit_seq): re-commit returns the prior receipt.
    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestError>;
    /// Recover engine state persisted by a previous run's commit.
    async fn read_state(&mut self, pipeline: &PipelineId) -> Result<Option<StateDoc>, DestError>;
}
```

### Destination obligations (conformance-tested)

| # | Clause |
|---|---|
| D1 | Staging invisibility: nothing from `write` is reader-visible until `commit` succeeds. |
| D2 | Atomicity: `commit` publishes the unit's data **and** `meta` (cursors, schema versions) together, or neither. |
| D3 | Idempotence: repeated `commit` with the same `(load_id, commit_seq)` re-publishes nothing and returns the prior receipt. |
| D4 | Staging teardown: `open` makes uncommitted staged data from any previous session invisible and reclaimable. |
| D5 | `ensure_table` is idempotent, applies migrations for the given schema version before returning, and records the table's write disposition for use at `commit`. |
| D6 | `read_state` returns exactly what the latest successful `commit` persisted (or `None` for a fresh pipeline). |
| D7 | Declared `DestCapabilities` are truthful — the engine plans (lowering, merge validation, ident normalization) from them and will not re-verify at runtime. |
| D8 | `Merge` (when capability declared): applies delete-by-`_rdlt_root_id` + insert subtree-replacement semantics for keyed tables. |

## Engine guarantees to connectors (the other side of the bargain)

| # | Clause |
|---|---|
| E1 | Per-table writes arrive in `seq` order; a `SchemaDelta`'s `ensure_table` always precedes the first write at the new version. |
| E2 | Exactly one stream owns a table (and its children) per run — rejected at build time otherwise; no cross-stream ordering falls on destinations. |
| E3 | Delivery to `write` is at-least-once; combined with D1–D4, visibility is exactly-once. |
| E4 | Batches passed to `write` conform exactly to the last `ensure_table`'d schema. |
| E5 | Transient/rate-limited errors are retried with backoff and `retry_after` honoring; retry counts surface in `RunReport`. |
| E6 | `since` cursors passed to `read` are only ever cursors the destination previously committed (never speculative). |
| E7 | *(feature 002)* Structured streams bypass the shredder: arrow schema maps to the logical schema (unmappable types are typed, column-naming errors), schema policies apply identically (Discard applies to column additions by projection; value-level discards are a typed error), `_rdlt_load_id` is the ONLY system column, and row data passes through zero-copy when a column's arrow type equals the table's current logical type; when cross-batch widening or an arrow representation difference (Large* variants, timestamp unit/zone) changed a column's type, values are cast losslessly to the current type — never semantically coerced. Append-mode delivery within a crash-recovery redelivery window is at-least-once for structured streams (no per-row identity to dedup with). Merge is rejected for them at plan time (clause B4, embedder contract). |
