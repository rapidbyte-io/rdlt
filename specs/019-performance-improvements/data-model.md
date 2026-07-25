# Data Model: feature 019

This feature changes almost no data. That is the point: it makes existing work
cheaper while holding every persisted value byte-identical (contract PI4). This
document records the four shapes that DO move, the three that are frozen and
why, and the facts each claim rests on.

Every registry and code fact below was verified at plan time against the local
cargo registry cache and the tree at commit `270c903`.

---

## 1. Recovery-log segment — the one authorised format change

**Today** (`crates/rdlt-engine/src/wal/mod.rs`):

| element | value |
|---|---|
| Manifest | `manifest.jsonl`, append-only, one `WalRecord` per line |
| `WAL_FORMAT_VERSION` | `1` (mod.rs:30) |
| Version seeding | `initial_wal_version()` returns a literal `1` (mod.rs:37), deliberately separate from the ceiling constant so a bump cannot silently claim old manifests are current |
| Segment file | `{load_id}-{seq:06}.parquet` (mod.rs:141) |
| Segment container | Parquet via `ArrowWriter::try_new(file, schema, None)` — `None` means **default `WriterProperties`**: dictionary encoding on, statistics on, RLE (mod.rs:216) |
| Reader | `ParquetRecordBatchReaderBuilder` (resume.rs:19-25), yielding `Iterator<Item = Result<RecordBatch, _>>` |
| Lifecycle | written per batch → fsynced at commit → `remove_file`d once the receipt lands |

**After** (spec decision D3, PI4's single authorised bump):

| element | change |
|---|---|
| Container | a streaming record-batch container that performs no dictionary construction, no statistics, no RLE. `arrow::ipc` — **already in the tree**: `arrow`'s default features are `["csv", "ipc", "json"]` and `arrow-ipc 58.3.0` is in `Cargo.lock`. File-vs-stream variant and truncation behaviour: `research.md` |
| `WAL_FORMAT_VERSION` | `1` → `2`. `initial_wal_version()` stays `1` by design — it seeds nothing new and must not track the ceiling |
| Segment file | extension renamed off `.parquet`; every reader, test and glob that assumes it moves in the same change |
| Reader | the displaced Parquet reader is **deleted** (PI2) — no fallback |
| Old manifests | refused by version, reason logged, recovery degrades to source re-extraction (FR-014). Never misread |

**Unchanged**: the manifest itself (JSONL, `WalRecord` variants, ordering =
replay order), the commit protocol ordering (record → destination write → fsync
→ destination commit → mark + GC), and the guarantee that write-ahead logging
happens for **every** run regardless of write mode (spec decision D2 — skipping
it for all-Replace runs was measured at only a further ~4–6% and rejected
because it would make recovery a full source re-extraction).

**Frozen alongside**: `StateDoc`, commit receipts, and the
`(load_id, commit_seq)` identity that makes replay idempotent. None of them
change.

---

## 2. Output-format settings — the one place surface grows

Today there is no vocabulary at all: three call sites pass `None`/`default()`
and a user cannot influence any of it.

| call site | today |
|---|---|
| `crates/rdlt-connector-file/src/dest/session.rs:51` | `ArrowWriter::try_new(&mut buf, schema, None)` |
| `crates/rdlt-connector-iceberg/src/dest/writer.rs:64` | `WriterProperties::default()` |
| `crates/rdlt-engine/src/wal/mod.rs:216` | `None` — superseded by §1 |

The measured consequence: rdlt writes **210.0 MB where the reference
implementation writes 73.7 MB** for the same 1M rows, and on high-cardinality
columns the dictionary builder is the largest single processor cost in the
pipeline.

**Setter names verified against `parquet 58.3.0`** (`src/file/properties.rs`):

| intent | builder method | notes |
|---|---|---|
| compression | `set_compression(Compression)` | variants: `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `LZO`, `BROTLI`, `LZ4`, `ZSTD`, `LZ4_RAW`. Levelled codecs take a level argument; `SNAPPY` does not |
| dictionary on/off | `set_dictionary_enabled(bool)` | |
| dictionary bail-out | `set_dictionary_page_size_limit(usize)` | the cheap high-cardinality fix — the encoder abandons the dictionary past the limit instead of interning a million distinct values |
| row-group sizing | `set_max_row_group_size(usize)` | `set_max_row_group_bytes` / `set_max_row_group_row_count` also exist in 58.3 |
| page sizing | `set_data_page_size_limit(usize)` | |
| statistics | `set_statistics_enabled(EnabledStatistics)` | |

Per-column equivalents exist (`set_column_compression`,
`set_column_dictionary_enabled`, …) if the vocabulary later needs them; v1 of
this vocabulary is destination-wide.

**Default changes from `UNCOMPRESSED` to `SNAPPY`** (spec decision D4). Costs
**zero new dependencies**: parquet's default features already include `snap`,
and `snap 1.1.2` is in `Cargo.lock`.

Vocabulary shape, its shared home across the file and Iceberg destinations, and
the contradictory-combination rules that FR-034 rejects: `research.md`.

**Position on Principle IX**: parquet files are pipeline *output*, not an
rdlt-internal persisted format. Changing the default changes what new runs
write; it does not make previously-written data unreadable. This is a
behaviour change to announce, not a format migration.

---

## 3. Source discovery scope — closing the hole that hid the benchmark defect

**Today** (`crates/rdlt-connector-postgres/src/source/config.rs`):

```rust
/// Absent ⇒ discover ALL tables in `schema`.
#[serde(default)]
pub tables: Option<TableConfig>,   // line 34-36
```

and `tables: []` is **rejected** at line 569 with *"`tables` present but empty
— omit it to discover all"*.

The two states are therefore `Some(non-empty)` = these tables, and `None` = all
tables. **There is no way to say "none"** — which is exactly why the
keep-in-sync benchmark cell silently delivered two extra 1M-row streams on top
of its declared query.

The needed state space is three-valued:

| intent | today | after |
|---|---|---|
| these tables | `tables: [...]` | unchanged |
| every table in the schema | `tables:` omitted | unchanged |
| **no tables — queries only** | **inexpressible** | expressible |
| nothing at all (no tables, no queries) | runs as a silent no-op | rejected at configuration time (FR-011) |

Under D1 (greenfield) the empty-list spelling may be **redefined** to mean
"none" rather than gaining a second spelling alongside it; the exact choice and
its interaction with `#[serde(deny_unknown_fields)]` and the `#[non_exhaustive]`
config-enum policy is in `research.md`.

---

## 4. Benchmark cell and artifact — what proves a comparison is a comparison

**Cell declaration** (`benches/cells/e2e.toml`). Today a cell declares one
verification target:

```toml
[cell.verify]
table = "events_merged"
expected_rows = 1_000_000
```

which is why two unintended 1M-row tables were invisible. FR-009/FR-010 need
the cell to declare its **full expected stream set**, and the harness to reject
a run whose delivered set differs.

**The delivered set needs no new engine channel.** The release CLI already
writes a `RunReport` JSON (`rdlt run <spec> --report <path>`), and the harness
already reads per-table figures out of it — `report_totals` and
`report_table_rows` in `crates/rdlt-bench/src/runner.rs` index
`report["tables"]`. The delivered table set is `report["tables"]` keys.

**Artifact schema** (`crates/rdlt-bench/src/artifact.rs`,
`ARTIFACT_FORMAT_VERSION = 2`):

- FR-035 wants bytes *written to the destination store* per arm. Note
  `RdltSide::bytes` already exists but means something different — "from the
  RunReport's own accounting", i.e. in-memory batch size processed, never
  storage volume. A new, distinct optional field is required; reusing `bytes`
  would silently redefine a recorded metric.
- **No `format_version` bump is needed** for an additive optional field. The
  precedent is in the struct already: `streams: Vec<StreamAttribution>` carries
  `#[serde(default)]`, and the reader rejects only on a version mismatch
  (`artifact.rs:206-209`). The harness is the sole reader and lives in-tree.
- **Deletion candidate under PI2**: `StreamAttribution` is documented as
  *"always empty since subprocess is the only run behavior"* — vestigial state
  left by feature 018's removal of library run mode. It is dead weight in a
  recorded format; this feature should delete it rather than carry it.

**Bars** (`benches/bars.toml`) follow Principle VIII unchanged: at most one per
cell, set below a recorded session floor, each with a `RESULTS.md` policy-log
entry. Increment 1 re-derives the keep-in-sync cell's bar from the corrected
session; post-improvement bars for other cells follow at close-out.

---

## 5. The destination session interface — conditional

`crates/rdlt-connector/src/lib.rs`:

```rust
#[async_trait]
pub trait LoadSession: Send {
    async fn ensure_table(&mut self, schema: &TableSchema, mode: &WriteMode) -> Result<(), DestinationError>;
    async fn write(&mut self, table: &TableName, batch: RecordBatch) -> Result<(), DestinationError>;   // :123
    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError>;
    async fn read_state(&mut self, pipeline: &PipelineId) -> Result<Option<StateDoc>, DestinationError>;
}
```

`write` takes `&mut self`, which forecloses concurrent per-table writes. The
documented contract — *"the engine guarantees `batch` conforms exactly to the
last ensured schema for `table` and that per-table batches arrive in order"* —
is the invariant any change must preserve (FR-041).

**Eight implementors** must move together if this changes: the four bundled
destinations (`postgres`, `duckdb`, `file`, `iceberg`), the testkit's memory and
crash destinations, and two test implementors
(`rdlt-engine/tests/mutation_closures.rs`,
`rdlt-testkit/tests/conformance_negative.rs`).

**This is version-gated, not optional.** CI runs `cargo semver-checks
check-release -p rdlt-core -p rdlt-connector` against `origin/main` as a
**blocking** job with no `continue-on-error`. Under 0.x semver an incompatible
change to `rdlt-connector` requires `0.2 → 0.3` — the window features 014 and
017 recorded and left closed. Per contract PI8 the decision is made when the
design is fixed, and if the parallelism target is reachable without a break the
window stays closed and that is recorded.

Whether a break is actually needed, and the shape if so: `research.md`.

---

## 6. Frozen — the things this feature must not move

| frozen shape | why it cannot move | what pins it |
|---|---|---|
| `_rdlt_id` values (roots and children at every depth) | Persisted. It is what makes nested-subtree merge detect changed children; a changed identity silently re-writes every child row. Spec decision D6 rejects a cheaper hash regardless of measured gain | identity property tests + a byte-identity corpus captured before any shred change (FR-028) |
| Binary wire bytes for every supported column type | The relational destination's encoder is being replaced wholesale (PI2 deletes the old one); byte-identity is the only proof the replacement is faithful | a pin captured **from the old encoder before it is deleted**, covering every type, null and non-null, at representable boundaries (FR-018, PI4) |
| Emitted statement text | Golden pins guard the shared merge core against accidental drift | `crates/rdlt-connector-postgres/tests/golden_sql.rs`, re-pinned deliberately and reviewably wherever an increment changes statements (FR-003) |
| `StateDoc`, receipts, `(load_id, commit_seq)` identity | Exactly-once rests on them | the crash-point sweep over the ten in-scope points (PI5) |

**Ordering obligation**: PI2 says the replaced implementation is deleted in the
same change as its replacement, and PI4 says its output is frozen. These are
compatible in exactly one order — **capture the pin from the old
implementation first, commit it as fixture data, then replace and delete**. An
increment that deletes first has destroyed its own oracle.

---

## 7. Numeric encoding — the one hand-written algorithm retained

Recorded here because PI3 requires the justification to be a fact, not a
preference.

`postgres-protocol 0.6.11` exposes public wire encoders for every scalar this
feature touches — `bool_to_sql`, `int2/int4/int8_to_sql`, `float4/float8_to_sql`,
`text_to_sql`, `bytea_to_sql`, `timestamp_to_sql`, `date_to_sql`, `time_to_sql`,
`uuid_to_sql` — writing directly into a caller-owned `BytesMut`.

It has **no `numeric_to_sql`** (grep over `src/types/mod.rs` returns nothing).
The crate substitutes fail on domain grounds, not preference:

- `rust_decimal` — 96-bit mantissa, ~28 significant digits. Arrow `Decimal128`
  carries up to 38. Adopting it **loses precision**.
- `bigdecimal` / `pg_bigdecimal` — arbitrary precision, but allocate per value,
  which is precisely what the encoder increment exists to remove.

So `numeric_wire_bytes` stays hand-written, is made allocation-free into the
shared buffer, and keeps its existing proptest round-trip oracle against the
crate's own source-side decoder.

`jsonb` is a version byte followed by the UTF-8 document — protocol framing,
not an algorithm, and PI3 admits framing explicitly.

`uuid`: `uuid 1.24.0` is in `Cargo.lock` (via `iceberg` → `apache-avro`) but not
in the postgres crate's tree; `tokio-postgres` also reaches it through the
`with-uuid-1` feature. Whether the hand-written `parse_uuid_text` is replaced —
and whether the accepted textual forms (`urn:uuid:`, braces, unhyphenated)
match `uuid::Uuid::parse_str` exactly, which is a user-visible question —
is resolved in `research.md`.
