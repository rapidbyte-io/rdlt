# rdlt-core

The rdlt vocabulary: the types every side of a pipeline has to agree on.
What a destination persists (the state document, table schemas and their
hashes), what crosses the connector wire (commit metadata and receipts,
cursors, identifiers, logical types), what a run reports back (the run
report, the event feed and its metrics fold, the error taxonomy), and the
one test-only macro production code arms.

**Charter: vocabulary that crosses a boundary, and nothing else.** A type
lives here because more than one side of a boundary must agree on it —
engine and connector, engine and embedder, this engine version and the
documents an older one wrote. Machinery with a single owner is engine
code and lives in `rdlt-engine`, however pure it is: row identity,
collision-safe column and table naming, and schema policy all live there,
not here. Everything else in the workspace — the engine, the SPI, the
connector sdk, the client, the CLI — names this crate's types by their
module paths.

Dependencies stay narrow: `serde` and `serde_json` for the wire forms,
`blake3` for the schema content hash and the short identifier digest,
`thiserror` for the error taxonomy, and an optional `fail` used only under
the `failpoints` feature. Deliberately NOT arrow: the schema vocabulary is
rdlt's own, and mapping it onto arrow types is the engine's job.

## The modules — the crate's table of contents

Every item lives on its canonical module path; the crate root holds no
re-exports.

| module | what it holds |
|---|---|
| `id` | the identifier newtypes `PipelineId`, `LoadId`, `StreamName`, `TableName`, and `SchemaHash` (a content hash that serializes as 64 lowercase hex digits; `InvalidHexId` is its parse error) |
| `cursor` | `Cursor` — an opaque, source-defined extraction position the engine stores and hands back, never interprets |
| `types` | `LogicalType`, the `widen` lattice join, and `DECIMAL_MAX_PRECISION` |
| `schema` | `TableSchema` and its `Column`/`ColumnType`/`Provenance`/`ParentLink`, the evolution `Delta` and its `Change` set, the `system` lineage column names, and the identifier vocabulary a destination declares — `IdentRules` with its legal range and `validate`, and `ident_hash` |
| `commit` | the commit protocol: `CommitMeta`, `CommitReceipt`, `Counters`, `WriteMode`, and the cadence policies `CommitPolicy` and `BatchPolicy` |
| `state` | `StateDoc` and `LastCommit`, `STATE_FORMAT_VERSION`, and the `UnsupportedStateVersion` refusal |
| `event` | `PipelineEvent`, the typed observability feed, and `PartCloseReason` |
| `report` | `Run` (with per-table `Table` and per-stream `Stream` totals), `ResumedFrom`, and `REPORT_FORMAT_VERSION` |
| `metrics` | `Metrics`, the one fold of the event feed into live numbers, and its `Snapshot` (per-stream `Stream`, per-table `Table`, `StreamState`) |
| `error` | `Error`, the embedder-facing taxonomy, and `ContractViolation` |
| `failpoint` | `crash_point!` — the crash-injection macro, inert without the `failpoints` feature |

## Persisted formats

Three persisted documents anchor the crate (the state document, the run
report, the schema hash); every other serde form here is wire-frozen the
same way. That serde layout is a compatibility contract: field names, tags,
and renames change only through an explicit, versioned format migration,
never incidentally. It is the
serde form that is frozen, not the Rust identifiers: a Rust field or type
may be renamed as long as the serialized bytes do not move (`Column`'s
wire key stays `type`; a `SchemaHash` stays 64 hex digits). `cargo
semver-checks` gates the crate's public surface on top of that.

- **The state document** (`state::StateDoc`) is written by the destination
  atomically with the data it covers, in the same transaction, which is
  why correctness survives total loss of the local work directory. It
  carries `STATE_FORMAT_VERSION`; a document newer than the reader knows is
  refused typed (`UnsupportedStateVersion`), never silently reset — a
  silent reset would re-extract from zero and duplicate under Append.
- **The run report** (`report::Run`) is what platforms persist across
  engine upgrades. Its totals equal destination-visible reality: every
  retry, widening, and discard appears. It carries
  `REPORT_FORMAT_VERSION`; fields added since the first version are
  `#[serde(default)]` so older reports still deserialize. Cursor values in
  it are source-defined and may embed sensitive material, so a serialized
  report is handled as carefully as the state store itself.
- **The schema hash** (`id::SchemaHash`, produced by
  `schema::TableSchema::content_hash`) is a blake3 digest over the
  schema's canonical JSON under a fixed domain prefix. Column order and
  provenance both participate, so the engine only ever appends columns,
  and a provenance-only change is a new schema version. The hash is
  recorded per table in the state document and as the `from`/`to` of
  every schema delta.

## The laws the vocabulary carries

- **Widening is a lattice, not a cast.** `types::widen` is the least upper
  bound: `Json` is the top, `Utf8` absorbs every textable type, the numeric
  chains are `Int64 → Float64 → Utf8` and `Int64 → Decimal → Utf8`, and
  there is deliberately no `Float64 → Decimal` edge (NaN, infinities, and
  the exponent range do not fit). Nothing narrows; a value that cannot be
  stored at the declared type is refused and counted, never coerced. The
  join's laws — commutativity, idempotence, associativity, monotonicity —
  are property-tested.
- **Schema change is additive only.** `schema::Change` has exactly three
  arms: create table, add a nullable column, widen a column along the
  lattice. There is no drop, rename, or narrowing, because those lose data
  or break readers; the engine's schema policy decides whether even these
  three are allowed.
- **Commit cadence is a disjunction.** `commit::CommitPolicy` and
  `commit::BatchPolicy` fire when ANY set threshold is reached; an unset
  threshold never fires, and a commit policy with no threshold at all is
  refused by `check` rather than honoured, since it would hold a whole run
  uncommitted.
- **Errors are matched by variant, never by text.** `error::Error` has one
  arm per operator action (the CLI's exit codes map from those arms), is
  fully serde-representable, and renders stable display strings — but no
  code classifies by grepping a rendered message.
- **The event feed and its metrics are advisory; the report is exact.**
  `metrics::Metrics` is the single fold shared by the CLI's live display,
  an embedder's metrics endpoint, and any telemetry bridge, so a rate
  computed wrong is fixed once. A lagging subscriber loses the oldest
  events rather than slowing the pipeline, so final totals are always
  taken from `report::Run`, never from the live fold.

## Features

- `failpoints` — enables the `fail` dependency behind `crash_point!`.
  Test infrastructure for crash sweeps; off by default, never enabled by a
  release build, and the macro expands to nothing without it.
