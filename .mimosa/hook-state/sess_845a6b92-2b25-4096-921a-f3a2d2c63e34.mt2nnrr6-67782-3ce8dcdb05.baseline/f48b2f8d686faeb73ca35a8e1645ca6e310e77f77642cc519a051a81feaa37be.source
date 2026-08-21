# rdlt-core

The rdlt vocabulary: pure data contracts and semantic laws. Every type that
is persisted, reported, or crosses a wire, plus the pure functions that
define rdlt's data semantics — the widening lattice, row identity, schema
hashing, and identifier naming.

**Charter: pure data and pure functions only.** If it needs tokio, I/O, or
arrow compute, it does not belong here. The dependency set stays narrow
deliberately: serde for serialization, blake3 for hashing, thiserror for the
error taxonomy, and an optional `fail` dependency used solely for
crash-point injection under the `failpoints` feature — never in a release
build.

## Semver-sacred

`cargo semver-checks` gates this crate, and the persisted formats it defines
(`StateDoc`, schema hashes, `RunReport`) are byte-stable. Their on-disk
serialization is a compatibility contract that changes only through an
explicit, versioned format migration — never incidentally.

## What lives here

| Area | Types |
|---|---|
| Identity | `PipelineId`, `LoadId`, `StreamName`, `TableName` |
| Schema | `TableSchema`, `ColumnDef`, `ColumnType`, `LogicalType`, `SchemaDelta`, `SchemaChange` |
| Semantics | the widening lattice (`Int64` → `Float64` → `Utf8` → `Json`), row identity, schema hashing, `normalize_ident` |
| Policy | `SchemaPolicy`, `PolicyAction` (Evolve / Freeze / DiscardRow / DiscardValue), `WriteMode`, `CommitPolicy` |
| Progress | `Cursor`, `StateDoc`, `CommitMeta`, `CommitReceipt`, `CommitCounters` |
| Outcome | `RunReport`, `PipelineEvent`, `RdltError` and its typed taxonomy |

## The error taxonomy

`RdltError` is the one taxonomy the whole workspace classifies into, and it
is matched by VARIANT, never by rendered text — a test that greps an error
message is forbidden by the project's constitution, because it pins the
prose instead of the behaviour.

## Widening is a lattice, not a cast

A column's type only ever widens, and only along declared edges. Nothing
narrows, and a value that cannot be stored at the declared type is refused
and counted rather than silently coerced — an ELT engine that guesses is
worse than one that stops.
