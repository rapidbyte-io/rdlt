# Phase 1 Data Model: Audit Remediation

**Feature**: 020-audit-remediation | **Date**: 2026-07-26

Only the shapes this feature *changes* are listed. Everything not named here is
unchanged, and three things are unchanged **by requirement**: the WAL format
(v2), emitted `_rdlt_id` values, and the golden SQL pins.

Each entry states whether the shape is **persisted** (and therefore governed by
Principle IX), **public** (and therefore governed by semver), or **internal**.

---

## 1. `StateDoc` — persisted, public, **versioned change**

`rdlt-core/src/state.rs`. The pipeline's per-stream recovery document, written
by every destination at every commit.

| field | before | after |
|---|---|---|
| `format_version` | `1`, and **never assigned anywhere in rdlt-engine** | `2`, assigned where the recovered document is adopted |
| `cursors` | `BTreeMap<StreamName, Cursor>` | unchanged |
| `schema_hashes` | `BTreeMap<TableName, String>` — written at every delta, **read nowhere** | **DELETED** |
| `schemas` | — | `BTreeMap<TableName, TableSchema>`, `#[serde(default)]` |

**Why the digest could not stay.** `schema_hashes` holds a blake3 digest of the
whole canonical `TableSchema`. A digest can prove *inequality* and nothing
else, so it cannot produce the `AddColumn` / `WidenColumn` the policy layer
resolves on — and inequality alone is precisely the FR-031 false-positive trap,
because column order and provenance both participate in the hash.

**Semantics of `schemas`.** It is the pipeline's **established** schema per
table: the monotone **union** of every schema ever ensured, not "what the last
run ensured". `apply_delta` merges — baseline columns first in baseline order
with their types joined against the incoming ones, then incoming columns absent
from the baseline appended in observed order. Without the union the
false-positive merely moves one run later: `{id,v}` → `{id}` → `{id,v}` would
report `v` as new on the third run. The union is also the truthful model of the
destinations, whose DDL is additive and never drops a column.

**Migration.** `#[serde(default)]` means a v1 document still loads, producing
an empty baseline for exactly one post-upgrade run, which then re-establishes
it. `StateDoc` sets no `deny_unknown_fields`, so a v1 document's stale
`schema_hashes` key is ignored. `check_readable`'s `found > supported` gate is
unchanged — it already refuses a newer document typed. The empty-baseline case
under a non-Evolve policy is **decided, not defaulted**: it is refused with a
typed variant naming the pipeline, because otherwise the first post-upgrade run
is a silent one-run Freeze bypass.

**Cost, recorded rather than gated.** A serialized `ColumnDef` is ~100 bytes, so
a 20-table / 50-column pipeline puts ~100 KB into a document rewritten on every
commit; under Iceberg that document is a table property, so this is metadata
growth per commit. The close-out records a measured byte count for the widest
bench cell. No threshold gate is added.

**Breaking**: yes — public field of a public type in a semver-sacred crate.
See the plan's Complexity Tracking.

---

## 2. `StreamBaseline` — internal, new

`rdlt-engine`. Read-only, per stream, constructed once at run start from
`StateDoc.schemas` and reachable from the shred context.

```text
StreamBaseline {
    schemas:     Arc<BTreeMap<TableName, TableSchema>>,
    established: bool,     // false only when the pipeline has no prior baseline
}
```

The registry is **not** seeded from it. `SchemaRegistry` keeps its within-run
semantics and the emitted `LoadItem` stream is unchanged; the baseline governs
*policy*, never emission. On every drain both paths compute:

```text
emit        = registry.diff(&observed)                     // drives LoadItems — unchanged
established = union(registry.get(&table), baseline.get(&table))
governed    = diff_against(established, &observed)         // drives policy
```

- `governed` empty → nothing is policed; every change in `emit` is Evolve.
- `governed == [CreateTable]` → exempt iff bootstrapping, else
  `policy.action_for(&table, None)`.
- otherwise → per governed change, as today.

This is the corrected shape. Governing only the `CreateTable` arm — the first
design — fired Freeze on columns the pipeline itself had established, from
drain 2 of every second run onward.

---

## 3. `SchemaPolicy` resolution — public behaviour, no shape change

`rdlt-core/src/policy.rs`. `action_for` resolves a **child** table through its
parent chain before falling through to the default: `per_table[child]`, then
`per_table[root]`, then `default`. No field changes. Without this, freezing a
parent does not freeze the child table a new nested collection creates, and
FR-030 is unmet for the exact policy shape the existing tests use.

---

## 4. `FileProgress` — persisted, **additive, no version bump**

`rdlt-connector-file/src/source/types.rs`.

| field | change |
|---|---|
| `row_groups_hash` | **NEW** — `Option<String>`, `#[serde(default, skip_serializing_if = "Option::is_none")]` |

A lowercase blake3 hex digest over the **consumed prefix only**: for each row
group `0..done`, the loop index, `num_rows`, `total_byte_size`, `num_columns`,
then per column chunk the dictionary page offset, data page offset and
compressed size — all read from the footer both sides already parse, so it
costs **zero additional footer parses**.

`CURSOR_FORMAT_VERSION` stays at **1**. `etag` and `tail_hash` were added to
this same struct with this same attribute shape and no bump; `format_version`
is serialized unconditionally, so bumping would have changed jsonl and csv
cursor documents that this fix does not touch — and, combined with a
version-refusal gate, would have made the increment non-revertible.

**Migration note**: additive optional field, no version change; parquet entries
carry no integrity value until the next checkpoint rewrites them.

**Invariant, stated at the site**: the descriptor covers groups `0..done`
**unconditionally** — a resume with no recorded expectation still builds it, so
the value written is comparable on the next run. Building it only when
verification ran was the defect that would have poisoned every pre-existing
cursor.

---

## 5. `FileTask` resume check — internal

`rdlt-connector-file/src/source/types.rs`. `tail_check: Option<(u64, String)>`
is **replaced** (greenfield — not extended) by:

```text
ResumeCheck =
    | TailBytes    { window: u64, hash: String }   // jsonl, csv — unchanged behaviour
    | RowGroupPrefix { groups: u64, hash: String } // parquet — new
```

Emitted only when `done_units > 0`, mirroring jsonl's existing arming filter.
Without that filter a hand-edited cursor reaches `done - 1` and underflows;
a persisted document an operator can edit must fail typed, never arithmetically.

---

## 6. `Drift` — internal, new

`rdlt-connector-iceberg/src/dest/schema.rs` — deliberately the module that
assigns field IDs, so the invariant and its ID-insensitivity live together. The
drift *policy* stays in `ensure.rs`.

```text
Drift =
    | Type         { column, wanted, live }
    | NestedFields { column, … }
    | Nullability  { column }
```

Comparison is recursive and **ignores catalog-assigned field IDs**. It must be:
`NestedField` derives `PartialEq` over all fields including `id`, so for any
catalog that normalizes IDs on create, an unchanged struct-bearing stream fails
its *second* ensure as though the user had made contradictory changes.

Nullability is **asymmetric**: `live.required && !wanted.required` is drift —
the write cannot honour it; the reverse is tolerated.

---

## 7. Byte-budget channel core — public (SPI), additive

`rdlt-connector/src/channel.rs` gains the one generic implementation;
`crates/rdlt-engine/src/runtime/channel.rs` is **deleted** in the same change
(greenfield, D17).

```text
trait ByteSized      { fn byte_size(&self) -> usize }
struct ByteTx<T>     // manual Clone; async send
struct ByteRx<T>     // async recv -> Option<Permitted<T>>; close()
struct Permitted<T>  // carries the budget permit with the item
```

Parameterized over what actually differed between the twin implementations: the
message cap, sender `Clone`-ability, `ByteSized`-driven sizing, and the
close-wake. Additive to the SPI's public surface. This is a correctness
invariant (backpressure accounting) that has been hand-maintained in two places
since it was first recorded as a deferral.

---

## 8. Smaller shape changes

| shape | crate | change | kind |
|---|---|---|---|
| `RestConfig.request_timeout_secs` | rest | **NEW** `u64`, `#[serde(default)]` = 300; `0` rejected at validation (0 must not mean "disabled") | public, additive |
| `TokenState { generation, cached }` | rest | replaces `Option<CachedToken>` inside the existing single-flight mutex; `attach`/`send` thread `Option<u64>` | internal |
| `DestFormat::ALL` | file | the extension set ownership is derived from, beside `extension()` so the exhaustive match forces a new variant to be considered | internal |
| `Scan::Discard` | engine | new sibling of `Scan::Nothing`, returned when a manifest was read but produced nothing replayable, so the residue is cleared | internal |
| `CliError::Io` | cli | splits file I/O out of `Usage`; `Internal` and the catch-all both map to 70 (EX_SOFTWARE), `Io` to 74 (EX_IOERR) | public (exit codes) |
| `DestSpec::File` | facade | struct variant → newtype `File(Box<FileDestConfig>)`, matching the Iceberg arm; the pg and duckdb variants **cannot** follow (both connectors are builder-shaped) and are re-recorded with a trigger | public, breaking-shaped but pre-publish |
| `parse_decimal` | engine | signature takes `precision`; rejects values whose scaled magnitude reaches `10^precision` | internal |
| `build_batch` | engine | returns a misfit count alongside the batch — a **positional** count of present-input/NULL-output cells, never a difference of totals | internal |
| `LoadItem::Discarded` | engine | one new construction site; all four sites updated together | internal |
| `WalRecord::Segment.rows` | engine | gains a consumer (a pass-1 replay cross-check that warns and degrades to re-extraction on mismatch) rather than being deleted; **no** WAL version bump | persisted, unchanged shape |

---

## What deliberately does **not** change

- **`_rdlt_id` and every other emitted identity byte.** Verified against the
  frozen `shred_identities.txt` corpus; no case is added to it in the
  increment that touches the shred path.
- **The WAL format (v2).** The row-count cross-check reads a field that is
  already written.
- **Golden SQL pins.** The sqlcore moves are required to keep them
  byte-identical; the two helpers that move gain the golden pin they lack
  today.
- **`DiscardReason` as a typed enum.** It is the better end state, but making
  the policy-versus-representability distinction carriable would be a breaking
  public change, and expressing it as two free-form strings would leave
  substring-matching as the only way to separate them — which Principle V
  forbids. The distinction is deliberately **not made** in this feature, and
  recorded as a named deferral with the trigger "the next feature that opens
  the version window for another reason".
- **`build.rs:184`'s explicit-null-becomes-empty-list.** Changing it would
  turn an explicit JSON null in a list column into NULL — a data-visible change
  needing its own red-before-green pin and its own close-out line. Recorded as
  a residual, not smuggled into the misfit-counting change.
