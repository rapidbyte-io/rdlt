# Data Model: DuckDB Destination Completeness

## 1. Shared options vocabulary (moves to rdlt-connector-sqlcore, R2)

| Type | Fields (unchanged from 008/010) | Notes |
|---|---|---|
| `MergeStrategy` | `DeleteInsert` (default) \| `Upsert` \| `Scd2` | serde names frozen ("delete_insert" …) |
| `Scd2Options` | `valid_from`, `valid_to`, `absent` (keep\|retire) | 008 vocabulary, incl. validity-column overrides |
| `TableOptions` | `merge_strategy?`, `hard_delete?`, `dedup_sort?`, `merge_key?`, `scd2?` | `deny_unknown_fields`; postgres re-exports as `PgTableOptions` |
| `DestOptions` | `merge_strategy?` (explicit-vs-default preserved — the 011 R5 Option), `tables: BTreeMap<String, TableOptions>` | postgres re-exports as `PgDestOptions` |

Validation (shared, two layers as today): parse-shape errors at
`.options(...)`; open-time existence/collision/capability checks per
destination. Typed errors name table + option + destination.

## 2. MergePlan (shared shapes)

The destination-agnostic plan for one table's commit-unit apply:

| Field | Meaning |
|---|---|
| `identity` | merge key columns (SET semantics, 006) |
| `dedup_order` | survivor ordering (dedup_sort columns + arrival tie-break; values beat NULL) |
| `scope` | merge_key columns for scope replacement (delete before strategy arm) |
| `strategy` | resolved strategy for this table (explicit or default) |
| `hard_delete` | flag column + typing (bool IS TRUE / other IS NOT NULL), decided on the DEDUPED survivor |
| `scd2` | validity columns + absent mode + boundary source |
| `unit_state` | per-table first-staged-unit discipline (single-commit-unit rules shared with scd2 absent-retire and merge_key, 010) |

## 3. MergeDialect (the trait each SQL destination implements)

| Hook | Postgres (extracted verbatim) | DuckDB (new) |
|---|---|---|
| `quote_ident` | today's quoting | DuckDB quoting |
| `dedup_select` | `DISTINCT ON … ORDER BY` | same shape (probe R4) |
| `delete_in` / `scope_delete` | today's SQL | same shape |
| `upsert` | `INSERT … ON CONFLICT DO UPDATE` | same shape vs unique ART index |
| `scd2_close` / `scd2_open` | today's SQL | same shape |
| `tx_boundary_expr` | today's expression | `now()` (tx-stable, R5) |
| `ensure_merge_index` | M5 auto-ensure | `CREATE UNIQUE INDEX` / `CREATE INDEX` |

Contract: dialects own SQL TEXT only; shapes, ordering, validation,
and unit rules live in the shared core. A dialect that cannot honor a
shape exactly returns a typed capability gap (never approximates).

## 4. Capability changes (DuckDB)

| Capability | Before | After | Proof |
|---|---|---|---|
| `json_type` | false (VARCHAR lowering) | true (native JSON via stage→target CAST, R6) | round-trip + `json_extract` cell; flips ONLY if the probe passes |
| others | as declared | audited true-and-proven or false-and-documented | matrix rows |

## 5. Differential oracle (R7)

Feed scripts (shared): append / replace / keyed merge × 3 strategies /
in-load duplicates with dedup_sort / hard_delete flags / scoped loads
(merge_key) / rejection cases (NULL-in-key, keyless dedup, explicit
strategy under append). Equivalence record: per-table canonical rows
(ordered, normalized), typed-error class parity, and the documented
type-affinity table (the only permitted differences).

## 6. Records (as in 011)

- `matrix.md`: option → cells (class: unit / duckdb-live / sweep /
  differential), zero uncited rows at close-out.
- Coverage record: baseline + final + classified exclusions in
  RESULTS.md alongside the 011 record.
- dlt parity record: per-option behavior vs pinned dlt duckdb
  destination, deviations documented individually (010 format).
