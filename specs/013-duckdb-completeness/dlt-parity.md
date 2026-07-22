# dlt Parity Record: DuckDB Destination (feature 013, FR-009)

Compared against pinned **dlt 1.29.0**'s duckdb destination —
SOURCE-GROUNDED (audited against the dlt checkout's
`destinations/impl/duckdb/{factory,duck,configuration}.py` +
`destinations/sql_jobs.py`, post-review revision). Format follows the
010 record: per-option comparison, deviations documented individually.

dlt's declared duckdb capabilities: merge strategies `delete-insert`,
`upsert`, `scd2`, `insert-only`; replace strategies
`truncate-and-insert`, `insert-from-staging`; JSON native; decimal;
timestamp precision to ns; loader formats insert_values/parquet/jsonl.

| Surface | dlt 1.29.0 (duckdb dest) | rdlt (this feature) | Verdict |
|---|---|---|---|
| append / replace | `write_disposition: append/replace` | WriteMode Append/Replace (unchanged) | parity |
| merge, delete-insert | `write_disposition: merge` (default merge strategy `delete-insert`) | `merge_strategy: delete_insert` (default) | parity |
| merge, upsert | `merge_strategy: upsert` — dlt REQUIRES primary_key, warns it "does not deduplicate" the load | `merge_strategy: upsert`, keyed-structured only; rdlt DOES dedup in-load (shared survivor shape, arrival last-wins / dedup_sort) | parity, one recorded improvement: rdlt's in-load dedup is deterministic where dlt delegates to the caller |
| merge, scd2 | `merge_strategy: scd2` with `_dlt_valid_from`/`_dlt_valid_to` | `merge_strategy: scd2` with `_rdlt_valid_from`/`_rdlt_valid_to` (names configurable) | parity (naming prefix differs by product; rdlt adds name overrides) |
| scd2 absent handling | retires absent records by default; with `merge_key` retires ONLY within delivered scopes (scoped partial-feed scd2) | `absent: keep` DEFAULT, `retire` opt-in; merge_key×scd2 REJECTED (MR6) | **deviation D1 (deliberate) + gap G1** — see below |
| scd2 open-marker / boundary | `active_record_timestamp` (custom literal instead of NULL) and `x-boundary-timestamp` (caller-supplied boundary) | NULL-only open marker; boundary is always the transaction timestamp | **gap G2 (minor, recorded)** |
| hard deletes | `hard_delete` column hint | `hard_delete` column (bool IS TRUE / other IS NOT NULL) | parity |
| dedup ordering | `dedup_sort` column hint (asc/desc) | `dedup_sort: {column, order}`; values beat NULL, deterministic ties | parity |
| merge key scoping | `merge_key` (falls back to delete-insert scope semantics) | `merge_key` scope replacement with NULL-not-a-scope + single-unit rule | parity, rdlt's MR4/MR5 semantics are stricter and documented |
| merge, insert-only | `insert-only` merge strategy (append semantics under the merge disposition) | the Append write mode | parity by equivalence (rdlt keeps dispositions orthogonal — insert-only IS append) |
| replace | truncate-and-insert OR insert-from-staging (staging avoids an empty-table window) | ONE transactional truncate+insert with the durable once-per-load guard | parity — rdlt's replace is transactional, so there is no visibility window to avoid |
| native JSON | JSON type support via duckdb JSON | `Json` → native JSON + json_extract proven | parity (flipped this feature) |
| nested data | NO struct columns — nested becomes child tables or JSON | native STRUCT/LIST columns, dot-queryable | **rdlt ahead** |
| duckdb runtime config | `read_only`, `extensions`, `pragmas`, `global_config`/`local_config` passthrough | `memory_limit` only | **gap G3 (practical)** |
| type width/precision | int precision hints (TINYINT…HUGEINT), timestamp precision to ns | engine logical types (Int64, TIMESTAMPTZ µs) | engine-level vocabulary difference, not duckdb-specific; recorded |
| `unique` column hint | UNIQUE in DDL | only merge-identity unique indexes (M5) | minor, recorded |
| schema evolution | alter-on-drift | D5 add/widen migrations (unchanged) | parity |
| staging datasets | dlt staging-dataset layer | NOT implemented anywhere in rdlt | out of scope everywhere (spec Out of Scope; future feature, not connector-specific) |
| MotherDuck | supported destination variant | out of scope (spec) | recorded |

## Deviations and gaps, individually

- **D1 — scd2 absent default**: dlt retires absent records by default;
  rdlt defaults `absent: keep` (008 contract S6 — partial incremental
  feeds must not mass-retire) with `retire` opt-in and the single-unit
  safety rule. Deliberate, documented in the scd2 contract.
- **D2 — upsert dedup**: dlt's duckdb upsert leaves staged duplicates
  to the caller; rdlt's shared survivor shape dedups deterministically
  before the conflict-update (MR2). Stricter, never weaker.
- **D3 — validity column naming**: `_rdlt_*` prefix vs `_dlt_*`; rdlt
  additionally allows overriding the names per table.
- **G1 — scd2 × merge_key (scoped retirement)**: dlt composes them —
  with a merge_key, scd2 retires absent records ONLY within the
  delivered scopes, enabling partial-feed scd2. rdlt rejects the
  combination (010 MR6: "scd2 retirement has its own absence policy").
  The 010 rejection predates this composition being understood as
  coherent; dlt's source shows a sound semantics. CANDIDATE FEATURE —
  re-examine MR6 rather than treating the rejection as permanent.
- **G2 — scd2 open-marker/boundary options**: dlt's
  `active_record_timestamp` (a literal like 9999-12-31 instead of NULL,
  useful for BI tools that can't range-query NULLs) and caller-supplied
  boundary timestamps. Small, additive, shared-core-shaped (would land
  in sqlcore for both destinations at once).
- **G3 — duckdb runtime configuration**: dlt passes through
  `extensions`, `pragmas`, `read_only`, and duckdb config maps; rdlt
  exposes only `memory_limit`. Practical gap for an embedded analytics
  destination (spatial/httpfs extensions, thread caps). Small surface
  on the existing `SET`-statement seam.

**Verdict**: merge-strategy CORE parity holds (with rdlt stricter on
determinism and ahead on native nested types); three enumerated gaps
(G1–G3) are recorded as future-feature candidates, none blocking — and
none silently: this record is the tracking artifact.
