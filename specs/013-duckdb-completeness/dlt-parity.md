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
| scd2 absent handling | retires absent records by default; with `merge_key` retires ONLY within delivered scopes (scoped partial-feed scd2) | `absent: keep` DEFAULT, `retire` opt-in; merge_key×scd2 = scoped retirement (MR6 amended, requires retire) | parity (**G1 CLOSED** this feature) + deviation D1 (default stays keep) |
| scd2 open-marker / boundary | `active_record_timestamp` (custom literal instead of NULL) and `x-boundary-timestamp` (caller-supplied boundary) | `scd2.active_record_timestamp` + `scd2.boundary_timestamp` (validated timestamp literals, shared core — both destinations) | parity (**G2 CLOSED** this feature) |
| hard deletes | `hard_delete` column hint | `hard_delete` column (bool IS TRUE / other IS NOT NULL) | parity |
| dedup ordering | `dedup_sort` column hint (asc/desc) | `dedup_sort: {column, order}`; values beat NULL, deterministic ties | parity |
| merge key scoping | `merge_key` (falls back to delete-insert scope semantics) | `merge_key` scope replacement with NULL-not-a-scope + single-unit rule | parity, rdlt's MR4/MR5 semantics are stricter and documented |
| merge, insert-only | `insert-only` merge strategy (append semantics under the merge disposition) | the Append write mode | parity by equivalence (rdlt keeps dispositions orthogonal — insert-only IS append) |
| replace | truncate-and-insert OR insert-from-staging (staging avoids an empty-table window) | ONE transactional truncate+insert with the durable once-per-load guard | parity — rdlt's replace is transactional, so there is no visibility window to avoid |
| native JSON | JSON type support via duckdb JSON | `Json` → native JSON + json_extract proven | parity (flipped this feature) |
| nested data | NO struct columns — nested becomes child tables or JSON | native STRUCT/LIST columns, dot-queryable | **rdlt ahead** |
| duckdb runtime config | `read_only`, `extensions`, `pragmas`, `global_config`/`local_config` passthrough | `extensions: […]` (LOAD) + `settings: {…}` (SET) + `memory_limit`; identifier-validated, injection-shaped keys typed-rejected. `read_only` deliberately absent — a destination writes | parity (**G3 CLOSED**; read_only recorded n/a) |
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
- **G1 — CLOSED (this feature)**: merge_key composes with scd2 as
  SCOPED RETIREMENT (MR6 amended; requires `absent: retire`, typed
  inert-rejection under keep; no scope DELETE ever runs for scd2 —
  history preserved). Landed in the shared core: both destinations at
  once, golden-pinned, differential-proven.
- **G2 — CLOSED (this feature)**: `scd2.active_record_timestamp` and
  `scd2.boundary_timestamp` — validated timestamp literals (RFC3339 /
  `YYYY-MM-DD [HH:MM:SS]`; injection shapes are typed parse errors,
  never SQL). Shared core: both destinations.
- **G3 — CLOSED (this feature)**: `extensions` (LOAD) + `settings`
  (SET) passthrough on the DuckDB destination + CLI YAML; keys/names
  identifier-validated. `read_only` deliberately not exposed — a
  DESTINATION writes; recorded as n/a rather than a gap.

**Verdict**: source-grounded parity holds across the full dlt duckdb
merge + scd2 + runtime-config surface, with rdlt stricter on
determinism (in-load dedup), stricter on safety (validated literals,
typed inert-option rejections), and ahead on native nested types.
Remaining deliberate deviations: D1–D3 above. Remaining
out-of-scope-everywhere: staging datasets, MotherDuck.
