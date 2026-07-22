# dlt Parity Record: DuckDB Destination (feature 013, FR-009)

Compared against pinned **dlt 1.29.0**'s duckdb destination (the
benchmark module's pin; dlt docs + observed behavior of the pinned
container). Format follows the 010 record: per-option comparison,
deviations documented individually.

| Surface | dlt 1.29.0 (duckdb dest) | rdlt (this feature) | Verdict |
|---|---|---|---|
| append / replace | `write_disposition: append/replace` | WriteMode Append/Replace (unchanged) | parity |
| merge, delete-insert | `write_disposition: merge` (default merge strategy `delete-insert`) | `merge_strategy: delete_insert` (default) | parity |
| merge, upsert | `merge_strategy: upsert` — dlt REQUIRES primary_key, warns it "does not deduplicate" the load | `merge_strategy: upsert`, keyed-structured only; rdlt DOES dedup in-load (shared survivor shape, arrival last-wins / dedup_sort) | parity, one recorded improvement: rdlt's in-load dedup is deterministic where dlt delegates to the caller |
| merge, scd2 | `merge_strategy: scd2` with `_dlt_valid_from`/`_dlt_valid_to` | `merge_strategy: scd2` with `_rdlt_valid_from`/`_rdlt_valid_to` (names configurable) | parity (naming prefix differs by product; rdlt adds name overrides) |
| scd2 absent handling | retires absent keys by default on full load | `absent: keep` DEFAULT, `retire` opt-in | **deviation D1 (deliberate)**: rdlt defaults to keep because incremental feeds are partial (008 S6 decision); retire is one option away |
| hard deletes | `hard_delete` column hint | `hard_delete` column (bool IS TRUE / other IS NOT NULL) | parity |
| dedup ordering | `dedup_sort` column hint (asc/desc) | `dedup_sort: {column, order}`; values beat NULL, deterministic ties | parity |
| merge key scoping | `merge_key` (falls back to delete-insert scope semantics) | `merge_key` scope replacement with NULL-not-a-scope + single-unit rule | parity, rdlt's MR4/MR5 semantics are stricter and documented |
| native JSON | JSON type support via duckdb JSON | `Json` → native JSON + json_extract proven | parity (flipped this feature) |
| schema evolution | alter-on-drift | D5 add/widen migrations (unchanged) | parity |
| staging datasets | dlt staging-dataset layer | NOT implemented anywhere in rdlt | out of scope everywhere (spec Out of Scope; future feature, not connector-specific) |
| MotherDuck | supported destination variant | out of scope (spec) | recorded |

## Deviations, individually

- **D1 — scd2 absent default**: dlt retires absent keys on full-feed
  scd2 loads; rdlt defaults `absent: keep` (008 contract S6 — partial
  incremental feeds must not mass-retire) with `retire` opt-in and the
  single-unit safety rule. Deliberate, documented in the scd2 contract.
- **D2 — upsert dedup**: dlt's duckdb upsert warns that staged
  duplicates are the user's problem; rdlt's shared survivor shape
  dedups deterministically before the conflict-update (MR2). Stricter,
  never weaker.
- **D3 — validity column naming**: `_rdlt_*` prefix vs `_dlt_*`; rdlt
  additionally allows overriding the names per table.

No parity gap requires code: every dlt duckdb-destination merge
capability has an rdlt equivalent with equal or stricter semantics.
