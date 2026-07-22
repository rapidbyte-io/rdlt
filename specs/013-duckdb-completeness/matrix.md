# Traceability Matrix: DuckDB Destination Options (feature 013)

The 011 rules applied to the DuckDB destination surface: every
user-settable option → behavioral cells (defaults observed, values
proven, typed errors pinned). Runtime classes: `unit` (no I/O),
`duck` (embedded DuckDB through the engine), `diff` (cross-destination
differential, container), `sweep` (failpoints). Cells live in
`crates/rdlt-connector-duckdb/tests/` unless prefixed; sqlcore parse
cells in `crates/rdlt-connector-sqlcore/src/options.rs`.

**Zero uncited rows at close-out.** The postgres matrix
(`specs/011-connector-verification/matrix.md`) remains the postgres
citation source; rows below cite the DUCKDB proof.

## Destination handle

| Row | Documented behavior | Cells | Class |
|---|---|---|---|
| `path` | open/create database file | every cell; `conformance::duckdb_destination_is_conformant` | duck |
| `memory_limit` | caps DuckDB buffer memory | 003-era cell retained (`DuckDb::memory_limit`); bench cells run with defaults | duck |
| `.options(...)` parse layer | shared vocabulary, `deny_unknown_fields`, typed field-naming errors | sqlcore `options::tests::validation_matrix_names_the_field` (shared with postgres — SM5) | unit |

## merge_strategy (destination-wide + per-table)

| Row | Documented behavior | Cells | Class |
|---|---|---|---|
| default (unset) | delete_insert; never rejects under any mode | `strategies::explicit_strategy_under_append_is_typed` (append-ok half); `conformance::merge_in_batch_dedup_is_last_wins` | duck |
| `delete_insert` | matched keys replaced; totals = source truth | `strategies::delete_insert_replaces_matched_keys`; `differential::differential_delete_insert_redelivery` | duck, diff |
| `upsert` | in-place update, no delete-visibility window; keyed-structured only; unique index auto-ensured | `strategies::upsert_updates_in_place_and_composes_with_hard_delete`; `upsert_on_shredded_stream_is_typed`; `upsert_over_preexisting_duplicates_is_typed` (M3 naming key cols); probe `probe_on_conflict_against_unique_index` | duck |
| `scd2` | history close/open, one boundary per unit, no churn on unchanged keys | `strategies::scd2_history_closes_and_opens`; `scd2_boundary_is_one_instant_per_unit` (S5); `differential::differential_scd2_history` | duck, diff |
| EXPLICIT strategy under append/replace | typed error (011 R5); unconfigured default never rejects | `strategies::explicit_strategy_under_append_is_typed`; `differential::differential_rejections_are_class_identical` | duck, diff |
| crash/rerun convergence per strategy | exactly-once outcomes after crashes at every fail point | `sweep::strategy_arms_survive_crash_sweep` (armed-fire pinned) | sweep |

## hard_delete

| Row | Documented behavior | Cells | Class |
|---|---|---|---|
| bool column | `IS TRUE` deletes the key; keep-side NULL-safe | `strategies::upsert_updates_in_place_and_composes_with_hard_delete`; `differential::differential_upsert_with_hard_delete_and_dedup` | duck, diff |
| non-bool column | `IS NOT NULL` deletes | `refinements::non_bool_hard_delete_flag_uses_is_not_null` | duck |
| with scd2 | typed parse rejection (S8) | sqlcore `validation_matrix_names_the_field` | unit |

## dedup_sort

| Row | Documented behavior | Cells | Class |
|---|---|---|---|
| ordered survivor; values beat NULL; ties keep arrival last-wins | MR1/MR2 | `refinements::dedup_sort_orders_survivors_values_beat_null`; probe `probe_distinct_on_ordering_semantics` | duck |
| keyless / missing column / key-constant / flag-collision | typed (MR6, identical postgres wording — shared validator) | `refinements::refinement_option_misuse_is_typed` | duck |

## merge_key

| Row | Documented behavior | Cells | Class |
|---|---|---|---|
| delivered scopes replaced wholesale; others untouched; NULL not a scope | MR3/MR4 | `refinements::merge_key_scope_replacement`; `differential::differential_merge_key_scope_and_null_scope` | duck, diff |
| split feed across units | typed on the second unit (MR5, shared message + D3 replay re-mark) | `refinements::merge_key_split_feed_is_typed_on_the_second_unit`; `sweep` (merge_key arm) | duck, sweep |

## scd2 block

| Row | Documented behavior | Cells | Class |
|---|---|---|---|
| validity column defaults + overrides | `_rdlt_valid_from`/`_rdlt_valid_to`; overrides via the shared options | `strategies::scd2_history_closes_and_opens` (defaults); postgres override cell carries the shared parse path (SM5) | duck, unit |
| `absent: keep` (default) | absent keys keep their active version | `differential::differential_scd2_history` (k=2 untouched) | diff |
| `absent: retire` | absent keys retire at the boundary; single-unit rule | `strategies::scd2_absent_retire_full_feed`; `sweep` (scd2_retire arm) | duck, sweep |
| validity-name collision / equal names / scd2 opts without strategy | typed (shared validator) | sqlcore `validation_matrix_names_the_field` | unit |
| `active_record_timestamp` + `boundary_timestamp` (013 G2) | ZONE-EXPLICIT RFC3339 only (zone-less typed-rejected); marker ≠ boundary enforced; active predicate is MARKER-TOLERANT (`IS NULL OR = marker` — pre-existing NULL-open rows keep working); injection shapes typed | `strategies::scd2_active_marker_and_boundary_override`; sqlcore validation matrix (zone-less + equality rejects); golden pins `pin_scd2_markers_and_boundary` | duck, unit |
| merge_key × scd2 = scoped retirement (013 G1, MR6 amended) | absent keys retire only within delivered scopes; requires `absent: retire` (typed under keep); history never deleted | `strategies::scd2_scoped_retirement_by_merge_key`; `scd2_merge_key_requires_retire`; `differential::differential_scd2_scoped_retirement`; pin `pin_scd2_scoped_retirement` | duck, diff, unit |
| `extensions` / `settings` passthrough (013 G3) | validated + applied eagerly AND replayed on every session connection (cloned connections are fresh DuckDB sessions); identifier-validated, typed refusals | `probes::g3_settings_and_extensions_passthrough` (session-scoped setting observed on a session connection); CLI `duckdb_options_pass_through_the_yaml` | duck, unit |

## Probe outcomes (R4/R5/R6 — all PASS; zero capability gaps)

| Assumption | Probe | Outcome |
|---|---|---|
| DISTINCT ON survivor shape | `probes::probe_distinct_on_ordering_semantics` | PASS — shared dedup shape verbatim |
| ON CONFLICT vs plain unique index | `probes::probe_on_conflict_against_unique_index` | PASS — M5 index pattern carries |
| tx-stable now() | `probes::probe_now_is_transaction_stable` | PASS — S5 boundary holds |
| IS DISTINCT FROM NULL-safety | `probes::probe_is_distinct_from_null_semantics` | PASS |
| bundled JSON extension | `probes::probe_bundled_json_extension` | PASS — json_type flipped |
| UPDATE…FROM / NOT EXISTS | `probes::probe_update_from_and_not_exists` | PASS |

## Capability audit (T010 — every field true-and-proven or false-and-documented)

| Capability | Value | Proof / reason |
|---|---|---|
| `merge` | true | strategy cells above; 006 conformance |
| `structs` | true | `conformance::end_to_end_nested_sync_into_duckdb` (struct dot-query) |
| `scalar_lists` | true | conformance suite (list lowering) |
| `json_type` | **true (flipped this feature)** | `json::json_columns_land_native_and_extract` (declared type JSON + json_extract), `json_columns_merge_by_key`; stage stays VARCHAR, publish SELECT casts (R6) |
| `decimal` | true | `sql_type` DECIMAL(p,s); conformance types |
| `ident_rules` | default | hashed stage names bound identifier length |

## Recorded deviations (destination-owned, none semantic)

1. **scd2 validity DDL**: DuckDB rejects `ADD COLUMN … NOT NULL`; the
   `valid_from` column adds as `TIMESTAMPTZ DEFAULT now()` without the
   belt constraint (the insert arm always supplies the boundary).
   Postgres keeps NOT NULL. Semantics identical — proven by the
   differential scd2 cell.
2. **Arrival order**: DuckDB stages use `rowid` (append order) instead
   of a real arrival column — the 006 finding-#7 determinism decision,
   now expressed through the dialect's `arrival_order()` hook.
3. **Unique-index error classification**: DuckDB has no SQLSTATE; the
   M3 duplicate-keys diagnosis fires only on constraint-violation
   message shapes, other failures surface as themselves (review
   finding 6). Mid-branch `rdlt_ix_`-named unique indexes are dropped
   before the `rdlt_ux_` create (review finding 7).
