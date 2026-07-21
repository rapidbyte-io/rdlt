# Data Model: Merge Refinements

Zero engine/connector entities change. Both controls are per-table
POSTGRES destination options on the existing 008 surface.

## PgTableOptions additions

| Field | Type | Default | Rules |
|---|---|---|---|
| `dedup_sort` | `{column: String, order: asc\|desc}` | absent (last-wins) | column must exist on the table; not the hard_delete flag or an scd2 validity column; keyed structured tables only; `order` REQUIRED |
| `merge_key` | `[String]` (non-empty, duplicate-free) | absent (identity-only merge) | columns must exist; disjoint from hard_delete flag / scd2 validity columns; keyed structured tables only; invalid with `merge_strategy: scd2` |

## Survivor selection (dedup_sort)

| Situation | Survivor |
|---|---|
| absent | last-arriving row (unchanged, `__rdlt_arrival DESC`) |
| `desc` | greatest `column` value; NULLs lose to values |
| `asc` | least `column` value; NULLs lose to values |
| tie / all NULL | last-arriving among the tied (deterministic, replay-stable) |

The surviving row (and only it) feeds every downstream decision:
hard-delete flag, upsert content, SCD2 change detection (FR-003).

## Scope replacement (merge_key)

| Item | Semantics |
|---|---|
| Scope value | tuple of the merge_key columns; any NULL ⇒ "not a scope" (matches nothing, both sides) |
| Delete set | target rows whose scope equals a stage scope NOT yet receipted for this load |
| Untouched | every scope absent from the load's stages (FR-005) |
| Composition | scope delete runs first; the strategy arm (identity delete-insert / upsert, hard_delete) runs unchanged after |

## New auxiliary table

`_rdlt_scope_receipts (load_id TEXT, table_name TEXT, scope TEXT,
PRIMARY KEY (load_id, table_name, scope))` — written inside the publish
transaction; a scope is deleted at most once per load (multi-commit-unit
loads are sound); other loads' receipts pruned at a load's first touch.
Redelivery of a committed unit never reaches merge SQL (D3), so receipts
never double-fire.

## Error taxonomy additions (each its own typed error, names table + column)

| Condition | When |
|---|---|
| dedup_sort/merge_key column absent from the table | open (ensure_table) |
| collision with hard_delete flag or scd2 validity column | open |
| dedup_sort or merge_key on a shredded (identity) stream | open |
| merge_key with `merge_strategy: scd2` | open |
| empty/duplicate merge_key list, empty column, bad order token | config parse |
