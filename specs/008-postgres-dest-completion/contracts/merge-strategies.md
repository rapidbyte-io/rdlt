# Contract: Merge Strategies (delete-insert / upsert) + Hard Delete + Indexes

Strategies are DESTINATION configuration for executing the engine's
frozen `Merge { key }` write mode — the connector SPI sees nothing new.
The 006 keyed-structured acceptance rules (merge-structured.md) are
unchanged and apply to every strategy.

## Rules

| # | Rule |
|---|---|
| M1 | With no strategy configured, behavior is byte-identical to pre-008: atomic delete-insert inside the publish transaction, arrival-order last-wins dedup, D3 idempotent receipts. |
| M2 | *(amended, review F4 2026-07-21)* `upsert`: KEYED STRUCTURED streams only — matched keys update in place, new keys insert via one conflict-update statement from the deduped stage, inside the same publish transaction. Exactly-once and idempotent re-commit hold identically; the strategy is swept by the crash sweep (every registered fail point, both occurrence passes, armed-fire pins). Shredded streams keep delete-insert: their `_rdlt_id` is a content hash for keyless streams (updates mint new ids — conflict-update can never match), and the destination cannot distinguish keyed from keyless shredded streams. |
| M3 | Upsert REQUIRES a unique index on the merge key; `ensure_table` creates it (`IF NOT EXISTS`, deterministic name). If creation fails because the existing table already violates uniqueness, the error is typed and NAMES the key columns (SQLSTATE 23505 shape). |
| M4 | `hard_delete: <column>` (per ROOT table, delete-insert or upsert — not scd2; configuring it on a CHILD table is a typed ensure-time rejection, review F6): rows whose flag is set (boolean → `IS TRUE`, other types → `IS NOT NULL`) DELETE their key at the destination and are EXCLUDED from insertion; the flag decision comes from the DEDUPED last-wins row (review F3 — a root flagged then re-created in the same load survives with its subtree); unflagged rows merge normally; deleting a never-loaded key is a no-op. Column existence validates at ensure time, errors naming the column. |
| M5 | Every merge-mode table gets a supporting index on its merge identity (`_rdlt_id` / `_rdlt_root_id` for shredded, key columns for keyed structured) — deterministic names, idempotent creation. The unindexed-scan elimination is MEASURED once (drop-index baseline vs indexed, same session) and recorded as a scoreboard entry; no new gate. |
| M6 | Strategy changes between runs on the same table are allowed when the table state supports them; upsert onto a table with duplicate keys fails per M3. Append and Replace modes are untouched by strategy configuration. |
| M7 | *(amended, review F4)* Upsert on ANY shredded (identity-carrying) stream is rejected typed at ensure time — the message names delete-insert as the strategy for shredded streams. Delete-insert keeps its identity-based behavior. |

## Conformance

Update-heavy convergence per strategy (one row per key, newest wins,
three stable re-runs); hard-delete exact totals + redelivery no-op;
23505 typed error cell; upsert crash sweep with armed-fire pins;
scoreboard measurements recorded in benches/RESULTS.md.

## Amendment (feature 010)

`specs/010-merge-refinements/contracts/merge-refinements.md` (MR1–MR8)
adds two per-table options to this surface: `dedup_sort` (ordered
in-load survivor selection through the shared dedup shape — the
"last wins" wording above becomes "last wins UNLESS dedup_sort is
declared") and `merge_key` (scope replacement before the strategy arm,
single-commit-unit rule mirroring S6). The M-rules stand unchanged.

## Amendment (feature 011, R5)

An EXPLICITLY configured `merge_strategy` (destination-wide or
per-table) under an append/replace write mode is a typed error at open
naming table + mode — closing the recorded silent-inert footnote. The
unconfigured default (`delete_insert`) never rejects; merge pipelines
are unaffected. `PgDestOptions.merge_strategy` is `Option` to carry the
explicit-vs-default distinction.
