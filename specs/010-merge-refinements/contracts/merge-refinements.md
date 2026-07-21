# Contract: Merge Refinements — Ordered Dedup + Scope Keys

Amends feature 008's `merge-strategies.md` (M-rules stand; these add
MR-rules). Postgres destination only.

## The options

```toml
[destination.postgres.tables.events]
dedup_sort = { column = "seq", order = "desc" }   # ordered survivor
merge_key = ["day"]                                # scope replacement
```

## Rules

| # | Rule |
|---|---|
| MR1 | `dedup_sort {column, order}` selects the in-load survivor per identity by `column` under `order`; values beat NULL; ties (and all-NULL groups) fall back to the existing deterministic arrival-order last-wins. Absent ⇒ behavior unchanged. |
| MR2 | The survivor drives EVERY downstream decision — hard_delete flag, upsert content, SCD2 change detection — because selection happens in the one shared dedup shape. |
| MR3 | `merge_key` declares a non-unique scope: a merge load deletes target rows whose scope appears in the load's delivered set, then applies the strategy arm unchanged (scope OR identity replacement). Scopes absent from the load are untouched. |
| MR4 | NULL is not a scope: rows with NULL in any merge_key column neither cause nor receive scope deletion (identity replacement still applies to them). |
| MR5 | Scope replacement requires the SCOPED TABLE's full feed in a SINGLE commit unit (amended per research R2, sharpened in the review round): it executes at the table's FIRST STAGED unit; a later unit STAGING rows for that table is a TYPED error advising the engine commit thresholds (recovery converges — the fixed configuration re-delivers the full feed); units where the table stages NOTHING are skipped, so other streams' checkpoints never trigger the rule (per-table tracking, ONE shared discipline with scd2 absent-retire — which also gains the empty-unit tolerance, closing its own mass-retirement-on-empty-stage edge). Committed-unit redelivery is a D3 no-op and re-marks the discipline. RECORDED RESIDUAL: a scoped/retire stream that checkpoints MID-feed and crashes between a committed partial unit and the split-detection error resumes as a NEW load with a partial feed — undecidable destination-side (the crash sweep killed the receipts scheme that tried); scoped streams must checkpoint only at feed end, making crash-redelivery full and the delete convergent. |
| MR6 | Both options are KEYED-STRUCTURED-only and validated at open: nonexistent columns, flag/validity collisions, shredded streams, and `merge_key` + scd2 are DISTINCT typed errors naming table + column. Parse-time shape errors (empty/duplicate lists, bad order token) fail before any connection. |
| MR7 | Both options ride the generated dest-options schema (examples validate; unknown fields fail both layers) and the CLI per-table passthrough. |
| MR8 | Sanctioned dlt deviations: no append-fallback without keys; no arbitrary survivor without dedup_sort; NULL semantics defined here, not inherited (research, "Sanctioned deviations"). |
