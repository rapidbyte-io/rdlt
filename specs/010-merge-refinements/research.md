# Research: Merge Refinements — Ordered Dedup + Scope Keys

Decisions verified against the code on branch `010-merge-refinements`
(main @ 54a7551) and against dlt 1.29.0's merge generator
(`dlt/destinations/sql_jobs.py`, audited 2026-07-21) as the parity
reference.

## R1 — dedup_sort: one ORDER BY injection point, no new machinery

**Fact (code-verified)**: every in-load survivor decision already flows
through `MergePlan::deduped()` (`dest/commit.rs`): `SELECT DISTINCT ON
(key) * FROM stage ORDER BY key, __rdlt_arrival DESC` — the stage-only
arrival column makes last-wins real (008 finding #7). delete-insert,
upsert (incl. its hard-delete pre-delete), and SCD2's staged-row base all
read this shape.

**Decision**: `dedup_sort` = per-table destination option `{column,
order: asc|desc}` (order REQUIRED — survivor selection is too important
for an implicit default). It rewrites the dedup ordering to
`ORDER BY key, <column> DESC NULLS LAST, __rdlt_arrival DESC` (asc:
`ASC NULLS LAST`):
- values beat NULL in either direction (spec US1-AS4);
- the arrival column stays as the trailing tie-breaker — ties and
  all-NULL groups keep the existing deterministic last-wins (US1-AS1/2,
  edge cases);
- because every strategy consumes `deduped()`, FR-003 (survivor drives
  hard-delete/SCD2/upsert) holds by construction — the flag decision
  already reads the deduped row (008 review F3).

**Scope**: KEYED STRUCTURED tables only, like upsert (008 M7 posture):
on a shredded stream the identity is a content hash — same identity IS
same content, so an ordered survivor is meaningless; declaring it there
is a typed error, not a silent no-op.

**dlt parity note**: dlt orders `ROW_NUMBER() OVER (PARTITION BY pk
ORDER BY <col> ASC|DESC)` and falls back to `ORDER BY (SELECT NULL)`
(arbitrary survivor) without the option (`sql_jobs.py:256-323`). rdlt
keeps its stricter default (deterministic arrival order) and documents
NULL placement, which dlt leaves to SQL defaults.

## R2 — merge_key: scope replacement via per-load scope receipts

**Problem**: "delete every target row whose scope appears in the batch"
is only correct per LOAD, but publishes happen per COMMIT UNIT. A naive
per-unit scope delete would destroy rows an EARLIER unit of the same
load published for the same scope — the exact bug class 008's review F2
found in scd2 absent-retire.

**Fact (code-verified)**: the destination already keeps durable
load-scoped guards in the publish transaction: `_rdlt_commits (load_id,
commit_seq)` drives D3 idempotence, and `load_committed_before` guards
replace-truncate-once. Redelivery of an already-committed unit exits
before any merge SQL runs (the `already > 0` branch), so replay never
re-executes a scope delete.

**Decision**: scope deletion is FIRST-TOUCH-PER-LOAD, guarded by a new
auxiliary table `_rdlt_scope_receipts (load_id, table_name, scope)` —
`scope` is the composite scope value's deterministic text form
(`ROW(s1, …)::text`). In each commit unit, per scoped table, inside the
one publish transaction:
1. delete target rows whose scope matches a stage scope that has NO
   receipt for this load (scopes with NULL in any column are excluded on
   both sides — NULL is not a scope, FR-005);
2. insert receipts for the stage's scopes;
3. run the strategy arm (identity delete-insert / upsert) unchanged.
Receipts for other loads are pruned when a new load first touches the
table (same hygiene moment as stage truncation). Crash → the tx rolls
back receipts with the delete (atomic); committed-unit replay is a D3
no-op — redelivery-stable by the same argument as everything else
(FR-006).

**Why not S6-style "single commit unit only"**: unlike scd2 retire
(which compares against the WHOLE feed), scope replacement composes
per-scope — receipts make multi-unit loads sound instead of banning
them, and the machinery is the house pattern (durable guard in the
publish tx), not new state semantics.

**Ordering with strategies**: scope delete runs BEFORE the arm. Under
delete-insert the identity delete then covers re-delivered identities
(the OR of FR-004 = two deletes in one tx); under upsert the scope
delete clears undelivered rows and the conflict-update handles the rest.
A scope-moving row (US2-AS3) cannot duplicate: its old row is removed
either by scope (old scope delivered) or by identity (same key).

**Scope**: KEYED STRUCTURED tables only (same reasoning as R1 — child
tables replace by root subtree, a scope key there has no sound
semantics); scd2 + merge_key is a typed error (spec Out of Scope).

**dlt parity note**: dlt ORs `primary_key` and `merge_key` conditions in
one delete (`sql_jobs.py:217-234, 597-663`) and silently falls back to
append when both are absent. rdlt keeps identity mandatory (engine B4
frozen) and rejects the undefined shapes typed.

### R2 amendment (implementation-discovered, 2026-07-21)

The receipts design was WRONG, and this feature's own crash sweep proved
it before it shipped: a crash-RESUMED load is a NEW load (fresh load_id)
delivering a PARTIAL feed — its scope delete destroyed rows the previous
attempt committed and never re-delivered (75/100 rows survived the
sweep's recovery cell). No destination-side bookkeeping can distinguish
resume-of-partial-load from fresh-load; the spec's premise — "the batch
is the complete truth for its scope" — only holds when the scope's truth
arrives ATOMICALLY. Scope replacement therefore requires the scoped
table's full feed in a SINGLE commit unit: it runs in the load's first
unit only, and a later unit with staged rows for a scoped table is a
typed error advising the commit thresholds — exactly the scd2
absent:retire rule and remedy (008 S6). The receipts table is deleted;
multi-unit pipelines remain fine when the scoped table's feed fits its
first unit (later empty units are tolerated, conformance-pinned).

## R3 — Validation surface

**Decision**: two layers, matching 008:
- Parse-time (`PgDestOptions::validate`): non-empty column names,
  non-empty/duplicate-free merge_key list, order field vocabulary —
  shape errors before any connection.
- Open-time (`ensure_table`, where M7/S1 rejections live): column
  existence against the stream schema (typed, names table + column);
  collisions — dedup_sort column or merge_key columns must not be the
  hard_delete flag or scd2 validity columns; keyed-structured-only
  rejections; scd2 + merge_key rejection. All before any data moves
  (FR-008).

`dedup_sort` on any EXISTING column type is accepted: every type the
destination creates is orderable in postgres; "non-orderable" reduces to
nonexistent columns.

## R4 — Performance protocol (FR-011)

**Decision**: scoreboard cells (5-run medians, recorded protocol, no new
gates) in `benches/run-merge-refinements.sh`: on the 10M-row
merge-index harness shape, (a) scope-replace of one 100k-row scope vs
the identity-only delete-insert of the same rows — the cost of the extra
scope delete + receipts; (b) dedup_sort on a 2×-duplicated 1M-row stage
vs last-wins — the cost of the extra sort key. Existing gated bars must
stay within tolerance (iai + e2e unchanged: the new SQL only runs when
the options are declared).

## R5 — Config + schema + CLI

**Decision**: `PgTableOptions` gains `dedup_sort: Option<DedupSort>`
(`{column: String, order: SortOrder}`) and `merge_key:
Option<Vec<String>>`; schemars-derived, examples in the dest-options
schema tests, unknown fields fail both layers (SC-005). The CLI's
`[destination.postgres.tables.<name>]` passthrough carries both with
zero CLI changes (serde), verified by the existing toml-shape test plus
new cells.

## Sanctioned deviations from dlt semantics (recorded)

1. No append-fallback when keys are missing (dlt `sql_jobs.py:597-599`)
   — rdlt's engine merge contract requires identity; absence stays a
   typed error.
2. No arbitrary survivor without dedup_sort (dlt `ORDER BY (SELECT
   NULL)`) — rdlt keeps deterministic arrival-order last-wins.
3. NULL policies are DEFINED (values beat NULL; NULL scopes match
   nothing) rather than inherited from SQL defaults.
