# Contract Amendment: Merge for Keyed Structured Streams (lifts clause B4)

**Feature**: 006-postgres-completeness | **Date**: 2026-07-20

**Amends**: feature-002's structured-stream rules (engine clause B4 /
E7 notes) and destination clause D8. The feature-002 contract text
gains a pointer to this amendment — recorded evolution, not a silent
rewrite. This closes the deviation recorded at 005 close (US2-AS5).

## Before → after

- BEFORE: `Merge` is rejected at plan time for ALL structured streams
  (no per-row `_rdlt_id` to dedup with).
- AFTER: `Merge` is ACCEPTED for a structured stream iff BOTH hold:
  1. the stream declares a non-empty key (`StreamSpec.primary_key` —
     reflected or configured);
  2. the destination declares `DestCapabilities.merge` (existing flag).
  All other structured cases keep the existing typed plan-time
  rejection, now pointing at the keyed alternative. The parquet
  destination remains append/replace-only.

## Semantics (destination obligation, extends D8)

- Per commit unit: delete rows whose key matches any staged row's key,
  then insert the staged rows — atomically with the commit (D1/D2).
- Key = the declared key columns (data columns), replacing
  `_rdlt_root_id` in the existing keyed machinery; multi-column keys
  compare column-wise.
- Idempotence unchanged (D3): re-commit of the same (load_id,
  commit_seq) re-publishes nothing.

## Engine obligations

- Plan-time validation per the acceptance rule above.
- Write-time validation: any NULL in a key column of a batch under
  Merge is a typed error (keys are identities; spec edge case).
- E7's redelivery note is UPDATED for this mode: keyed structured
  merge is dedup-safe across the crash-recovery redelivery window
  (replay converges by key) — the documented at-least-once caveat no
  longer applies to keyed merge streams.

## Conformance obligations

Update-heavy incremental workload converges to one row per key with
newest values on BOTH SQL destinations; crash sweep runs the Merge
mode across every registered fail point with armed-fire pins extended;
keyless and non-capable rejections keep their tests.
