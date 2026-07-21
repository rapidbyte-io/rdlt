# Contract: SCD2 History Tracking

A DESTINATION-LOCAL semantic extension of merge for keyed streams: the
engine still runs `Merge { key }`; this destination keeps every version
instead of overwriting. Documented as destination-local — no engine or
connector contract changes.

## Table shape

Stream columns + two validity columns (default `_rdlt_valid_from`
TIMESTAMPTZ NOT NULL, `_rdlt_valid_to` TIMESTAMPTZ NULL; names
configurable). Active version of a key: `valid_to IS NULL`.

## Rules

| # | Rule |
|---|---|
| S1 | Keyed streams only — scd2 on a keyless stream is a typed ensure-time error. Validity-column names colliding with stream columns are a typed ensure-time error naming the collision. |
| S2 | First load: every (deduped) row inserts as active with `valid_from = boundary`, `valid_to = NULL`. Totals equal the source. |
| S3 | Subsequent loads: a staged row whose key HAS an active version compares column-wise (NULL-safe `IS DISTINCT FROM` over non-key data columns). CHANGED → active version retires (`valid_to = boundary`) and the staged row inserts as the new active version (`valid_from = boundary`). UNCHANGED → nothing happens (no churn versions). NEW key → inserts active. |
| S4 | In-batch determinism: the stage dedups last-wins by key (arrival order) BEFORE versioning — one load produces at most one new version per key. |
| S5 | Boundary timestamp: one per commit unit, minted at its first execution. Crash-recovery redelivery of the same (load, commit unit) returns the recorded receipt and re-executes NOTHING (the existing D3 idempotency) — zero duplicate versions, verified by a redelivery conformance cell. |
| S6 | *(amended, review F2 2026-07-21)* Absence policy: `keep` (default) — keys absent from the load keep their active version. `retire` — active keys absent from the FEED retire at the boundary; because the destination sees one commit unit at a time and has no end-of-load hook (SPI frozen), `retire` requires the load's full feed in a SINGLE commit unit: it executes on a load's first unit, and a second unit arriving under `retire` fails typed telling the user to raise the engine commit thresholds — loud, never a partial mass-retirement. |
| S7 | Point-in-time correctness: for any timestamp T, the version with `valid_from <= T < COALESCE(valid_to, 'infinity')` is unique per key; ranges are non-overlapping; exactly one active version per key at all times. Conformance queries pin this across three load rounds. |
| S8 | `hard_delete` does not combine with scd2 in this feature (a deletion-as-retirement policy is future work; the combination is a typed config error). |

## Conformance

Three-round history test (initial + two update rounds): version counts,
range non-overlap, single-active invariant, point-in-time queries;
absence policy both settings; redelivery-zero-duplicates; collision and
keyless rejections.
