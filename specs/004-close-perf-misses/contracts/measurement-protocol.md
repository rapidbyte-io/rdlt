# Contract: Measurement Protocol & Decision Rules

**Feature**: 004-close-perf-misses | **Date**: 2026-07-20

This feature amends NO public API contracts (rdlt-core / rdlt-connector SPI
untouched). The contract here is procedural: how measurements are taken and
how accept/reject/re-baseline decisions are made. It binds this feature and
survives it — future benchmark work inherits these rules unless a recorded
policy event changes them.

## P1 — Frozen comparison basis

- Baseline pin: **dlt 1.29.0** for every cell, all feature long. A pin bump
  is out of scope and is its own version-policy event afterward.
- dlt-side scripts (`benches/baseline/*.py`) are frozen. Improving a
  multiple by changing baseline methodology is protocol drift, not a win.
- Datasets identical to the 003 matrix (200k nested NDJSON → 600k rows;
  one-row cold pipeline); every evidence artifact records the dataset hash.

## P2 — Shred-cell measurement

- Stage comparison: dlt `normalize()` in-process self-timing (unchanged 003
  method) vs rdlt `shred_only`, wall-time median of 5 runs, quiet machine
  (no concurrent builds — the 003 contention lesson is codified here).
- Attribution: callgrind on the existing iai bench entry points; instruction
  shares are attribution only, never the acceptance verdict.
- Bar: **≥ 20×** ⇔ rdlt ≤ 297.5 ms against the frozen dlt 5.95 s median —
  unless/until a resolution record (b) adjusts it.

## P3 — Cold-start absolute bar

- Gated statistic: **median** of ≥ 20 `hyperfine` runs after ≥ 3 warmups,
  warm FS cache, reference machine (the 003 matrix machine — identity
  recorded in every artifact's environment header).
- Bar value: `N = measured floor × 1.5, rounded UP to the nearest 5 ms`,
  where the floor is the US2 composition profile's irreducible sum. N is
  fixed once, in the resolution record, with the derivation shown.
- The dlt cold-start ratio is **scoreboard only** from this feature on. No
  gated criterion may reference a competitor-relative quantity.
- Flap rule: if unchanged code fails the bar under this protocol during the
  feature window, the bar is mis-set — re-derive it (spec edge case), do not
  "fix" the code.

## P4 — Candidate accept/reject rule (A/B)

A candidate optimization is **ACCEPTED** only if ALL hold, evidenced in one
A/B record:

1. Like-for-like: both sides measured under the identical protocol, same
   session; profiled runs are never compared against unprofiled runs.
2. Net win on its target cell (wall time for the cell; callgrind explains,
   never decides).
3. No gated criterion regresses beyond the armed gate's tolerance (±3%
   instructions) — proven by the gate run in the A/B record.
4. Correctness nets green: full nextest, doc-tests, and for any
   `shred/*` change the equivalence proptest (byte-identical rows,
   identities, schemas). **Safe Rust only**: the workspace-wide
   `unsafe_code = "deny"` lint stands, and this feature adds no
   `#[allow(unsafe_code)]` exceptions (the single pre-existing CLI FFI
   exception is untouched). A candidate that only wins via `unsafe` is
   REJECTED; that rejection legitimately contributes to a measured-ceiling
   outcome (b).
5. Code quality consistent with existing standards — the T023/T025
   precedent: feasible-but-costly is a valid REJECT.

Anything else is **REJECTED**, with the measurement retained in evidence.

## P5 — Gate re-baseline rule (FR-007)

- `benches/perf-baselines.json` is re-recorded ONLY as part of landing an
  ACCEPTED candidate; the re-record commit names the A/B record.
- Re-recording to absorb unexplained drift is forbidden. If the gate fires
  with no accepted change in flight, that is a regression to diagnose, not a
  baseline to refresh.
- The gate stays armed at its existing tolerance for the entire feature; the
  cross-toolchain refusal stands (toolchain mismatch → re-record
  deliberately on the recorded toolchain, never compare across).

## P6 — Resolution completeness

- Each of the two cells ends in exactly one decision-tree leaf — (a) closed
  or (b) re-baselined — captured in its resolution record (formats:
  data-model.md §2).
- Leaf (b) is valid ONLY when every viable R2 candidate carries a measured
  accept/reject; "didn't get to it" is not a ceiling.
- The partial case (ceiling between current and bar) takes BOTH branches:
  accepted improvements land AND the bar adjusts to the measured value.
- Feature close requires the full-matrix re-measure (all rows, frozen pin)
  with every row's Gated?/status current, and the SC-006 traceability walk
  passing: matrix row → resolution record → evidence, no contradictions.
