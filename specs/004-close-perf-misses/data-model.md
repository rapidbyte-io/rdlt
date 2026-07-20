# Data Model: Close or Re-baseline the Two Benchmark Misses

**Feature**: 004-close-perf-misses | **Date**: 2026-07-20

No runtime data structures change. The entities here are measurement-record
formats — the durable artifacts the spec's SC-006 traceability review walks.

## 1. Benchmark cell (matrix row, `benches/RESULTS.md`)

One measured comparison. The matrix table gains a **Gated?** column.

| Field | Values | Rules |
|---|---|---|
| Cell | prose label | unchanged from 003 |
| baseline / rdlt values | measured pair | baseline-first methodology, both columns or no multiple |
| multiple | derived | scoreboard context on non-gated rows |
| bar | `≥/≤ value` or `—` | on gated rows only; absolute bars carry units + protocol link |
| **Gated?** | `gated` \| `scoreboard` | exactly one; a gated row can block, a scoreboard row cannot |
| status | ✅ met / ❌ missed / `resolved (a)` / `resolved (b)` | `resolved (…)` rows link their resolution record |

**State transitions** (this feature's two cells):
`missed (honest)` → `resolved (a)` [bar met, gate re-baselined] or
`resolved (b)` [bar adjusted, policy entry cites evidence]. No other
transitions permitted; `missed` may not simply disappear.

**Cold-start split**: the current single row becomes two —
- `Cold start, one-row pipeline (absolute)` — gated, bar `≤ N ms` per the
  measurement-protocol contract;
- `Cold start vs dlt (ratio)` — scoreboard, no bar, ratio reported for
  context.

## 2. Resolution record (`evidence/resolution-{shred,cold-start}.md`)

One per formerly-missed cell. Required sections:

- **Outcome**: `(a) closed` or `(b) re-baselined` — exactly one.
- **Final measurement**: the closing values, protocol-conformant, with date.
- **Evidence links**: every profile and A/B record relied on (relative links
  into `evidence/`).
- **Candidates table** (shred only): each R2 candidate → measured share →
  attempted? → accepted/rejected → link. A candidate with no row is a
  traceability failure.
- **Bar derivation** (cold start only): floor composition table, the
  `floor × 1.5 → round to 5 ms` arithmetic, resulting N.
- **Policy entry reference**: which version-policy entry (if outcome (b))
  records the adjustment.

## 3. Evidence artifact (`evidence/*.md`)

Profiles and A/B records. Every artifact opens with the **environment
header** (R7): CPU model, kernel, rustc version, dataset identity (row count
+ content hash), date. Two kinds:

- **Profile**: tool + invocation, top-N attribution table (share %, absolute
  instructions or ms), and the candidate classification it supports
  (viable / exhausted per R2).
- **A/B record**: the change (branch/commit), both sides measured under the
  identical protocol, deltas on: target microbench, e2e cell, all gated
  criteria (gate run output), correctness nets (equivalence proptest,
  nextest). Verdict `ACCEPTED`/`REJECTED` with the one-line reason. Rejected
  changes record enough to not be re-attempted blind (T023/T025 style).

## 4. Version-policy entry (`benches/RESULTS.md` policy block)

Same prose format as the existing 1.29.0 bump entry. For a bar adjustment
(outcome (b)): old bar → new bar, the measured ceiling, one-sentence cause,
and a link to the resolution record. Policy entries are append-only.

## 5. Perf-gate baseline re-record (`benches/perf-baselines.json`)

Unchanged format. New rule from FR-007, recorded here because the file is the
entity: a re-record commit MUST name the accepted A/B record in its message;
a re-record commit with no accepted-change reference is a review-blocking
defect.

## Validation rules (cross-entity)

- Every `resolved (…)` matrix status resolves to exactly one resolution
  record, which resolves to ≥ 1 evidence artifact — no dangling links
  (SC-006).
- No gated row's bar may reference a competitor-relative quantity (SC-003's
  invariance rule); ratios live only on scoreboard rows.
- An `ACCEPTED` A/B record with any gated-criterion regression beyond the
  gate tolerance is invalid by construction (FR-002); the gate run output in
  the artifact is the proof.
