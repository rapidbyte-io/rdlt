# Evidence directory — formats and rules (T005)

Every artifact here justifies a decision in feature 004's decision tree.
Formats are binding (data-model.md §3); write each artifact AGAINST this
checklist, not from memory.

## Universal rule: the environment header

Every artifact opens with the Identity block copied from
[environment.md](environment.md) (machine, CPU, kernel, rustc, tool
versions, dataset hash), plus the artifact's own date. An artifact without a
header fails the SC-006 walk.

## Profile artifacts (`profile-*.md`)

Required sections:

1. **Identity header** (above) + exact tool invocations.
2. **Attribution table**: top-N cost centers with share % and absolute units
   (instructions for callgrind; ms/cycles for wall/perf lenses).
3. **Candidate classification**: what this profile says about each named
   candidate — viable / exhausted — with the share arithmetic shown.

## A/B record artifacts (`ab-*.md`)

Required sections:

1. **Identity header** + the change (commit/diff summary).
2. **Both sides, identical protocol**: never a profiled run vs an unprofiled
   run.
3. **Deltas**: target microbench, e2e cell(s), full `make bench TARGET=iai`
   gate output, correctness nets (nextest + shred_equivalence + doc-tests).
4. **Verdict**: `ACCEPTED` or `REJECTED` + one-line reason. Rejected records
   keep enough detail that the candidate is never re-attempted blind
   (T023/T025 style).

## The P4 accept checklist (verbatim from contracts/measurement-protocol.md)

A candidate is ACCEPTED only if ALL hold:

1. Like-for-like: both sides measured under the identical protocol, same
   session; profiled runs are never compared against unprofiled runs.
2. Net win on its target cell (wall time for the cell; callgrind explains,
   never decides).
3. No gated criterion regresses beyond the armed gate's tolerance (±3%
   instructions) — proven by the gate run in the A/B record.
4. Correctness nets green: full nextest, doc-tests, and for any `shred/*`
   change the equivalence proptest (byte-identical rows, identities,
   schemas). **Safe Rust only**: `unsafe_code = "deny"` stands, no new
   `#[allow(unsafe_code)]` exceptions; a candidate that only wins via
   `unsafe` is REJECTED (valid ceiling evidence).
5. Code quality consistent with existing standards — feasible-but-costly is
   a valid REJECT.

## Resolution records (`resolution-{shred,cold-start}.md`)

Required sections per data-model.md §2: Outcome (exactly one leaf),
final protocol-conformant measurement, evidence links, candidates table
(shred: every C1–C5 disposition) / bar derivation (cold start: floor
composition + `floor × 1.5 → round up to 5 ms`), policy entry reference.

## Traceability — SC-006 walk (T018, 2026-07-20)

**Shred cell**: matrix row (`resolved (b)`, 5.75 s / 0.52 s → 11.0×, bar
≥ 10× adjusted) → [resolution-shred.md](resolution-shred.md) → links
resolve: [environment.md](environment.md), [profile-shred.md](profile-shred.md),
[rematrix-final.md](rematrix-final.md). Candidates table complete —
C5′, C6, C3, C1, C2, C4 each carry a measured share and an explicit
disposition (viable/marginal, not attempted — owner decision, backlog);
C5 (algorithm swap) explicitly not reopened per the T007 narrow-reopen
scope. Policy entry present and value-consistent with the record
(≥ 20× → ≥ 10×). Perf gate correctly NOT re-recorded (no accepted A/B;
FR-007/P5). Note: profile-shred.md's attempt-order sentence names the
prospective A/B files `ab-c5-identity-pipeline.md`/`ab-c6-column-interning.md`
— those were never created because no A/B ran; the resolution record
states this explicitly, so the walk treats it as explained, not dangling.
**P6 deviation** (leaf (b) without per-candidate accept/reject) is
declared in both the resolution record and the policy entry — recorded
policy event, not a silent skip.

**Cold-start cell**: matrix rows (gated absolute `resolved (a)` ≤ 40 ms
+ scoreboard ratio 1/17.7) → [resolution-cold-start.md](resolution-cold-start.md)
→ links resolve: [profile-cold-start.md](profile-cold-start.md),
[environment.md](environment.md), [rematrix-final.md](rematrix-final.md).
Bar derivation present (floor 23.6 ms × 1.5 → 40 ms, flap check).
SC-003 invariance statement present; no gated row references a
competitor-relative quantity. T015 negative result recorded;
`ab-cold-startup.md` intentionally absent and explained. Policy entry
present. Protocol recorded in `benches/run-e2e.sh` cold cell and
contract P3.

**Design doc**: `2026-07-18-rdlt-engine-design.md` §8 targets updated by
pointer to the RESULTS.md policy entries (both adjusted bars) — no
silent rewrite.

**Result: PASS** — every `resolved (…)` row reaches exactly one
resolution record and ≥ 1 evidence artifact with no contradictions; the
one stale cross-value found during the walk (0.418 vs 0.417 s dlt cold
in the resolution record) was reconciled in the same session.

## Verification sweep (T019, 2026-07-20)

`make check` green on the final tree (exit 0): `cargo fmt --check` +
`clippy -D warnings`, `cargo nextest run --workspace`,
`cargo test --doc --workspace`, crash sweeps (engine + postgres,
failpoints), and the armed iai gate — 4/4 benches within the ±3%
tolerance against the UNTOUCHED `benches/perf-baselines.json`
(shred_nested +0.66%, passthrough/identity ±0.00%; gate output verbatim:
"perf gate: all benches within tolerance"). No baseline was re-recorded
during this feature (FR-007 — no accepted optimization). SC-005
satisfied.
