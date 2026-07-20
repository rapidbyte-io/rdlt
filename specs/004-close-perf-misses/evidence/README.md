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

## Traceability (filled by T018)

The SC-006 walk results are appended here at feature close.
