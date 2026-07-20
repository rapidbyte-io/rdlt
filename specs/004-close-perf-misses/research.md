# Research: Close or Re-baseline the Two Benchmark Misses

**Feature**: 004-close-perf-misses | **Date**: 2026-07-20

All decisions below resolve the plan's Technical Context unknowns. Numbers are
grounded in the committed 003 close-out matrix (`benches/RESULTS.md`,
2026-07-20, dlt 1.29.0): shred-only rdlt 0.50 s vs dlt 5.95 s (12.0×); cold
start rdlt 30 ms vs dlt 418 ms (1/14.2); shred tape path recorded at 362 M
instructions / 10k nested rows in `benches/perf-baselines.json`.

## R1 — Attribution tooling for the shred profile

**Decision**: callgrind (via the existing `iai-callgrind` bench entry points,
annotated with `callgrind_annotate`) is the PRIMARY attribution tool;
`perf record`/`perf stat` (cycles, cache-misses, branch-misses) is the
SECONDARY lens, mandatory only for memory-shaped candidates (C3 below).

**Rationale**: callgrind gives deterministic instruction attribution in the
same units as the armed gate (`perf-baselines.json` records instruction
counts), so profile shares, A/B deltas, and gate re-baselines all speak one
currency. But instruction counts are blind to stalls: an arena-layout change
can win wall time with near-zero instruction delta. Hence the two-lens rule —
any candidate whose mechanism is locality/latency MUST show its win in
`perf stat` cycles AND in the wall-time microbench, not just callgrind.

**Alternatives considered**: sampling flamegraphs only (non-deterministic,
wrong units for the gate); adding a new tracing/timing framework to the bench
harness (needless — the 003 harness already has both toolchains installed and
the mislabeled-entry-point lesson from 003 says keep the bench surface small
and well-known).

## R2 — Shred candidate inventory and ranking rule

**Decision**: the profile ranks these named candidates by measured
instruction share (with the C3 caveat from R1); nothing is implemented before
its share is known. Inventory, with mechanism:

- **C1 — structural scan (SIMD-style)**: `shred/tape.rs` owns its JSON parser
  (serde_json appears only as an error type). Candidate: two-stage parsing —
  a byte-scanning stage that finds structural characters (quotes, braces,
  colons, escapes) via `memchr`/wide compares before the state machine, in
  the style of simdjson's stage-1. `memchr` is already a workspace dependency
  (003 slab splitting), so the cheap variant adds no new crates.
- **C2 — UTF-8 validation consolidation**: establish where input bytes are
  validated more than once between slab arrival and tape strings; candidate
  is validate-once-per-slab, restructured so later stages receive an
  already-validated `&str` through safe APIs (the type system carries the
  proof, not an `unchecked` conversion). The workspace denies `unsafe_code`
  (`Cargo.toml`, one documented CLI FFI exception) and this feature adds NO
  new exceptions: if the only winning implementation needs
  `from_utf8_unchecked` or similar, the candidate is REJECTED on the quality
  bar and its measured potential recorded as unreachable-by-policy in the
  resolution record.
- **C3 — arena/tape layout**: node width, field ordering, slab reservation
  strategy in `shred/arena.rs` + `tape.rs`. Memory-shaped: requires the
  two-lens rule (R1).
- **C4 — scalar fast paths**: integer-only number parse fast path,
  escape-free string fast path (memchr for `\` and `"`), datetime cast paths
  in `build.rs` (chrono `parse_from_str` per row is format-string
  interpretation; a fixed-layout parser is the candidate).
- **C5 — identity/hash share re-check**: blake3 stays FROZEN per T023 unless
  the fresh profile shows its share materially changed because everything
  around it got faster. Reopening threshold: identity hashing ≥ 25% of stage
  instructions in the new profile. Below that, T023 stands unrevisited.

**Ranking rule**: a candidate is viable if `measured share × plausible
reduction` contributes meaningfully toward the required 1.68× stage speedup
(R3); candidates are attempted in descending measured-share order, and the
cell is declared ceiling-reached only when every viable candidate holds a
measured accept or reject.

**Alternatives considered**: adopting the `simd-json` crate wholesale
(rejected as a first move: it imposes its own tape/DOM model and would
replace the 003 `JsonView` seam rather than optimize under it — only
reconsidered if C1's in-house variant wins big and still falls short);
parallelizing the shred stage (rejected: changes the measurement's meaning —
the cell is a single-stage efficiency claim, and the flagship e2e already
overlaps stages).

## R3 — What closing the shred bar actually requires

**Decision**: record the arithmetic as the evidence baseline. 20× vs dlt's
5.95 s means rdlt ≤ 297.5 ms; from 0.50 s that is a ≥ 40.5% cut (≈ 1.68×).
In gate units, holding IPC constant, roughly 362 M → ≤ 215 M instructions on
the 10k-row microbench — with the explicit caveat (committed with the
evidence) that IPC will NOT stay constant for memory-shaped changes, which is
why acceptance is judged on the wall-time microbench and the e2e cell, with
callgrind as attribution, not as the verdict.

**Rationale**: writing the requirement down in both currencies before
starting prevents the classic drift where a pile of small callgrind wins
"adds up to 40%" on paper but the wall-time cell doesn't move.

## R4 — Cold-start absolute bar: value derivation and protocol

**Decision**: the gated criterion becomes `cold start ≤ N ms`, where
`N = measured floor × 1.5, rounded up to the nearest 5 ms`, fixed once the
US2 composition profile establishes the floor (the irreducible sum: process
exec + dynamic linking + config parse + DuckDB open/catalog + empty-pipeline
state init + teardown). With today's 30 ms median and DuckDB open dominating,
N plausibly lands at 30–50 ms; the exact value is an implementation-phase
output, recorded with its derivation.

Protocol (recorded in `contracts/measurement-protocol.md`, enforced by the
harness): reference machine = the 003 matrix machine; one-row pipeline
identical to the existing cell; `hyperfine` with ≥ 3 warmup runs and ≥ 20
measured runs; MEDIAN is the gated statistic; warm filesystem cache; a bar
that fails under its own protocol on unchanged code within the feature window
is treated as mis-set and re-derived (spec edge case).

**Rationale**: floor × 1.5 gives real headroom against environmental noise
while staying honest — the bar stays anchored to what the startup measurably
IS, not to a competitor's release cadence. Median over p95: the cell's
purpose is regression detection on a quiet reference machine, and the 003
history shows the dlt side's bimodality was the noisy part, not rdlt's.

**Alternatives considered**: keeping any dlt-relative gate with a pinned
version (rejected: the pin bump policy re-measures every cell, so the same
false-alarm class returns at every bump); p95/max statistic (rejected:
conflates scheduler noise with regressions on a 30 ms quantity); cold FS
cache (rejected: not reproducibly attainable in the harness).

## R5 — Startup-composition profiling method

**Decision**: a temporary instrumented build — coarse `std::time::Instant`
spans printed at phase boundaries (pre-main→main entry via a first-line
stamp, config parse, DuckDB open, catalog/state init, first-batch readiness,
teardown) — committed as an evidence table, NOT shipped; corroborated by
`strace -T -c` for the syscall view of the DuckDB open/catalog phase.
Dynamic-linking cost measured from outside via the hyperfine total minus
main-entry stamp.

**Rationale**: at 30 ms total, callgrind's distortion and a tracing
framework's overhead both exceed the thing measured; two crude independent
views (in-process stamps + syscall times) that agree are stronger evidence at
this scale. The instrumentation is throwaway by design — the deliverable is
the committed table, keeping FR-007's "no permanent measurement drift"
posture.

**Alternatives considered**: shipping a permanent `--timings` flag (scope
creep — nothing in the spec needs it; revisit only if a cheap win lands and
needs a regression guard, in which case the e2e cold cell already covers it).

## R6 — Record formats (gated vs scoreboard, resolution, policy)

**Decision**: three additions, formats specified in `data-model.md`:

1. `benches/RESULTS.md` matrix gains an explicit **Gated?** column — every
   row is `gated` or `scoreboard`; the cold-start row becomes two rows
   (absolute gated bar; dlt ratio scoreboard).
2. **Resolution records** live at
   `specs/004-close-perf-misses/evidence/resolution-{shred,cold-start}.md`,
   each declaring the decision-tree leaf reached — (a) or (b) — and linking
   every evidence artifact it relies on.
3. **Version-policy entries** append to the existing policy block in
   RESULTS.md, same prose format as the 1.29.0 bump entry, each citing its
   resolution record.

**Rationale**: RESULTS.md stays the single project-wide scoreboard (it
already survived two features as that); per-feature evidence stays in the
spec dir like 003's mutation-report.md precedent, so the matrix links
downward and nothing is duplicated.

**Alternatives considered**: a new top-level `benches/policy.md` (rejected:
splits the version policy from the numbers it governs); machine-readable
JSON resolution records (rejected: the consumer is a human reviewer — SC-006
is a traceability read, not a tooling need).

## R7 — Environment prerequisite

**Decision**: before any measurement, restore the distrobox build environment
(missing from PATH at feature start; see build-env history — the container
can lose gcc on recreation) and verify measurement identity: same machine as
the 003 matrix, rustc version matching `perf-baselines.json`'s recorded
toolchain (the gate's cross-toolchain refusal from `f02eabe` makes a mismatch
loud rather than silent). Every evidence artifact opens with a header
recording CPU model, kernel, rustc, and dataset hash.

**Rationale**: the whole feature is measurement; an unverified environment
poisons every downstream decision. The header rule makes evidence artifacts
self-describing for the SC-006 traceability review.
