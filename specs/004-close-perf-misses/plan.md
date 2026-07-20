# Implementation Plan: Close or Re-baseline the Two Benchmark Misses

**Branch**: `004-close-perf-misses` | **Date**: 2026-07-20 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/004-close-perf-misses/spec.md`

## Summary

Resolve the two cells the 003 close-out left honestly missed against dlt
1.29.0, each through the spec's decision tree — (a) bar met with maintainable
code and the perf gate re-baselined, or (b) an evidence-backed bar adjustment
recorded in the benchmark version policy:

1. **Shred-only, 12.0× vs ≥20×** (P1): rdlt 0.50 s vs dlt normalize 5.95 s.
   Hitting 20× means rdlt ≤ 297.5 ms — a ~40% stage cut on top of the 003
   rewrite (tape shredder, hex encoder). Fresh callgrind attribution on
   current code ranks the remaining candidates (SIMD structural scanning,
   UTF-8 validation, arena/tape layout, number/string fast paths); each is
   A/B'd and accepted or rejected with committed evidence. If the measured
   ceiling is < 20×, the bar is adjusted to the measured value with the
   evidence attached (T023/T025 precedent).
2. **Cold start, 1/14.2 vs ≤1/20** (P2): a measurement-design fix, not a perf
   fix — rdlt measured 30 ms; the ratio regressed only because dlt got
   faster. The gated criterion becomes an ABSOLUTE bound (value derived from
   a startup-composition profile: floor costs + explicit headroom), with the
   dlt ratio demoted to a non-gated scoreboard number. Cheap startup wins
   found by the profile (rdlt cold start is dominated by DuckDB
   open+catalog) pass the same A/B rule.

No public contracts change. The deliverables are internal engine/CLI code (if
optimizations are accepted), benchmark-record structure (gated vs scoreboard
status), version-policy entries, and committed evidence artifacts.

## Technical Context

**Language/Version**: Rust stable (2024 edition), workspace v0.2.0

**Primary Dependencies** (all existing unless a candidate is ACCEPTED):
`iai-callgrind` + valgrind/callgrind (attribution profiles AND the armed
regression gate), `hyperfine` (cold-start wall-time protocol), existing bench
harness (`benches/run-e2e.sh`, `benches/baseline/normalize_only.py`,
`benches/baseline/cold_start.py`, `crates/rdlt-engine/benches/shred.rs`).
Candidate-only (added ONLY if their A/B wins): SIMD JSON scanning support
(e.g. wider `memchr` use or explicit SIMD structural indexing inside
`shred/tape.rs`). No candidate dependency is added speculatively.

**Storage**: n/a (no persistence changes; measurement records are committed
files)

**Testing**: `cargo nextest run` (+ `cargo test --doc`); existing equivalence
proptest and mutation-closure suites must stay green through every accepted
optimization; the shred equivalence property test is the safety net for any
`tape.rs`/`build.rs` change

**Target Platform**: Linux; reference hardware for absolute bars = the same
machine/environment used for the 003 final matrix (documented in the
measurement protocol contract)

**Project Type**: Rust library workspace + dev CLI (unchanged)

**Performance Goals**: shred-only cell ≥ 20× vs pinned dlt 1.29.0 normalize
(rdlt ≤ 297.5 ms on the 200k-record dataset) OR evidence-backed re-baseline;
cold start ≤ N ms absolute (N fixed in implementation from the measured
composition; current median 30 ms) with the protocol recorded

**Constraints**: baseline pin frozen at dlt 1.29.0 for the whole feature;
both sides of any cell re-measured with the UNCHANGED 003 methodology (no
protocol drift mid-feature); every accept/reject is a like-for-like A/B; the
iai-callgrind gate stays armed at ±3% throughout and is re-baselined only as
part of accepting a specific change (FR-007); shred output must remain
byte-identical (equivalence proptest) for any accepted shred change

**Scale/Scope**: 0 new public API surface; ≤ ~3 source files touched per
accepted candidate (`shred/tape.rs`, `shred/build.rs`, CLI startup path);
records: RESULTS.md restructure (gated/scoreboard column), version-policy
entries, `evidence/` artifacts in this spec dir; possible small bench-harness
additions (startup-phase timing)

## Constitution Check

`.specify/memory/constitution.md` remains the unfilled template. Governing
principles are the approved design doc (`2026-07-18-rdlt-engine-design.md`)
as applied by features 001–003: correctness before speed, seams sacred, no
silent failures, benchmark honesty. Gate evaluation:

- **Benchmark honesty**: PASS by construction — this feature IS the honesty
  mechanism: both misses stay recorded until resolved, negative results are
  first-class outcomes, gated vs scoreboard status becomes explicit
  (removing a class of accidental dishonesty where a ratio silently changes
  meaning when the baseline moves).
- **Correctness before speed**: PASS — the 003 nets (equivalence proptest,
  crash sweeps, mutation closures) are prerequisites that must stay green
  across every accepted optimization; a candidate that wins the A/B but
  breaks a net is rejected, full stop.
- **Seams sacred**: PASS — no `rdlt-core`/`rdlt-connector` API changes.
  Candidates live inside `rdlt-engine` internals and the CLI startup path.
- **No silent failures**: PASS — FR-007 forbids re-baselining the perf gate
  to absorb drift; every re-record is tied to a named accepted change.

Post-design re-check (after Phase 1): still PASS — the contracts add
measurement/record formats only; no runtime seam is touched.

## Project Structure

### Documentation (this feature)

```text
specs/004-close-perf-misses/
├── plan.md              # This file
├── research.md          # Phase 0 (R1–R7: tooling, candidates, bar design, record formats)
├── data-model.md        # Phase 1 (cell/resolution/evidence/policy record schemas)
├── quickstart.md        # Phase 1 (how to profile, A/B, and record outcomes)
├── contracts/
│   └── measurement-protocol.md  # Phase 1 (absolute-bar protocol, A/B accept rule, gate re-baseline rule)
├── evidence/            # Implementation output: committed profiles + A/B records
└── tasks.md             # Phase 2 (/speckit-tasks)
```

### Source Code (repository root)

```text
crates/rdlt-engine/src/shred/
├── tape.rs              # P1 candidate site: structural scan, UTF-8 validation, parse fast paths
├── build.rs             # P1 candidate site: value materialization, casts
└── arena.rs             # P1 candidate site: layout/locality (only if profile implicates it)
crates/rdlt-engine/benches/
├── shred.rs             # existing shred microbench — A/B vehicle for P1
└── iai_hotpath.rs       # armed gate; re-baselined ONLY on accepted changes
crates/rdlt-cli/…        # P2 candidate site: startup path (lazy/deferred DuckDB open if profile supports it)
benches/
├── RESULTS.md           # restructured: explicit gated-vs-scoreboard status per row; resolution links
├── perf-baselines.json  # re-recorded only alongside accepted optimizations
├── run-e2e.sh           # cold-start cell gains the absolute-bar protocol (runs, aggregation, cache state)
└── baseline/            # unchanged dlt-side scripts (protocol frozen; pin stays 1.29.0)
```

**Structure Decision**: no new crates, no new workspaces. All measurement
records live under this feature's spec directory (`evidence/`) except the two
project-wide records that outlive the feature: the matrix + version policy
(`benches/RESULTS.md`) and the gate baselines (`benches/perf-baselines.json`).

## Phase ordering (mirrors spec priorities)

0. **Prerequisite (not a deliverable)**: restore the build/measurement
   environment (distrobox was missing from PATH at feature start; profiling
   is impossible without it) and confirm the reference machine matches the
   003 matrix environment.
1. **US1 — shred**: callgrind attribution on current code → ranked candidate
   list with measured shares → A/B each viable candidate (equivalence
   proptest green, no gated regression) → resolve: bar met + gate re-baseline,
   or bar adjusted + policy entry. All evidence committed as it is produced.
2. **US2 — cold start**: startup-composition profile → absolute bar chosen
   (floor + recorded headroom) with full protocol → criterion conversion in
   RESULTS.md (ratio → scoreboard) → any cheap win A/B'd under the same rule.
   Independent of US1; may interleave after the US1 profile exists.
3. **US3 — coherent record**: full-matrix re-measure against the pinned
   baseline; every row explicitly gated or scoreboard; both resolution
   records traceable row → record → evidence with no contradictions.

## Complexity Tracking

No constitution violations to justify. Two deliberate scope guards:

- The 003 closed decisions (blake3 kept — T023; thin-LTO rejected — T025) are
  NOT reopened unless the fresh profile produces materially new evidence
  (e.g. hashing's measured share changed because surrounding code got
  faster). Re-litigating them without new data is out of scope.
- The dlt-side measurement scripts are frozen for this feature: improving
  rdlt's number by changing how the baseline is measured would be a protocol
  drift, not a win. Any baseline-methodology change is its own version-policy
  event outside this feature.
