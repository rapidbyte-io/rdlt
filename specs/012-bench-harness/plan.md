# Implementation Plan: Unified Benchmark Framework

**Branch**: `012-bench-harness` | **Date**: 2026-07-21 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/012-bench-harness/spec.md`

## Summary

Replace the six run-*.sh scripts with one dev-only harness crate
(`crates/rdlt-bench`, binary `rdlt-bench`: list/run/gate/report) driven
by declarative TOML cells (R2). Metrics come from the seams that already
exist: `RunReport` rows/bytes → exact rows/s + MB/s, the engine
`events()` stream → per-stream attribution (library mode), a safe-Rust
procfs sampler (`VmHWM`, utime/stime) for the gated CLI-subprocess runs
(R3), and cgroup v2 for the dlt container (R4) — dlt promoted to a
first-class competitor module with a variant registry. Results are
versioned, committed JSON artifacts with an environment fingerprint
(R5); the gated bars move from RESULTS.md prose into `benches/bars.toml`
enforced by `rdlt-bench gate` (R6); RESULTS.md tables become generated
(BH7). Migration re-verifies every gated cell in-band against the
recorded numbers or records a version-policy re-derivation (R7). The
iai instruction gate, criterion shred, and hyperfine cold-start
protocol are retained unchanged (R8). Zero new dependencies, zero
runtime-crate changes, SPI frozen.

## Technical Context

**Language/Version**: Rust stable (2024 edition), workspace v0.2.0

**Primary Dependencies**: rdlt-bench uses existing workspace deps
(`rdlt` full-featured, `serde`, `serde_json`, `toml`, `tokio`) plus
clap (derive) for its CLI — dev-only, owner-approved during
implementation (R1 amendment); runtime crates gain nothing

**Storage**: TOML inputs (`benches/cells/`, `benches/bars.toml`,
competitor variants); committed JSON artifacts (`benches/results/`)

**Testing**: unit cells for cell/bars parsing (typed-error offenders),
statistics, artifact schema round-trip, gate verdict logic, report
marker splicing; a self-test cell (`kind = "none"` fixture, trivial
subprocess) exercises the protocol end-to-end in nextest without
containers; container-backed runs are operator-driven (like today)

**Target Platform**: Linux (procfs + cgroup v2; the reference machine)

**Project Type**: Rust workspace + benchmark assets; one NEW dev-only
crate (`publish = false`)

**Performance Goals**: none for the harness itself; the harness must not
perturb measurements (sampler ~50 ms cadence, measurement loop does no
allocation-heavy work while timing)

**Constraints**: safe Rust only (no getrusage FFI — procfs instead,
R3); SPI frozen; `make check` semantics untouched; gated bars must
survive migration in-band or via version-policy entries (R7); dlt wall
numbers stay self-timed for continuity (R4)

**Scale/Scope**: ~12 existing cells to migrate + bars for the 8 gated
rows; 3 dlt variants; one new crate ≈ small (cells, fixtures, sampler,
stats, gate, report modules); benches/ relayout

## Constitution Check

Constitution file remains the unfilled template; governing principles
carried from 001–011. **Seams sacred**: PASS — consumes public seams
(`RunReport`, `events()`, the CLI); zero SPI change. **No silent
failures**: PASS — typed loader errors, loud MISSING baselines, null
metrics carry reasons, gate fails naming the offender. **Correctness
before speed**: PASS — verify blocks prove loads happened; continuity
protocol prevents silent renumbering. **Measured, not asserted**: PASS —
this feature is that principle turned into tooling; fingerprints +
committed artifacts make every number reproducible-or-refused. **Safe
Rust**: PASS — R3 explicitly rejects the libc route; procfs/cgroup are
text files.

Post-design re-check: PASS.

## Project Structure

### Documentation (this feature)

```text
specs/012-bench-harness/
├── plan.md              # This file
├── research.md          # R1–R8
├── data-model.md        # Cell / Fixture / CompetitorVariant / Artifact / Bar
├── quickstart.md        # list / run / gate / report
├── contracts/
│   └── bench-harness.md # BH1–BH8
└── tasks.md             # /speckit-tasks output (NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
crates/rdlt-bench/                # NEW dev-only member, publish = false
├── Cargo.toml
└── src/
    ├── main.rs                   # arg parsing (house style), subcommand dispatch
    ├── cells.rs                  # TOML model + validation (BH1)
    ├── fixtures.rs               # container/dataset lifecycle + identity hashes
    ├── protocol.rs               # env guard, warmups, runs, stats (BH2)
    ├── sample.rs                 # procfs sampler thread (R3)
    ├── library_mode.rs           # in-process run: RunReport + events attribution
    ├── competitors.rs            # dlt module: image, variants, cgroup delta (R4)
    ├── artifact.rs               # schema v1, read/write (R5)
    ├── gate.rs                   # bars.toml evaluation (BH6)
    └── report.rs                 # RESULTS.md generated-section splicing (BH7)

benches/
├── cells/{e2e,pg,cdc,merge}.toml # migrated cells (US3)
├── cells/pipelines/*.yaml        # pipeline spec templates
├── fixtures/                     # seed SQL, dataset generators (from baseline/)
├── competitors/dlt/              # Dockerfile, variants.toml, entry scripts
├── bars.toml                     # the 8 gated bars (R6)
├── results/                      # committed artifacts (+ raw/ gitignored)
├── RESULTS.md                    # narrative + generated sections
├── README.md                     # rewritten for the new layout
├── compare-iai.sh                # RETAINED (iai gate)
└── perf-baselines.json           # RETAINED (iai baselines)
                                  # run-*.sh: DELETED in US3
Cargo.toml                        # + member crates/rdlt-bench
Makefile                          # bench verbs delegate to rdlt-bench (R8)
```

## Design Notes (delta-level)

- **US order is build order**: US1 (crate + cells + protocol + fixture
  lifecycle, self-test cell green in nextest) → US2 (sampler,
  competitors/cgroup, artifacts, gate, report) → US3 (migrate cells,
  paired re-measure on the reference machine, delete scripts, rewrite
  README, regenerate RESULTS tables).
- **Continuity is the migration gate** (R7): each migrated gated cell's
  same-session paired median must land in the documented ±2–10% band of
  its recorded number; out-of-band → diagnose, and only an explained,
  accepted delta gets a version-policy entry. The first full-matrix
  re-measure doubles as the evidence artifact.
- **RESULTS.md markers**: report splices between explicit
  `<!-- rdlt-bench:BEGIN <table> -->` markers; everything outside is
  narrative and preserved byte-for-byte (BH7). History section stays
  hand-written forever.
- **Cold start**: declared as a cell with `mode = "hyperfine"`; the
  harness shells the exact recorded protocol (20 runs, 3 warmups) and
  parses hyperfine's JSON — instrument unchanged, governance unified.
- **Library mode is additive detail**: gated numbers bind to subprocess
  wall time (FR-011); library-mode twins of hot cells feed attribution
  scoreboards only, so no recorded number changes meaning.

## Verification Map (story → proof)

| Story | Proof surface |
|---|---|
| US1 harness/cells | loader typed-error cells; self-test cell runs the full protocol under nextest; `list` output pins coordinates |
| US2 metrics/gate/report | artifact schema round-trip cells; gate verdict cells (tightened-bar fails naming cell); report splice cells (narrative byte-identical); one real gated cell's artifact carries every BH3 metric |
| US3 migration | continuity table (per-cell old/new/delta/verdict) in evidence; zero run-*.sh remaining; full gated set green via `rdlt-bench gate` |
| Governance | make check + doc-tests + semver-checks green; `git diff` shows zero runtime-crate manifest changes (BH8) |

## Phase 2 note for /speckit-tasks

Order: crate scaffold + cell/bars loaders WELDED to their typed-error
cells → protocol + fixtures + self-test cell → sampler + artifact →
competitors module (absorbing baseline/) → gate + report → migration
per suite (e2e, pg, cdc, merge — parallelizable) → the paired
re-measure session + continuity record → script deletion + README +
Makefile rewiring last (nothing is deleted before its replacement has
measured in-band).
