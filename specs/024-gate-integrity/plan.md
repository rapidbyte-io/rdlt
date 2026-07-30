# Implementation Plan: Test-gate integrity

**Branch**: `024-gate-integrity` | **Date**: 2026-07-30 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/024-gate-integrity/spec.md`

## Summary

Eight measured defects let the local verification gate report green while
verifying less than it appears to. This feature fixes the gate, changing no
product behavior.

The approach turned out cheaper than the spec's framing implied, because Phase 0
found that the test runner already defaults to failing on an empty selection
(research R1). So the dominant fix is **deleting nine permissive flags**, not
building new tooling. The genuinely new work is three things: a shared
source-scanning helper so a dropped crash point cannot hide (ten registries, six
crates — R4), symmetric assertive probe modes plus a committed count baseline so
a disarmed suite is visible (R8), and a local `make semver` against a recorded
baseline sha because the automated one compares against a 73-commit-stale
reference (R5).

Phase 0 also **found a live instance of the defect class while looking for it**:
`make test TARGET=prop` selects `test(shred_property)`, a test-name filter
matching nothing — the binary is `shred_property`, the test inside it is
`shred_invariants_hold` — so the 4,096-case property run has been reporting
success while running zero tests (R0). That is the feature's premise
demonstrated, and it is the first detection demonstration FR-015 requires.

## Technical Context

**Language/Version**: Rust, pinned 1.96.0 (`rust-toolchain.toml`;
`RUSTUP_TOOLCHAIN` in the environment silently overrides it, so every gate run
uses `env -u RUSTUP_TOOLCHAIN`)

**Primary Dependencies**: `cargo-nextest` 0.9.135 (the runner whose `--no-tests`
semantics this feature relies on), `cargo-semver-checks`, `cargo-llvm-cov`, GNU
Make. **No new dependency is added.**

**Storage**: N/A — no persisted product data. One new committed artifact: a
per-binary test/skip count baseline, plain text, reviewed as a diff.

**Testing**: `cargo nextest run`; doc-tests `cargo test --doc`. New tests are
gate-verification tests: one per crash-point registry, one pinning runner group
membership, one pinning probe-mode behavior.

**Target Platform**: Linux development host. Container-backed suites use rootless
podman; the host has a known consecutive-run port-contention issue mitigated by
`make reclaim` plus a TIME_WAIT drain.

**Project Type**: Rust workspace, 14 crates. This feature touches build/test
tooling (`Makefile`, `.config/nextest.toml`) and `rdlt-testkit`, plus one
verification test per crash-point registry.

**Performance Goals**: N/A as a product metric. The gate's added wall-clock cost
is a recorded measurement (SC-010), not a target — the point is to know the
price, not to hit a number.

**Constraints**: The gate may not become easier to pass in any respect (FR-013).
No product behavior, persisted format, generated SQL, or user-facing
configuration/CLI vocabulary may change (FR-016). The 101.5-minute
credential-gated sweep stays out of the routine gate. Skip-not-fail remains the
default for resource-gated suites; the assertive mode is opt-in.

**Scale/Scope**: 9 permissive selectors, 10 crash-point registries across 6
crates, 1 orphaned target, 1 never-compiled test file, 1 implicit runner group, 2
probe families, 2 unexecutable recorded practices. Each is a countable item that
must carry a disposition at close-out.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| principle | assessment |
|---|---|
| **I. Small Core, Verified Breadth** | PASS. Adds no product surface. Strengthens the "verified" half, which is the principle's own stated rationale for the gate. |
| **II. Library-First, Thin CLI** | N/A. No library or CLI surface changes. |
| **III. One-Boundary Wrapping** | PASS. The one new abstraction — the source-scanning helper — lives in `rdlt-testkit`, already the single home for shared test machinery. No third-party type crosses a new boundary. |
| **IV. Exactly-Once Is Sacred** | **PASS, and in direct service of it.** The crash sweeps ARE the evidence for the exactly-once claim, and ten of eleven registries cannot currently detect a dropped point. No sweep's behavior changes; each gains an assertion that its matrix is complete. |
| **V. Typed Error Taxonomy** | PASS. No product error path is touched. New failures are test assertions naming a divergence, not classified errors, and no rendered error is substring-matched. |
| **VI. Self-Contained Comments** | PASS, and load-bearing here: every retained permissive flag, every exemption, and the baseline sha must carry its reason AT THE SITE, with no feature or task ID in the text. |
| **VII. Test-and-Verification Gate (NON-NEGOTIABLE)** | **PASS — this feature is the principle's own enforcement.** Skip-not-fail for container-backed tests stays the default, as the principle requires; the assertive mode is additive and opt-in. Coverage ≥80% and close-out matrix discipline apply to this feature too. |
| **VIII. Benchmark Governance** | PASS. No bar proposed, added or moved. The `bench` targets are read during the FR-014 audit; their governance is untouched. |
| **IX. Contracts and Persisted Formats Are Frozen** | PASS. FR-016 restates this as a requirement. Golden SQL pins, WAL v2, bench artifact v3 and `_rdlt_*` shapes unmodified; the count baseline is a NEW artifact, not a change to an existing format. |

**Gate result: PASS, no violations — Complexity Tracking is therefore empty of
justifications.**

One point deserves statement rather than a checkmark. Principle VII *mandates*
skip-not-fail, and US4 introduces a mode that fails instead. These do not
conflict: the mandate is about the DEFAULT, so a contributor without a container
runtime can still run the gate, and that default is preserved unchanged. The
assertive mode is opt-in and serves the opposite audience — a maintainer on a
machine where resources ARE present, who needs to know the legs actually ran.
Adding it makes the gate strictly harder to pass vacuously, which is FR-013's
direction.

## Project Structure

### Documentation (this feature)

```text
specs/024-gate-integrity/
├── plan.md              # This file
├── research.md          # Phase 0 — R0..R10, two self-corrections recorded
├── data-model.md        # Phase 1 — the gate's own entities
├── quickstart.md        # Phase 1 — how a maintainer runs and reads the gate
├── contracts/
│   └── gate-integrity.md   # GI1..GI8
├── checklists/
│   └── requirements.md  # spec quality checklist (3 failures found and fixed)
└── tasks.md             # Phase 2 — NOT created by /speckit-plan
```

### Source Code (repository root)

Tooling and test-side verification only. No product `src/` behavior changes; the
only `src/` edits are `rdlt-testkit` additions and documentation corrections
where a registry's module doc cites a suite by the wrong name.

```text
Makefile                          # 9 flag dispositions; +TARGET=e2e in check;
                                  #   +semver verb; +snowflake failpoints lint leg;
                                  #   coverage exclusion codified
.config/nextest.toml              # spelling kept; membership pinned by a test
crates/rdlt-testkit/src/
├── crash.rs                      # + shared source-scanning registry assertion
├── containers.rs                 # + RDLT_TESTKIT_REQUIRE_CONTAINERS
└── snowflake.rs                  # + RDLT_TESTKIT_REQUIRE_SNOWFLAKE
crates/rdlt-testkit/tests/
└── gating_pin.rs                 # NEW — probe decisions under forced environments
crates/rdlt-connector-{postgres,file,duckdb,rest,iceberg,snowflake}/tests/
                                  # one registry-vs-sources assertion per registry (10)
crates/rdlt-connector-iceberg/tests/config_schema.rs
                                  # + runner group membership pin
```

The count baseline's exact location and granularity are deliberately open — see
research open question 1. It is decided when the first real diff is visible, not
guessed now.

## Implementation phases

Five increments, ordered so the cheapest, highest-leverage fix lands first and
each is independently mergeable with the gate green. **US1 must land before the
others**: until an empty selection fails, any later fix can silently regress.

| # | story | scope | why here |
|---|---|---|---|
| 1 | US1 | Nine flag dispositions, including the R0 selector fix | Highest leverage, mostly deletion, makes eight other checks honest. The R0 fix is the first detection demonstration. |
| 2 | US2 | `TARGET=e2e` into `check`; enumerate every suite and disposition it | Depends on US1 — adding a target whose selector could pass empty would add a lie, not a check. |
| 3 | US3 | Shared scanner in testkit; one assertion per registry (10) | Largest new-code increment. Independent of US1/US2, scheduled after so "demonstrate detection" is already established on cheaper ground. |
| 4 | US4 | Assertive probe modes; `gating_pin.rs`; count baseline | Needs US1–US3 landed, so the baseline records the FIXED gate rather than the broken one. |
| 5 | US5 | `make semver` with recorded sha; coverage exclusion codified; FR-014 audit closed | Last, because the audit must examine the gate as this feature leaves it. |

**Every increment ends with**: `env -u RUSTUP_TOOLCHAIN make check` green —
preceded on this host by `make reclaim` and a TIME_WAIT drain if a gate ran
recently — plus a recorded detection demonstration for each defect the increment
fixed (FR-015): the gate observed FAILING on a deliberate regression, then green
once reverted, with output captured.

## Complexity Tracking

No constitutional violation, so nothing requires justification. One decision is
recorded here because it *looks* like added complexity and is a reduction:

**The shared scanner is one implementation used ten times, not ten copies.** The
alternative — copying the engine's thirty-line scanner into each crate — was
rejected in R4 because a scanner that drifts **fails open**: it finds fewer sites
and the assertion still passes. Ten copies would be ten independent chances at a
silent failure of the very check being added.

## Risks and how each is handled

| risk | handling |
|---|---|
| Removing a permissive flag breaks a contributor's build for lacking a container runtime | Measured in R2: these binaries are SELECTED and then self-skip internally, so an empty selection was never the mechanism. `--no-tests` governs selection, not execution — the two were conflated when the flags were added. |
| The added `e2e` and lint legs slow the gate | Measured and recorded per SC-010. If the figure is unacceptable the disposition changes — but it changes on a number, not a hunch. |
| The count baseline becomes a number nobody reads, bumped reflexively | It is a committed file reviewed as a diff, and deliberately NOT a hard assertion (R8): a difference is a report requiring a reason in the commit, not an automatic failure that trains people to bump it. |
| The pinned semver baseline goes stale | By design, and stated as such (R5): advancing it is a deliberate act visible in a diff, which is the opposite of the current invisible drift against a 73-commit-stale reference. |
| A gate-hardening feature verified only by "the gate still passes" | FR-015 forbids exactly this. Every fixed defect needs an observed failure-then-recovery, recorded with its output. |
