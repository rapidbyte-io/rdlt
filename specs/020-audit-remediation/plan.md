# Implementation Plan: Audit Remediation — Silent Losses Closed, the Record Made True

**Branch**: `020-audit-remediation` | **Date**: 2026-07-26 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/020-audit-remediation/spec.md`

## Summary

Execute `NEXT_STEPS.md` as eleven independently mergeable increments in
value-per-risk order. The work is four kinds and they must not be confused:

1. **Silent data loss, reachable today through ordinary configuration.** A
   `u64` above `i64::MAX` becomes NULL with no error and no discard count; a
   grown-and-rewritten parquet input is resumed into on stale evidence; a
   full-refresh load leaves the previous load's files behind when the output
   format or partitioning changed. These are the P1 increments and each ships
   with a pin demonstrated red on the pre-fix build.
2. **A promise the engine does not keep.** `Freeze` is documented without a
   per-run qualifier and enforced only within a run, while the field persisted
   for cross-run detection is written and never read. Resolving it is the
   feature's one genuine design increment — and it resolved by NARROWING the
   promise and fixing the within-run hole (a mid-run child table escaped every
   contract), not by building cross-run enforcement. The design review that
   settled it is the strongest evidence in this feature; see close-out D-10.
3. **Hardening and honesty.** Sixteen small defects, a mutation record that
   describes code deleted nine features ago, a missing LICENSE, and a project
   instruction file that tells every future session the last feature is
   unimplemented.
4. **Questions, not instructions.** The performance queue is measurement-first
   throughout. Two allocation removals in 019 measured *worse* on airtight
   counting arguments (D-13, D-21); that is the standing null hypothesis, and
   a queue that ends in recorded negatives is a successful outcome.

**Phase 0 changed the plan three times, and those reversals are the most
valuable output of this round.** Each of the three riskiest designs was written,
then attacked against the code, and each failed:

| design | what the attack found | adopted shape |
|---|---|---|
| Cross-run schema baseline | Governing only the `CreateTable` arm re-creates the audited defect in mirror image: run N+1's drain 2 fires **Freeze on a column the pipeline itself established**. Plus a panic on the first drain of every second run, an inert version bump, and no child-table inheritance. | Corrected to two diffs per drain — then the CORRECTION was itself attacked before implementation and **the whole cross-run baseline was abandoned**: it reported a narrowing as drift (aborting conforming runs, panicking the debug gate) and its Discard path deleted a column uncounted. Shipped instead: within-run enforcement + ancestry inheritance, no persisted change. See close-out D-10 |
| Hinted-misfit counter | The difference-of-totals formula **underflows on an ordinary nullable list column** — `[{"tags":[…]},{"tags":null}]` panics in debug, wraps to `u64::MAX` in release, and that value reaches destination-persisted commit metadata. | Positional count of present-input/NULL-output cells — R2.4 |
| Parquet resume hash | The descriptor was built only when verification ran, so the **first upgraded resume records a hash over the wrong range** and every later legitimate append hard-fails, permanently, for 100% of pre-existing cursors. | Build the descriptor unconditionally; no `CURSOR_FORMAT_VERSION` bump (the in-tree precedent adds such fields additively); mirror jsonl's arming filter — R3.3/R3.4/R3.5 |

Phase 0 also **corrected the audit itself** in two places, which the close-out
must record rather than repeat: the type-hint defect does *not* create a child
table (it turns a preserved-verbatim JSON column into a NULLed one — worse, and
in the other direction), and the Iceberg field-ID divergence is not "plausible"
but **guaranteed** for any schema with a struct followed by another column.

Two decisions bind the whole feature:

- **CI repair is out of scope** (spec E1 — organisation billing). The local
  gate is the gate of record. Items verifiable only on a hosted runner land
  reviewed with their verification recorded as unperformed; they never block.
- **Publishing is out of scope** (spec E2); readiness is not.

## Technical Context

**Language/Version**: Rust 1.96.0, edition 2024, pinned by
`rust-toolchain.toml`; MSRV floor `rust-version = "1.96"`
(`Cargo.toml:22`). Workspace denies `unsafe_code` (`[workspace.lints.rust]`),
sole sanctioned exception the CLI `mallopt` FFI. Nothing in this feature
requires unsafe.

**Primary Dependencies**: unchanged by this feature except one direct edge.
`arrow`/`arrow-schema`/`parquet` 58.3 workspace-wide, `tokio` 1.x
multi-thread, `serde`/`serde_json`, `thiserror` 2, `tracing` 0.1,
`blake3` 1, `chrono` 0.4, `object_store` 0.12 (aws), `iceberg`/
`iceberg-catalog-rest`/`iceberg-storage-opendal` `=0.10.0`, `reqwest` 0.12,
`testcontainers` 0.23.3, `fail` 0.5.

**The one dependency change**: `httpdate = "1"` becomes a direct dependency of
`rdlt-connector-rest`. **Registry facts**: `httpdate 1.0.3` is already in
`Cargo.lock` (pulled by `hyper 1.10.1`, which `reqwest` already requires), so
the direct edge costs **zero new dependency-tree entries** — it is a new edge
for one crate, not a new crate. Hand-rolling was priced and rejected: the code
would be in the tree either way and HTTP-date parsing has real edge cases.
Everything else this feature needs is already present: `tracing::Instrument` is
re-exported unconditionally and `tracing-attributes` is already in the lock;
`testcontainers` already exposes `ImageExt::with_label`; `blake3` is already a
direct dependency of `rdlt-connector-file`; `valgrind --tool=dhat` is already a
hard prerequisite of the iai gate.

**Storage**: **no persisted format changes at all.** The `StateDoc` change
planned here (gain `schemas`, lose `schema_hashes`, `STATE_FORMAT_VERSION` 1→2)
was **withdrawn during implementation** after a pre-implementation design review
found two critical defects in the enforcement it existed to support — see
close-out D-10. `StateDoc` keeps its shape and version. The one shipped
persisted change is an **additive optional field**
(`FileProgress.row_groups_hash`, `#[serde(default,
skip_serializing_if)]`, **no** version bump, matching how `etag` and
`tail_hash` were added in `91eab01`). WAL format v2, receipts, golden SQL
pins, and emitted `_rdlt_id` bytes are all unchanged.

**Testing**: `cargo nextest run` (doc-tests `cargo test --doc`), crash sweeps
behind `--features failpoints`, golden SQL pins, `shred_identity_pin` corpus,
proptest, `cargo mutants`, `cargo llvm-cov`, iai instruction counts, container
fixtures that skip-not-fail. The full local gate is
`make check` = lint + test + `TARGET=sweep` + `bench TARGET=iai` +
`bench TARGET=cold`.

**Target Platform**: Linux x86-64; developed inside distrobox (the Fedora
Atomic host lacks a C toolchain). Container-backed legs need rootless podman.

**Project Type**: Rust cargo workspace — an embeddable ELT library behind the
`rdlt` facade plus a thin CLI, 13 crates, ~62k lines.

**Performance Goals**: none set by this feature. Its performance requirement is
that every queued question ends with a recorded number and a decision
(FR-075/FR-080); no percentage target is claimed. The standing matrix
(13.2× / 1.7× / 95.0× / 63.6× / 2.6× vs dlt) must not regress and all four
bars must still pass (FR-082).

**Constraints**: every increment leaves the full local gate green and is
independently revertible; behaviour changes confined to the named defects;
persisted formats and golden pins byte-identical unless explicitly versioned;
typed error taxonomy with no substring matching in tests; comments state live
invariants only; coverage ≥ 80% baseline-first.

**Scale/Scope**: ~120 audit items across eight sections; 29
verification-confirmed defects; 82 functional requirements; 11 increments; 12
publishable crates.

## Constitution Check

*GATE: evaluated before Phase 0 and re-evaluated after. Constitution v1.1.0.*

| Principle | Assessment | Evidence |
|---|---|---|
| **I. Small core, verified breadth** | **PASS.** No connector, destination, or pipeline capability is added. Two config fields appear (`request_timeout_secs`, `row_groups_hash`) and both exist to close a defect, not to grow surface. The one new dependency edge costs zero tree entries. Two phase-2 doors were re-probed and deliberately left **closed** (R7.7). | R5.1, R5.4, R7.7 |
| **II. Library-first, thin CLI** | **PASS.** The only CLI changes are exit-code classification and two diagnostics; no capability is added to the CLI that the library lacks. | R8.7, R8.10 |
| **III. One-boundary wrapping** | **PASS, and strengthened.** The Iceberg structural comparison stays inside `dest/schema.rs`, the module that owns ID assignment; no `iceberg` type moves to the crate's public surface. `object_store::Error` classification collapses to **one** rulebook. | R7.2, R4.5 |
| **IV. Exactly-once is sacred** | **PASS, and strengthened.** A new crash point `pg.tx.acked` closes the recorded sweep blind spot — the state where the destination committed and the client never learned, which is how two real defects survived 23/23. WAL replay gains a row-count cross-check that degrades to re-extraction rather than trusting a short span. | R9.15, R11.7 |
| **V. Typed error taxonomy** | **PASS, and it constrained the design.** A typed `DiscardReason` was the better end state but is breaking, so the alternative — two free-form strings only substring-matching could separate — was **rejected outright** and the distinction is deliberately not made (R2.5). Five internal-invariant sites reclassify from `Config` to `Internal`; three encoder wraps become typed refusals; deterministic storage failures stop being retried. No test asserts on rendered text. | R2.5, R8.1–R8.3, R8.6, R4.5 |
| **VI. Self-contained code & comments** | **PASS.** Four false or stale comments are corrected as part of the increments that own their subjects, including one that claims test coverage which does not exist. New invariants are stated at their sites. No unsafe. | R9.4, R3.3, R1.7 |
| **VII. Test-and-verification gate** | **PASS.** Every fix ships a red-before-green pin. Where a live container is the only realistic reproduction, the red pin is captured container-free because *a skipping test is green* and therefore inadmissible as FR-001 evidence (R7.4). Mutation testing is regenerated against the current tree. Coverage ≥ 80%. Container legs still skip-not-fail. | R7.4, R9.1–R9.3, FR-001 |
| **VIII. Benchmark governance** | **PASS.** No new bar is proposed. All four existing bars must still pass. Every performance item is measurement-gated and a negative result is a valid, recorded outcome. | FR-075, FR-082, R12 |
| **IX. Frozen contracts & persisted formats** | **PASS with one recorded versioned change and one deliberate non-bump.** `STATE_FORMAT_VERSION` 1→2 with migration note **and the version actually stamped** — Phase 0 found the naive bump would have been inert (R6.4). `FileProgress` gains an optional field with **no** bump, matching the in-tree precedent; Phase 0 rejected a bump there because it would have changed jsonl and csv documents that this fix does not touch, and would have made the increment non-revertible (R3.4). | R6.4, R3.4 |

**Gate result: PASS.** One item requires justification and is recorded in
Complexity Tracking: the `StateDoc` change is breaking on a semver-sacred
crate.

### Constitution re-check, post-Phase-0

Re-evaluated after research. No principle moved to a violation. Three
findings *strengthened* the assessment: the typed-taxonomy principle killed the
two-strings design before it was written (V); the "a skipping test is green"
observation closed a hole in what counts as red-before-green evidence (VII);
and the persisted-format principle produced opposite answers for the two format
questions on their merits rather than by reflex (IX). The dependency question
was settled with registry facts rather than preference (I).

## Project Structure

### Documentation (this feature)

```text
specs/020-audit-remediation/
├── plan.md              # This file
├── spec.md              # Feature specification
├── research.md          # Phase 0 output — decisions of record, incl. 3 reversals
├── data-model.md        # Phase 1 output — types and persisted shapes that change
├── quickstart.md        # Phase 1 output — how to work an increment
├── contracts/
│   └── audit-remediation.md   # AR1–AR8
├── checklists/
│   └── requirements.md  # Spec quality checklist (complete)
└── tasks.md             # Phase 2 output — NOT created by /speckit-plan
```

### Source code (repository root)

Existing workspace; this feature adds no crate and no module tree. Touched
areas, by increment:

```text
crates/
├── rdlt-core/           # StateDoc v2 (schemas replaces schema_hashes); naming bound;
│                        #   policy pin; arrow-schema dependency removed
├── rdlt-connector/      # SPI: the one byte-budget channel core (engine copy DELETED)
├── rdlt-engine/
│   ├── shred/           # UInt→Utf8, hint pin, decimal precision, misfit counting,
│   │                    #   child-table policy, build.rs gains its first test module
│   ├── schema/          # baseline diff, contract violation pins
│   ├── load/            # lowering totality + parity pin, Config→Internal, byte_size pin
│   ├── runtime/         # two diffs per drain, version stamping, span Instrument,
│   │                    #   WAL residue clearing, hint validation; channel.rs DELETED
│   └── wal/             # segment-sequence pin, row-count cross-check, recovery offload
├── rdlt-connector-file/ # resume integrity, ownership/truncation, classification,
│                        #   retention, RAII temp dir, skip-fetch (US11)
├── rdlt-connector-rest/ # timeout, one client, pagination validation, Retry-After date,
│                        #   token generation, path encoding, header blocklist
├── rdlt-connector-iceberg/  # structural drift comparison, nullability, nested live cell
├── rdlt-connector-postgres/ # encoder refusals, pg.tx.acked crash point
├── rdlt-connector-duckdb/   # classify at probes/DDL, comment-tag sweep
├── rdlt-connector-sqlcore/  # dedup seam, shared index SQL (+ its first golden pin)
├── rdlt-cli/            # exit-code taxonomy, two diagnostics
├── rdlt-testkit/        # container labels, fixture version constant
└── rdlt-bench/          # report-read honesty, CARGO_TARGET_DIR, history annotation

LICENSE                  # NEW
benches/, Makefile, tools/, fuzz/, .cargo/mutants.toml, .config/nextest.toml
```

**Structure Decision**: unchanged. This is a remediation feature over an
existing 13-crate workspace; every change lands in the crate that owns the
defect. The only structural deletions are
`crates/rdlt-engine/src/runtime/channel.rs` (superseded by the SPI core, D17)
and dead dependency declarations.

## Phase Sequencing

Eleven increments in spec story order. Each merges with the full local gate
green and is independently revertible.

| # | Increment | Story | Notes and hard sequencing |
|---|---|---|---|
| 1 | Record and license | US1 | Doc/comment only, zero behaviour. Land first: it is what makes later planning correct. |
| 2 | Shred value fidelity | US2 | Identity corpus frozen and asserted byte-identical. Two schema-affecting changes recorded. |
| 3 | File ownership and truncation | US3 | Key-derivation fix and the widened ownership predicate ship together — a widened predicate on a listing that can still mis-split is worse than neither. |
| 4 | Resume integrity | US3 | **Must precede increment 11's skip-fetch** (R12.6): the fetch reorder changes what the planner sees. |
| 5 | Classification, formats, retention | US3 | One rulebook for storage failures; CSV two-pass; RAII temp dir; commit-log bound; cursor growth documented and pinned. |
| 6 | REST robustness | US4 | Timeout + single client land together (the client is where the timeout lives). |
| 7 | Schema contracts | US5 | Two commits: (a) baseline persisted and stamped, written but not yet read — safe alone; (b) the two-diff enforcement + child-table inheritance. **Breaking; see Complexity Tracking.** |
| 8 | Iceberg nested types | US6 | Red pin is container-free (R7.4); the live cell is confirmation. Polaris tag pinned by live probe at increment start. |
| 9 | Engine sharp edges | US7 | Sixteen small fixes, each with its own pin; groups cleanly because none shares a file with another increment's subject. |
| 10 | The gate | US8 | Mutation re-run is a **fresh full run** (the cache is dead across the 017 renames); budget 60–90 min. Its decimal grammar table lands **after** increment 2 shipped the precision refusal. |
| 11 | Publish readiness | US9 | Runs the local semver check knowing increment 7 broke `rdlt-core` — the break is expected and recorded, not a surprise. CI-only items recorded unperformed. |
| 12 | Recorded deferrals | US10 | D17 channel unification (engine copy deleted), lowering parity mechanized, `DestSpec::File` embedded, sqlcore moves, dependency hygiene. Increment 10's parity pin is written as a schemars field-set pin so it survives this embedding. |
| 13 | The performance queue | US11 | Last: measurement capacity is the scarce resource and should be spent after the code under measurement stops changing. |

*(Thirteen increments deliver eleven stories: US3 splits into three and US5
into two commits, per the research sequencing above.)*

## Complexity Tracking

| Violation | Why needed | Simpler alternative rejected because |
|---|---|---|
| **Breaking change to `rdlt-core`**: `StateDoc` loses `schema_hashes` and gains `schemas` (a public field of a public type in a semver-sacred crate, re-exported by `rdlt-connector`) | FR-028/FR-029 require the promise and the enforcement to agree and forbid a persisted field that is written and never read. The field cannot be diffed — it is a digest of the whole schema and can only prove inequality, which is exactly the FR-031 false-positive trap. | Keeping `schema_hashes` alongside `schemas` violates greenfield (a superseded field kept alive) and leaves the unreadable digest in the format forever. Narrowing the contract instead (option B) was priced at ~0 code and **rejected on merit**: essentially every drift a user cares about appears at a run boundary, so a contract that resets each run is close to worthless while destinations quietly apply the drift as additive DDL. **Consequence, recorded**: this converts the standing publish-time 0.2 → 0.3 bump into a *required* one. Nothing is published yet, so no consumer is broken; `cargo semver-checks` will flag it locally and that flag is the intended, recorded outcome. The CI semver job cannot run (billing) and its verification is recorded as unperformed. |
| **One new direct dependency edge**: `httpdate` for `rdlt-connector-rest` | FR-043 requires server pacing honoured in every standard form; the HTTP-date form is currently ignored, so a date-sending rate limiter falls back to generic backoff. | Hand-rolling was priced. Rejected because the crate is **already in the tree** via `hyper` (zero new entries either way), so hand-rolling buys nothing and costs the edge cases. Principle I's default-no is satisfied by the registry fact, not by preference. |

No other deviation. In particular: no new crate, no new module tree, no
`unsafe`, no new persisted format, no new bar, and no capability added to the
engine.

## Phase 0 → research.md

Complete. Twelve research clusters plus an adversarial pass; 147 decisions;
three designs overturned and their corrections adopted; six unknowns carried
forward with the probe that settles each (none blocking). See
[research.md](research.md).

## Phase 1 → data-model.md, contracts/, quickstart.md

Complete. See [data-model.md](data-model.md),
[contracts/audit-remediation.md](contracts/audit-remediation.md), and
[quickstart.md](quickstart.md).

## Spec amendments made at plan time

Recorded here rather than silently applied, per the project's convention:

1. **US6's severity.** The spec described the Iceberg field-ID divergence as
   pinned by nothing "either way". Phase 0 confirmed it is **guaranteed**, not
   plausible, for any schema in which a struct or list column is followed by
   another top-level column (R7.1). The acceptance scenarios are unchanged;
   the framing is corrected.
2. **FR-024's preferred branch is confirmed feasible.** The spec left counting
   versus documenting to plan-time analysis. Counting is taken — but by a
   positional count, not the obvious differential, which Phase 0 showed
   panics on an ordinary nullable list column (R0.2/R2.4).
3. **FR-028's resolution was (A) at plan time and is now (B), narrow the
   contract** — reversed on evidence during implementation. A pre-implementation
   design review found two CRITICAL defects in (A) (a narrowing reported as
   drift, aborting conforming runs and panicking the debug gate; and a
   Discard-path rollback that deletes a whole column uncounted), and concluded
   every hole came from the cross-run baseline rather than from the
   requirements. See close-out D-10. **Consequence: the breaking `rdlt-core`
   change in Complexity Tracking is WITHDRAWN** — no persisted format changes,
   the 0.2 → 0.3 bump returns to standing rather than required, and cross-run
   detection is a named deferral reframed as a report rather than a gate.
