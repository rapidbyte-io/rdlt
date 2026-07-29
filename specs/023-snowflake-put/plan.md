# Implementation Plan: Snowflake Internal-Stage Ingestion, and the Retirement of Two Paths

**Branch**: `023-snowflake-put` | **Date**: 2026-07-29 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/023-snowflake-put/spec.md`

## Summary

Make the service's own recommended ingestion mechanism the connector's **only**
one, and delete the two workarounds that existed because it was unreachable.

022 verified in source that internal-stage upload could not be reached through
the adopted library, and shipped two substitutes: rows carried inside statements
(the default, needing no infrastructure) and files written to a bucket the user
supplied (opt-in). A fork of the library now implements the upload. With it
reachable, the bucket path has no reason left — it substituted for storage the
user had to provide — and the statement path loses its justification, because
service-provided storage needs no user infrastructure either.

The technical approach: consume the fork at the existing single library
boundary, replace the storage backend behind the connector's staging seam with
one that builds a local part and uploads it, and delete both superseded paths
together with their configuration, credential handling, encoding routines,
tuning constants, test suites and dependencies. Four of the connector's runtime
dependencies fall out. The crash-sweep matrix halves.

## Technical Context

**Language/Version**: Rust, workspace pinned to 1.96.0 via `rust-toolchain.toml`.
Note: `RUSTUP_TOOLCHAIN` in the developer environment silently overrides that
pin; gates must run as `env -u RUSTUP_TOOLCHAIN make check`.

**Primary Dependencies**: `snowflake-connector-rs` consumed from a fork
(`rapidbyte-io/snowflake-connector-rs`, branch `feat/put-file-upload`) pinned by
exact revision, features `["key-pair-auth", "put"]`, **`version` key
deliberately omitted** (see Constitution Check — this is load-bearing, not
stylistic). `parquet` and `arrow-array` stay. `object_store`, `bytes`, `futures`
and `chrono` are expected to fall out of the connector; each must be verified by
use-search before removal.

**Storage**: staging area provided by the service, addressed as a named,
schema-scoped object per pipeline. Local temporary files during part
construction only.

**Testing**: `cargo nextest run`; doc-tests via `cargo test --doc`. Live legs
credential-gated, skip-not-fail, announcing the skip. Crash sweep with
armed-fire pins. Differential oracle against the postgres destination.
Conformance via the testkit clauses.

**Target Platform**: Linux; the connector is embeddable and also reached through
the CLI.

**Project Type**: Rust workspace — connector crate within an embeddable engine.

**Performance Goals**: none as a gate. Supersession is *recorded* against 022's
figures (statements 582 rows/s; bucket path 2,191 rows/s at 250k and 1,941
rows/s at 1M, on a 12-column row shape that must not change). Benchmark
governance forbids a bar on a hosted-service cell.

**Constraints**: exactly-once is preserved unchanged; staging stays inside the
atomic unit (measured safe); the staging object's teardown must not, because
dropping it commits the unit (measured); emitted SQL of the other destinations
stays byte-identical; no `unsafe`; no compatibility shims.

**Scale/Scope**: one connector crate plus testkit and two fixtures. Deletion
substantially exceeds addition. The crash-sweep matrix loses its path dimension.

## Constitution Check

*GATE: must pass before Phase 0. Re-checked after Phase 1.*

| Principle | Assessment |
|---|---|
| **I. Small Core, Verified Breadth** | **Strengthened.** Two of three ingestion paths are deleted; the configuration surface shrinks; four dependencies fall out. |
| **II. Library-First, Thin CLI** | Unaffected. No CLI-only capability; the CLI reaches the connector through the facade as before. |
| **III. One-Boundary Wrapping** | **Held.** The fork is consumed at the existing boundary; no library type crosses the crate's public surface. The boundary gains a method rather than the crate gaining a second transport. |
| **IV. Exactly-Once Is Sacred** | **Preserved and re-proven.** Staging remains inside the unit (measured: the upload does not commit an open transaction). The sweep still crashes, crashes again during recovery, and requires exact totals. |
| **V. Typed Error Taxonomy** | **Extended.** Local-storage failures gain their own typed classification; per-row upload status is read structurally, never by matching message text. |
| **VI. Self-Contained Code & Comments** | Held. No feature or task identifiers in code or user-facing strings. |
| **VII. Test-and-Verification Gate** | Held. Conformance, parity record, coverage floor and the zero-uncited-disposition close-out all apply unchanged. |
| **VIII. Benchmark Governance** | Held. The supersession measurement is recorded and gates nothing; no bar is proposed. |
| **IX. Contracts and Persisted Formats Frozen** | **Amendment required, explicitly.** Two clauses of the 022 contract change; removing the storage configuration field is a breaking configuration change. Both are recorded in this feature rather than implied. Persisted formats (receipts, state, golden SQL of other destinations) are untouched. |
| **Additional Constraints — dependency resolvability** | ⚠️ **VIOLATED.** See Complexity Tracking. |

### Complexity Tracking

| Violation | Why it is necessary | Simpler alternative rejected because |
|---|---|---|
| *"New dependencies for connector crates MUST be resolvable at plan time with registry facts (versions, feature flags, dependency-tree compatibility with the workspace pins) rather than assumed."* A dependency pinned to a git revision has no published version, no registry feature metadata, and no resolution anyone can reproduce from the registry. | The capability exists nowhere else. The published library cannot perform the upload — 022 verified this in source, not by assumption. The fork is pinned to an exact revision, its features are known from its manifest, and it was exercised live against the real account before this plan was written, which substitutes evidence for registry metadata. | **Wait for upstream**: unbounded latency, and the feature is blocked on someone else's review queue. **Hand-roll the upload**: a second transport beside the library boundary, violating Principle III — a worse trade, since that principle exists to keep upgrades tractable. **Vendor the fork's upload code**: copies 6,380 lines of cloud-storage, signing and crypto code into a connector with no upstream to track. |

**Recorded consequences of the violation**, so the publishing feature inherits
them knowingly rather than discovering them:

1. Three crates — connector, facade, CLI — cannot be published while this holds.
   The bench crate is unaffected (it does not publish).
2. The dangerous form is the one that *looks* safer. A dependency carrying both
   a git source **and** a version key **publishes successfully with the git
   source silently stripped**, shipping a crate that resolves upstream,
   compiles, and fails only at run time. Verified locally against the toolchain.
   Omitting the version key makes packaging refuse instead. This is why the
   check's primary job is catching the version-carrying form.
3. The exits, either of which retires the violation: an accepted upstream
   change, or publishing the fork under its own name and consuming it through a
   package rename — which requires zero source changes, since every import keeps
   resolving.
4. A mechanical check records the arrangement and fails when it changes
   unnoticed, because this hazard is invisible to every other gate and surfaces
   only at publish time.

**Post-Phase-1 re-check**: unchanged. The design added no further violation; the
dependency arrangement remains the single one, with its exits recorded.

## Project Structure

### Documentation (this feature)

```text
specs/023-snowflake-put/
├── spec.md              # what and why (written)
├── plan.md              # this file
├── research.md          # Phase 0 decisions, measured (written)
├── data-model.md        # entities and lifecycle (Phase 1)
├── quickstart.md        # the shrunken configuration (Phase 1)
├── contracts/
│   └── snowflake-put.md # SP1–SP8 (Phase 1)
├── drafts/              # research output, NOT adopted, NOT wired
└── checklists/
    └── requirements.md  # spec quality (written)
```

### Source Code (repository root)

```text
crates/rdlt-connector-snowflake/
├── src/dest/
│   ├── client.rs      # CHANGED — the boundary gains an upload method that
│   │                  #   returns per-row outcomes; this is the seam FR-003
│   │                  #   depends on, and it cannot be a deletion-only file
│   ├── stage.rs       # REPLACED — the storage backend behind the staging seam
│   │                  #   becomes local-build-then-upload; the load-scoped
│   │                  #   ownership discipline and the file-list construction
│   │                  #   survive, the bucket machinery does not
│   ├── config.rs      # SHRUNK — the storage vocabulary is deleted outright
│   ├── encode.rs      # SHRUNK — statement-rendering routines and the measured
│   │                  #   byte budget go; parquet encoding stays
│   ├── session.rs     # SIMPLIFIED — the path-selection branch disappears
│   ├── dialect.rs     # unchanged
│   ├── ddl.rs         # unchanged
│   └── mod.rs         # capabilities unchanged; crash-point registry revised
└── tests/             # suites move onto the single path; two whole files go

crates/rdlt-testkit/src/snowflake.rs   # the bucket credential gate is deleted
Cargo.toml (workspace)                 # the dependency form, and what falls out
benches/parity_specs.yaml              # fixture loses its storage block
crates/rdlt-cli/src/main.rs            # parse pin loses its storage block
tools/                                 # the distribution check lands here
```

**Structure decision**: no new crate and no new module layer. The connector
already has a staging seam with the right shape; this feature swaps what sits
behind it and deletes the alternative. Adding a backend abstraction to hold two
implementations would be building for a second path the feature exists to
remove.

## Phasing

Ordered so that each increment is independently mergeable on a green gate, and
so nothing is deleted before its replacement is proven.

1. **Record repair.** Resolve the contract's uncited claim, and the narrowing of
   the recorded closure routes. Precedes code because it is the evidence the
   rest of the feature is authorised by.
2. **Dependency and its gate.** Adopt the fork; land the distribution check with
   its record. Verify the check catches both failure modes, including the
   version-carrying one and implicit workspace members.
3. **The boundary method.** Extend the single library boundary to issue an
   upload and return per-row outcomes, with the partial-failure case tested
   before anything depends on it.
4. **The internal staging backend**, behind the existing seam, with the three
   measured service facts pinned as live checks.
5. **Cut over.** The single path becomes the only path; suites move onto it.
6. **Delete.** Both superseded paths, their configuration, their tests, their
   dependencies — in one change, with the mechanical residue search.
7. **Auth matrix.** Provision the two outstanding credentials; turn the two
   written-but-skipping live legs green.
8. **Measure and document.** The recorded session against 022's figures; the
   egress prerequisite; parity, contract and README corrections.
9. **Close.** Gate twice clean on the pinned toolchain, secret sweep, close-out
   with zero uncited dispositions.

## Risks carried into implementation

Recorded here because Phase 0 could not close them, and a plan that hides its
open questions is worth less than one that names them.

| risk | disposition |
|---|---|
| Per-part size bound is source-dependent; the upload has a per-file ceiling | Establish the bound across sources during Phase 4. If a limit must be enforced, its refusal must name something the user can change. |
| Reclamation of staged objects may be weaker than the bucket path's, which used modification time | Determine what the listing exposes; if weaker, record the cost rather than claiming parity. |
| Two crash moments may be indistinguishable to every durable observer | If so they earn one point, not two. Any point added must carry an assertion the sweep can actually make. |
| The drafted distribution check does not scan implicit workspace members | Fix before wiring; that gap is the exact silent pass the check exists to prevent. |
| Merge mode is absent from the sweep while shipped in the connector | Decide explicitly against the criterion requiring total cells to fall. |
