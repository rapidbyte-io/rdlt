<!--
Sync Impact Report
==================
Version change: 1.0.0 → 1.1.0 (2026-07-24, feature 018)
Modified principles: VIII Benchmark Governance — materially reworded: the
  gated/scoreboard cell taxonomy is removed from the constitutional
  vocabulary; enforcement is expressed as cells (measured, reported) and
  bars (enforced). The mechanism is preserved and STRENGTHENED: a bar now
  explicitly requires a recorded measurement-session floor in addition to
  the governance (policy-log) entry.
Added/Removed sections: none.
Templates requiring updates: none (plan-time gates read this file live).
Follow-up TODOs: none.

Prior report (1.0.0 initial ratification):
Version change: (template, unversioned) → 1.0.0
Modified principles: n/a (initial ratification — template placeholders replaced)
Added sections:
  - Core Principles I–IX (Small Core; Library-First; One-Boundary Wrapping;
    Exactly-Once; Typed Error Taxonomy; Self-Contained Code & Comments;
    Test-and-Verification Gate; Benchmark Governance; Frozen Contracts &
    Persisted Formats)
  - Additional Constraints
  - Development Workflow & Quality Gates
  - Governance
Removed sections: none (template slots all filled)
Templates requiring updates:
  - .specify/templates/plan-template.md — ✅ compatible (Constitution Check
    gates are derived from this file at plan time; no static edit needed)
  - .specify/templates/spec-template.md — ✅ compatible (no constitution-
    specific sections)
  - .specify/templates/tasks-template.md — ✅ compatible (no constitution-
    specific sections)
Follow-up TODOs: none
-->

# rdlt Constitution

## Core Principles

### I. Small Core, Verified Breadth

rdlt is a SMALL embeddable Rust ELT engine plus a FEW most-used, deeply
verified connectors. Connector breadth, orchestration, scheduling, and product
features belong to products built on top (rapidbyte) — never to rdlt. Any
proposal that grows rdlt's surface MUST argue explicitly why the capability
belongs in the engine rather than in a product above it; absent that argument,
the default answer is no.

**Rationale**: the project's value is a core that one person can audit
end-to-end. Breadth dilutes verification depth, and verification depth is the
product.

### II. Library-First, Thin CLI

All capability lives in the library crates behind the `rdlt` facade. The CLI
adds zero engine capability: it MAY only parse input, construct pipelines via
the public library API, and render events. Anything the CLI can do MUST be
reachable through the public library API by an embedding application.

**Rationale**: rdlt is embeddable first; a CLI-only capability would be a
capability the primary audience cannot use.

### III. One-Boundary Wrapping

Each connector wraps its underlying third-party library at exactly one module
boundary. Third-party types (database drivers, catalog clients, format
internals) MUST NOT cross the connector crate's public surface. Error
translation into the shared typed taxonomy happens at that boundary and
nowhere else.

**Rationale**: one boundary makes dependency upgrades and swaps tractable and
keeps the public API stable across upstream churn.

### IV. Exactly-Once Is Sacred

Commit identity, replay detection, WAL recovery, and state-document updates
are correctness-critical. Any change touching them MUST ship crash-point
sweeps (live fail-point tests at the write, commit, and receipt-visible
points) with duplicate-free verification. Unsupported capability MUST degrade
to a TYPED error that is recorded — never a silent fallback or silent
narrowing of semantics.

**Rationale**: exactly-once is the engine's central promise; a silent
degradation is indistinguishable from data corruption to the user.

### V. Typed Error Taxonomy

Sources and destinations MUST classify failures as Transient, RateLimited, or
Fatal through typed constructors. Classifying by substring-matching a rendered
error string is forbidden: match structured codes or context values, or — when
the upstream exposes nothing structured — pin the assumption with a test that
fails loudly when the upstream wording drifts. Contract clause IDs and spec
citations MUST NOT appear in user-facing error or warning strings.

**Rationale**: retry behavior is driven by classification; a misclassification
silently burns retry budget or aborts recoverable runs. Users cannot resolve
internal citation IDs.

### VI. Self-Contained Code & Comments

Every comment MUST stand alone: state the rule or invariant itself, optionally
followed by a short tag. Meaning MUST NOT live only in `specs/` documents,
review-finding numbers, or task IDs — published crates cannot resolve them.
Relocation breadcrumbs and historical narratives are forbidden in code. The
workspace denies `unsafe_code`; performance candidates requiring unsafe are
rejected even with valid ceiling evidence (sole recorded exception: the CLI
mallopt FFI).

**Rationale**: the code is read on docs.rs and in editors where `specs/` does
not exist; comments that depend on it rot silently.

### VII. Test-and-Verification Gate (NON-NEGOTIABLE)

Tests run via `cargo nextest run` (doc-tests via `cargo test --doc`).
Container-backed integration tests MUST skip-not-fail when the container
runtime is absent, with images and environment verified at feature start.
Every connector MUST be certified against the testkit conformance clauses.
Every feature closes with: a verification matrix with zero uncited claims,
parity records against the reference implementation with deferrals named, and
coverage of at least 80% measured baseline-first.

**Rationale**: "verified connectors" is the product claim (Principle I); the
gate is what makes the claim true rather than aspirational.

### VIII. Benchmark Governance

Benchmarks are declarative cells — end-to-end pipeline comparisons,
measured and reported. Enforcement exists only as bars: a bar references
exactly one existing cell, lives in `bars.toml`, and is enforced by the
bench gate. No bar exists without recorded measurement evidence — a bar
is set below the floor of a recorded session and cites a governance
(policy-log) entry. Performance claims MUST be backed by harness
evidence, not ad-hoc timing.

**Rationale**: ungoverned gates rot into flaky CI; ungoverned claims rot
into marketing; importance taxonomies rot into labels that substitute
for evidence.

### IX. Contracts and Persisted Formats Are Frozen

Each feature ships a contract with numbered clauses. Persisted formats (WAL,
state documents, receipts, wire schemas) change only with explicit versioning
and migration notes. Golden SQL / golden output pins guard shared cores.
Semver-breaking changes are batched and recorded ahead of the break; config
enums are `#[non_exhaustive]`.

**Rationale**: embedded engines are upgraded in place against data written by
older versions; an unversioned format change is data loss deferred.

## Additional Constraints

- Language and toolchain: Rust workspace; `unsafe_code` denied workspace-wide
  (Principle VI).
- Test tooling: `cargo nextest` is the canonical runner; bare `cargo test` is
  used only for doc-tests.
- User-facing strings (errors, warnings, CLI output) MUST be self-contained
  and free of internal citation IDs (Principle V).
- New dependencies for connector crates MUST be resolvable at plan time with
  registry facts (versions, feature flags, dependency-tree compatibility with
  the workspace pins) rather than assumed.

## Development Workflow & Quality Gates

- Features follow the spec-kit flow: spec → plan → tasks → implement, with the
  plan documenting design decisions and contracts before implementation.
- `/speckit-plan` MUST include a Constitution Check pass against these
  principles; violations either change the design or are justified in the
  plan's Complexity Tracking table.
- Review findings are fixed with verification evidence, not acknowledged in
  comments (Principle VI forbids finding-ID breadcrumbs in code).
- A feature is complete only when the full gate of Principle VII is green.

## Governance

This constitution supersedes ad-hoc practice. Where a `specs/` document and
this constitution conflict, the constitution wins and the spec is amended.

- **Amendments**: require a version bump with rationale recorded in the Sync
  Impact Report, and propagation to dependent templates in the same change.
- **Versioning policy**: semantic — MAJOR for principle removals or
  redefinitions, MINOR for new/materially expanded principles or sections,
  PATCH for clarifications and wording.
- **Compliance review**: every plan passes the Constitution Check; every
  feature close-out re-verifies the Principle VII gate; complexity deviations
  MUST be justified in the plan's Complexity Tracking table or removed.

**Version**: 1.1.0 | **Ratified**: 2026-07-24 | **Last Amended**: 2026-07-24
