# Specification Quality Checklist: Test-gate integrity

**Purpose**: Validate specification completeness and quality before proceeding to planning
**Created**: 2026-07-30
**Feature**: [spec.md](../spec.md)

## Content Quality

- [X] No implementation details (languages, frameworks, APIs)
- [X] Focused on user value and business needs
- [X] Written for non-technical stakeholders
- [X] All mandatory sections completed

## Requirement Completeness

- [X] No [NEEDS CLARIFICATION] markers remain
- [X] Requirements are testable and unambiguous
- [X] Success criteria are measurable
- [X] Success criteria are technology-agnostic (no implementation details)
- [X] All acceptance scenarios are defined
- [X] Edge cases are identified
- [X] Scope is clearly bounded
- [X] Dependencies and assumptions identified

## Feature Readiness

- [X] All functional requirements have clear acceptance criteria
- [X] User scenarios cover primary flows
- [X] Feature meets measurable outcomes defined in Success Criteria
- [X] No implementation details leak into specification

## Notes

### Validation iterations

**Iteration 1 — three failures found and fixed.**

1. *No implementation details* — FAILED. The draft named specific tools and
   paths throughout: a nextest flag spelling, `Makefile` line numbers,
   `.config/nextest.toml`, `cargo semver-checks`, `containers.rs:56`. Rewritten
   to describe the defect classes in behavioral terms — "a selector permitted to
   match nothing", "a registry compared against itself", "an environment probe".
   The concrete locations belong in `plan.md` and `research.md`, and are already
   recorded in the feature description this spec was written from, so nothing is
   lost by their removal here.
2. *Success criteria technology-agnostic* — FAILED for the same reason.
   SC-001…SC-010 rewritten as counts and demonstrations rather than tool
   invocations: "the count of unexplained permissive selectors falls from nine to
   zero" replaces naming the flag.
3. *Written for non-technical stakeholders* — PARTIAL. This feature's subject IS
   the verification apparatus, so a fully non-technical rendering would be
   dishonest. Resolved by making every story state the CONSEQUENCE in plain
   language first ("the maintainer believes a suite ran that did not exist") and
   keeping mechanism at the level of "a check that reports success while
   verifying nothing". A reader who has never opened this repo can follow why
   each story matters.

**Iteration 2 — all items pass.** No further changes.

### Deliberate characteristics worth noting for planning

- **Nine, five, two, one** are load-bearing counts, not illustrations: nine
  permissive selectors, five crash-sweep registries that cannot detect a drop
  (of six that arm points), two uninvoked end-to-end suites, one file no gate
  compiles. Each becomes a completeness check at close-out — a disposition per
  item, not a summary.
- **FR-015 is the criterion that makes this feature honest.** A gate-hardening
  feature verified only by "the gate still passes" would be exactly the vacuous
  verification it exists to eliminate. Detection must be demonstrated by
  observed failure-then-recovery, with output recorded.
- **FR-013 is a one-way constraint.** No change may make the gate easier to
  pass. It is stated as a requirement rather than an assumption because it is
  the boundary a reviewer should check every diff against.
- **US4 and US5 are separable.** If the feature had to ship early, US1–US3
  deliver the detection value; US4 adds visibility on resource-gated suites and
  US5 closes a documentation-versus-reality gap.
- **No `[NEEDS CLARIFICATION]` markers were needed.** Every defect was measured
  in the tree before the spec was written, and the owner's constraints (routine
  gate stays local, the hour-plus sweep stays out, skip-not-fail stays the
  default, the version window stays closed) were already recorded.
