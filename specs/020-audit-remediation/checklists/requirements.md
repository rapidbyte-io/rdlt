# Specification Quality Checklist: Audit Remediation

**Purpose**: Validate specification completeness and quality before proceeding to planning
**Created**: 2026-07-26
**Feature**: [spec.md](../spec.md)

Marks: `[x]` passes as written · `[~]` passes with a recorded calibration (see Notes)

## Content Quality

- [~] No implementation details (languages, frameworks, APIs)
- [x] Focused on user value and business needs
- [~] Written for non-technical stakeholders
- [x] All mandatory sections completed

## Requirement Completeness

- [x] No [NEEDS CLARIFICATION] markers remain
- [x] Requirements are testable and unambiguous
- [x] Success criteria are measurable
- [x] Success criteria are technology-agnostic (no implementation details)
- [x] All acceptance scenarios are defined
- [x] Edge cases are identified
- [x] Scope is clearly bounded
- [x] Dependencies and assumptions identified

## Feature Readiness

- [x] All functional requirements have clear acceptance criteria
- [x] User scenarios cover primary flows
- [x] Feature meets measurable outcomes defined in Success Criteria
- [~] No implementation details leak into specification

## Validation Detail

**Counts**: 11 user stories (each an independently mergeable increment),
82 functional requirements in 12 groups, 18 success criteria, 11 edge cases,
12 assumptions, 5 non-goal classes. Zero clarification markers.

**Requirement-to-story coverage** — every FR group is exercised by at least one
story, and every story is constrained by at least one FR group:

| FR group | story |
|---|---|
| Cross-cutting (FR-001–012) | all stories; FR-011 closes at close-out |
| Record and license (FR-013–020) | US1 |
| Value fidelity (FR-021–027) | US2 |
| Schema contracts (FR-028–031) | US5 |
| File family (FR-032–039) | US3 |
| Network robustness (FR-040–046) | US4 |
| Catalog destination (FR-047–051) | US6 |
| Engine hardening (FR-052–060) | US7 |
| Verification gate (FR-061–066) | US8 |
| Publish readiness (FR-067–070) | US9 |
| Recorded deferrals (FR-071–074) | US10 |
| Performance (FR-075–082) | US11 |

**Testability spot-checks** on the requirements most at risk of being
unfalsifiable:

- FR-019 ("statements contradicted by code MUST be corrected") is bounded by an
  enumerable set — the source document lists each contradiction — and SC-003
  makes the residual count checkable at close-out.
- FR-024 and FR-028 admit two outcomes each. Both are stated as testable
  alternatives with the fallback named in Assumptions, not as open questions.
- FR-075/FR-080 make "no measurable improvement" a *satisfying* outcome, so the
  performance group cannot be failed by an honest negative result — the failure
  mode they forbid is closing an item without a number.

## Notes

Two checklist items are calibrated rather than met literally, and one follows
from them. Recorded here rather than silently checked:

1. **"No implementation details" / "No implementation details leak"** — the
   specification names no language, framework, crate, module, function, or file
   path, and no code shape. It does use the *domain* vocabulary the defects live
   in: signed and unsigned integer ranges, declared decimal precision and scale,
   nullability, partitioning, row identity, credential refresh, process exit
   codes, container labels. Removing that vocabulary would make requirements
   like FR-021 and FR-023 unfalsifiable, which the "testable and unambiguous"
   item weighs more heavily. Three genuine leaks found during validation were
   removed rather than justified: a named wire format for request bodies, a
   named header date form, and a named profiling mode — each restated as the
   behaviour it implies.

2. **"Written for non-technical stakeholders"** — a category mismatch for this
   project rather than a defect in the document. rdlt is an embeddable engine;
   its stakeholders are the maintainer and developers embedding it
   (constitution Principles I and II). The specification is written for that
   audience, which is also the register every prior feature specification in
   `specs/` uses. No business-stakeholder audience exists to write for.

3. Item-level `file:line` anchors are deliberately absent. They live in
   `NEXT_STEPS.md`, in the same division of labour `PERF_ANALYSIS.md` held for
   feature 019 — which is also what keeps this document free of the code-shape
   detail item 1 is about.

No item requires a spec update before `/speckit-plan`. One design question is
carried deliberately into planning with both candidates priced and the outcome
constrained by FR-028/FR-031: whether the schema-policy contract is satisfied by
extending enforcement across run boundaries or by narrowing the documented
promise. The audit records why the naive form of the first candidate is wrong, so
this is design work, not a missing requirement.
