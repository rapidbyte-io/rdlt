# Contract: Gate Integrity Rules (GI1–GI8)

Numbered clauses for feature 024. Each is verifiable, and each is either met or
carries a cited disposition at close-out — no clause may be silently satisfied.

The interface this feature exposes is not an API. It is the **gate's own
verdict**: the set of guarantees a maintainer or agent is entitled to assume when
`make check` exits zero. That verdict is consumed by every close-out document in
this repository, which is what makes it a contract rather than a convention.

| # | Rule |
|---|---|
| **GI1** | **An empty test selection is a failure, not a pass.** Every test selector in the gate fails when it matches nothing, UNLESS the site carries a written reason why matching nothing is legitimate — in which case it warns audibly rather than passing silently. Zero selectors may pass silently on an empty match. The distinction between SELECTION (which tests the runner picks) and EXECUTION (whether they then skip) is stated at any site where it could be confused, because conflating them is what produced the defect. |
| **GI2** | **Every test suite in the repository is reachable or exempt, by name.** For each test binary, either a gate path invokes it, or an exemption names it and states its reason — and where the reason is cost, the MEASURED cost. A suite that is neither invoked nor exempt is a defect, not an oversight. The enumeration is exhaustive and mechanically re-derivable, not a hand-kept list. |
| **GI3** | **A crash-point registry is verified against its sources, never against itself.** For every registry in the workspace, the set of instrumented sites found by reading that crate's sources equals the registry, count-exact. The expected set is NEVER derived from the registry being checked. Removing a site from both the source and the registry fails; adding a site to the source alone fails. The scanner is ONE shared implementation, because a scanner that drifts fails open — it finds fewer sites and the assertion still passes. |
| **GI4** | **A file the gate does not compile does not exist as far as the gate is concerned.** Every test file in the workspace is type-checked by the routine gate, including files compiled only under a non-default configuration. Running a suite and compiling it are separate obligations: a suite may be too expensive to run in the routine gate and still must compile in it. |
| **GI5** | **A skip is distinguishable from a pass.** Resource-gated suites keep skip-not-fail as their DEFAULT and keep announcing the skip. In addition, an opt-in mode makes each resource probe fail — naming the missing resource — so a maintainer can demand that a leg actually ran. Per-binary counts of tests run and skipped are recorded, so a suite that silently stops running appears as a difference rather than as green. A count difference is a report requiring a reason, not an automatic failure: a number that fails every commit that adds a test is a number nobody reads. |
| **GI6** | **An execution-group constraint asserts its own membership.** Wherever the runner's configuration bounds a set of tests, the membership of that set is pinned by a test, so drift in EITHER direction fails — a cheap test dragged into an expensive bound, or an expensive test escaping it. Re-spelling a constraint from exclusion to inclusion is not sufficient: both spellings fail silently in opposite directions, and the defect is that membership is implicit. |
| **GI7** | **A recorded practice exists in executable form.** Any gate procedure or figure recorded in a project document is reproducible by running a named command. A practice that lives only as prose is a claim nobody can check, and a recorded measurement whose method is not runnable is not a measurement. Public-surface comparison in particular runs against a baseline whose result is INTERPRETABLE, with the baseline recorded such that a reader can re-derive it. |
| **GI8** | **The gate is strictly harder to pass, and each fix proves it detects.** No change in this feature may make the gate easier to pass in any respect. For every defect fixed, detection is DEMONSTRATED: the gate observed failing on a deliberately introduced regression that previously passed silently, then green once reverted, with the observed output recorded. A gate-hardening feature verified only by "the gate still passes" is the vacuous verification it exists to eliminate. An audit establishes that no further check can pass while verifying nothing, recording a disposition for every check examined — including those found SOUND, because a list of only the defects cannot show the search was exhaustive. |

## Verification map

| clause | how it is verified |
|---|---|
| GI1 | Each of the nine sites has a stated disposition; a renamed binary is demonstrated to fail the gate (GI8). The R0 selector — which matches nothing TODAY — is the first demonstration. |
| GI2 | Mechanical enumeration of every `tests/*.rs` binary against the set the gate invokes; the difference is empty or every member of it is a named exemption. |
| GI3 | Per registry: delete a site from source and registry together, observe failure; add a site to source alone, observe failure; restore, observe pass. Ten registries, ten demonstrations. |
| GI4 | The previously-uncompiled file is deliberately broken, the gate observed catching it, then reverted. |
| GI5 | Default mode: resource absent → announced skip (unchanged, re-verified). Opt-in mode: resource absent → failure naming it. Forced-absent probe with resources present → count difference against the baseline. |
| GI6 | Add a test binary without listing it; observe the membership pin fail. Remove one; observe it fail. |
| GI7 | Run each named command and reproduce each recorded figure, including the coverage figure whose exclusion was previously prose-only. |
| GI8 | Every demonstration above, with output captured in close-out; plus the audit table with a disposition per check examined. |

## What this contract deliberately does NOT require

Stated so a later reader does not mistake an omission for an oversight:

- **It does not require the expensive credential-gated sweep to run in the
  routine gate.** GI4 requires it to COMPILE there. Running it costs 101.5
  minutes and needs live credentials; it remains a separate manual run. The
  defect was never that it does not run — it is that nothing noticed when it
  stopped compiling.
- **It does not require resource-gated suites to fail by default.** Principle VII
  mandates skip-not-fail so a contributor without a container runtime can still
  run the gate. GI5 preserves that and adds the opposite mode alongside it.
- **It does not require the count baseline to be a hard assertion.** GI5 says so
  explicitly, with the reason: a check that fails on every legitimate test
  addition trains maintainers to update it without reading it, which is how a
  pinned number stops being a pin.
- **It does not require continuous integration to be repaired.** That is blocked
  outside this repository, and every CI-only verification stays recorded as
  unperformed rather than claimed green.
- **It does not change how crash-point registries are DECLARED.** Generating a
  registry from its call sites would make divergence impossible and is the better
  end state; it changes a product-adjacent declaration pattern and is recorded as
  a candidate for a later feature (research R4), not smuggled in here.

## Amendment note

This contract governs the verification apparatus that every prior feature's
contract relied on implicitly. It does not amend any of them: no earlier clause
becomes false. What changes is that claims of the form "gate twice clean" become
checkable, where previously they rested on the gate checking what it appeared to.
