# Contract: Workspace Refactoring Program (WR1–WR8)

Clauses this feature must satisfy. Verification cites these IDs from the
close-out matrix; failures in the verifying tests print the clause ID (the
testkit D1–D8/S1–S6 pattern). Per constitution Principle V, these IDs never
appear in user-facing strings.

## WR1 — Behavior preservation

Persisted formats (WAL, StateDoc, receipts, snapshot properties), golden-SQL
pin text, and conformance-clause behavior are byte-for-byte /
behavior-identical before and after every increment. The only permitted
behavior changes are the catalogued defect fixes (B1–B12) and
error-classification corrections (B1/B5/B6/B8/B9/R8), each of which must be
named in the close-out matrix. Crash-point sweeps pass duplicate-free at
every increment that touches commit or replay paths (increments 1, 5, 6, 8,
10).

## WR2 — Citation-free user surface

After increment 2, no user-facing string (error, warning, CLI output, log at
info level or above) contains a contract clause ID, `specs/` path, task ID,
or review-finding number. Verified by a recorded sweep command (quickstart)
that returns zero hits; the sweep runs again at close-out to prove no
regression from later increments.

## WR3 — Single-source collapse

Each named duplication has exactly one implementation with all former sites
consuming it: secret masking 3→1 (R3); commit-unit protocol 2→1 (R2);
live/replay apply 2→1 (R6); file location abstraction 2→1 (R7);
pipeline-spec model 2→1 (B3/increment 12); container-runtime probe 3→1,
postgres test fixture ~6→1, conformance fixture trio 6 files→1 (D1–D4); CI
disk-free step 5→1 (D6); `iai-callgrind` version 3 declarations→1 (D7/D13).
"Exactly one" is verified by grep for the former definitions.

## WR4 — Honest taxonomy

No failure classification depends on substring-matching rendered error text.
Where an upstream exposes nothing structured, the textual assumption is
pinned by a probe test that fails loudly on drift, and the pin is recorded
in the close-out matrix with the upstream reference. Destinations expose the
rate-limited channel (`DestError::RateLimited`); recoverable conditions at
every catalogued site (B1, B8, B9, R8 list) classify as recoverable, and
context-adding layers preserve the incoming classification.

## WR5 — Panic-free library paths

Every catalogued panic site (R9 list) is a typed error or is impossible by
construction (owner types, `Option`-returning fixtures). Fault-injection and
conformance runs over the catalogued sites produce zero panics in library
code. Test-only and truly-unreachable invariants that remain are named in
the close-out matrix with justification.

## WR6 — Regression pin per defect

Each B1–B12 fix carries a regression test demonstrated to fail against the
pre-fix tree (red run captured per research D-14) and passing after. The
pin survives at close-out (no pin deleted or weakened by later increments).

## WR7 — Complete close-out

The close-out matrix covers 100% of catalogue items (B1–B12, R1–R13
including Part 3 sub-items, D1–D15 plus Part 5 low-severity notes) with a
terminal disposition and non-empty evidence per row. Deferrals name their
target window; overtaken items cite the contradicting code. Final coverage
is at or above the recorded pre-feature baseline.

## WR8 — Increment discipline

Work lands as the plan's increments in order; each merge has the full gate
green (nextest, doc-tests, conformance, failpoint sweeps, container cells
skip-not-fail, perf gate). Public API changes are additive or shimmed; the
0.2→0.3 semver window is not opened; deprecated aliases carry
`#[deprecated]` notes naming the replacement. Bench RESULTS.md regeneration
is diff-clean for cells whose measurements this feature does not touch.
