# Detection demonstration — US1 (FR-015, GI1)

Observed, not predicted. Each exit code was captured from the command itself, not
from a pipeline — a filtered command hides the failure it is being asked about,
which is the same class of defect this feature exists to remove and which caught
me once while producing this evidence.

## The dead selector, before and after

Three runs of the same target, differing only in the selector and the flag.

| case | selector | flag | result | **exit** |
|---|---|---|---|---|
| **A** | `test(shred_property)` | none | `0 tests run: 0 passed, 140 skipped` + `error: no tests to run` | **4 — FAILS** |
| **B** | `test(shred_property)` | `--no-tests=pass` | `0 tests run: 0 passed, 140 skipped`, silent | **0 — PASSES** |
| **C** | `binary(shred_property)` | none | `1 test run: 1 passed, 0 skipped` | **0 — PASSES** |

**B is what the gate did before this feature**: exit 0, no error, no warning,
zero tests executed, and a Makefile line claiming a 4,096-case property run.

**A is what the same mistake does now**: exit 4, `error: no tests to run`. The
regression that previously passed silently now fails.

**C is the fix**: the selector reaches the binary, and the suite that had been
dead runs its one property test.

Note the `140 skipped` in A and B — the runner reports the tests it deselected.
That number was visible the whole time and meant nothing was selected, which is
worth stating: the information was never hidden by the runner, only by the flag.

## The wall clock says it too

`make test TARGET=prop`, end to end:

| | wall clock | tests |
|---|---|---|
| before (dead selector + flag) | **0.000 s** | 0 |
| after (fixed selector) | **38.026 s** | 1 (at `PROPTEST_CASES=4096`) |

Thirty-eight seconds of property-based testing that the project believed it was
already doing. A zero-second "pass" is the signature of this whole defect class,
and it was sitting in plain sight in every deep-tier run.

## Why this is the strongest demonstration available for US1

It does not require introducing a regression, because **the regression was
already there**. A and B are the pre-feature gate reproduced exactly; the only
change is the flag whose removal is US1's content. A demonstration that had to
manufacture a fault would prove less than one that reproduces a fault the project
was actually shipping.

## The second half: a renamed binary

The generic case — a binary renamed out from under a selector that other tasks in
this feature depend on. Verified by the same method: with the flags removed, a
selector naming a binary that does not exist exits 4 rather than 0. Case A above
IS that case, since `test(shred_property)` names something that exists nowhere.
