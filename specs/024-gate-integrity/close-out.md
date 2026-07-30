# Close-out: Test-gate integrity (024)

Branch `024-gate-integrity`, off `main` at `34ccd379`.

Eight measured defects let the gate report green while verifying less than it
appeared to. This document records what each became, with the evidence that
settled it. A disposition without a citation is a defect in this document.

## Contract matrix (GI1–GI8)

| clause | status | evidence |
|---|---|---|
| **GI1** empty selection fails | MET | Nine permissive flags removed; the runner's default is already `fail`, so this is deletion rather than construction and the reliance is recorded. The selection/execution distinction is stated at the site, because conflating them is what produced the defect. Demonstrated: the same target exits **4** with `error: no tests to run` where it previously exited **0** silently — `evidence/demo-us1.md`. |
| **GI2** every suite reachable or exempt by name | MET | All **107** test binaries enumerated mechanically from the filesystem against what each gate target selects (`evidence/suite-reachability.md`). Zero unreachable-and-unexplained. `TARGET=e2e` — reachable from no target, not even `deep` — is now in `check`. One exemption, with its reason and measured cost: the 101.5-minute credential-gated sweep. |
| **GI3** registry verified against sources, never itself | MET | Ten registries across six crates, plus the engine, all verify against their own sources through ONE shared scanner. Three detection cases demonstrated, plus the scanner's own vacuity guard — `evidence/demo-us3.md`. |
| **GI4** a file the gate does not compile does not exist | MET | A targeted `--features failpoints` clippy leg compiles the one test file no gate command built. Enabled for that crate ALONE: workspace-wide would change what compiles in seven others. |
| **GI5** a skip is distinguishable from a pass | MET | Symmetric `REQUIRE_*` overrides beside the existing `FORCE_NO_*` pair; both-set is an error. Eight cases pinned in `gating_pin.rs`, all passing. Counts recorded per binary with the reading-by-direction guide in the testkit README. |
| **GI6** group constraint asserts its own membership | MET | The iceberg live-group membership is asserted by a test rather than implied by a filter. The filter's negative spelling is deliberately KEPT — a positive list of ten fails the other way when an eleventh live binary is added and nobody updates it. |
| **GI7** recorded practice is executable | MET | `make semver` against a recorded, re-derivable baseline sha; the coverage exclusion that the recorded figure was actually measured with is codified with its reason and cost. |
| **GI8** strictly harder, and each fix proves detection | MET | Five demonstrations, one per story, each an OBSERVED failure-then-recovery with output captured. The FR-014 audit records a disposition for every check examined — twelve found SOUND, nine fixed, no ninth defect (`evidence/gate-audit.md`). |

## Story matrix

| story | status | independent test |
|---|---|---|
| US1 — an empty selection fails | DELIVERED | A renamed binary, and the previously-dead selector, both exit 4 where they exited 0. |
| US2 — suites that exist are invoked | DELIVERED, with a weaker guarantee than the others — see D-4 | 107 binaries enumerated; `TARGET=e2e` in `check`; zero unexplained. |
| US3 — a dropped crash point is detected | DELIVERED | Per-registry: remove a site and it fails; add one and it fails; the pure both-sides deletion is caught by the committed count. |
| US4 — a disarmed probe is visible | DELIVERED | Demand-and-fail fails naming the resource; forced-absence still skips; both-set errors. |
| US5 — recorded practice is executable | DELIVERED | Each named command runs and reproduces its recorded figure. |

## The gate, before and after

`GATE_EXIT=0` on a clean run with all five stories in. Full table in
`evidence/gate-cost.md`.

| | before | after |
|---|---|---|
| workspace tests | 948 (2 skipped) | **961** (2 skipped — the same two named instruments) |
| `TARGET=e2e` | **not run by anything** | 2 passed, in `check` |
| `TARGET=prop` | **0 tests, 0.000 s** | 1 test, **38.026 s** |
| `make semver` | **did not exist** | clean, both sacred crates |
| snowflake failpoints | **compiled by nothing** | clean |
| perf / cold start | 0 regressed / 23.8 ms | 0 regressed / 23.9 ms |

## Deviations and corrections

Numbered from D-1: this feature starts its own ledger rather than continuing 023's.

### D-1 — The fix was cheaper than the spec assumed, and the spec is left as written

`--no-tests` already defaults to `fail`. Eight of the nine flags were therefore
protecting against nothing: their selectors already matched. The dominant fix is
deletion, not construction, and no wrapper or counting script was needed.

The flags existed because two things were conflated. `--no-tests` governs
**selection**; what these suites do is **skip during execution**. A skipped test is
still selected, and the runner counts it — so an empty selection never meant
"resources absent" and could only ever mean a binary was renamed or misspelled.

### D-2 — A dead selector, found while researching the feature that fixes it

`test(shred_property)` filters on test names; `shred_property` is the BINARY and
its one test is `shred_invariants_hold`. The 4,096-case property run had been
reporting success while executing zero cases. The suite and its regression corpus
were intact — only the selector was wrong.

Recorded because it needed no manufactured regression to demonstrate: the gate's
own history supplied one.

### D-3 — Three scanner designs, each overturned by measurement

**Set equality** does not survive this workspace: three postgres points are armed
indirectly, and a set-equality scanner reports them missing. The plausible fix for
that report is to SHRINK the registry — removing points from the sweep while every
assertion passes. Two directions instead.

**Counting occurrences** was the first spelling of direction two, and it assumed
the declaration lives inside the scanned tree. Six connectors satisfy that by
coincidence of where they declare things; the ENGINE does not. Migrating the engine
— the one crate that already had a working check — is what exposed it. Declaration
blocks are now located by shape and excluded.

**One assertion per registry** fails where a crate declares several over one source
tree: file and postgres have three each, so checking one reports its siblings as
undeclared. Per crate, against the union.

### D-4 — US2's guarantee is structural, and weaker than the others

GI2 is satisfied by an enumeration derived from the filesystem, so a new binary
appears in it automatically. But nothing FAILS when a binary is unreachable — it
depends on someone reading the enumeration when the set of suites changes.

The stronger form needs the gate to know its own target graph, which is a larger
change than this feature's scope. Recorded as the weaker guarantee it is rather
than counted alongside the four that fail on their own.

### D-5 — Two self-inflicted instances of the defect class

Recorded because they are evidence the class is easy to fall into, not just
evidence about me.

The iceberg membership pin's first version asserted `!contains("mod common")` and
failed on **its own source**, because the search term appears in the check. A test
that reads its own file has to treat its own text as data.

And an exit code was read through a pipe, reporting **0** for a command that had
exited **4** — the precise trap this feature exists to remove, in its own
verification. Every subsequent exit code was captured from the command itself.

### D-6 — The `unsafe_code` ban improved the probe tests

`gating_pin.rs` first set process environment variables and would not compile:
this workspace denies `unsafe_code` and `set_var` is unsafe in this edition.

Separating each probe's DECISION from where its answers come from is strictly
better than what it replaced. Demand-and-fail can now be exercised **on a machine
that HAS the resource** — the machine a maintainer would be on. An env-mutating
test could only reach that path by removing the resource.

### D-7 — Three gate runs were contaminated and discarded

`make check` spawns sub-makes that re-read the Makefile, so editing during a run
measures a mixture of old and new. It happened three times before the workflow was
disciplined to "all edits, then one untouched gate". No contaminated run is
reported anywhere; the recorded baseline comes from two runs that completed on an
untouched tree, and the recorded result from one run nothing touched.

Also recorded: waiting on a PID captured by `pgrep -f 'make check'` is unreliable,
because the pattern matches the waiting shell itself. The final runs wait on a
completion marker written into the log.

### D-8 — US2 through US5 were gated together, not one increment at a time

The plan said every increment ends with a green gate. US1 got its own (green,
recorded). US2, US3, US4 and US5 were then gated **together** in one run.

Two reasons, and the second is the honest one. Each story's own verification ran
in isolation as it was built — the registry assertions, `gating_pin.rs`, the
membership pin, `make semver` and `TARGET=e2e` were each run and green before the
combined gate. And a full gate on this host costs container and live-credential
suites plus a reclaim ritual, and I had already discarded three contaminated runs
(D-7), so a fourth and fifth full cycle bought confidence I had by other means.

What this costs: if the combined run had failed, attribution would have been
across four stories instead of one. It did not fail, so the cost was not paid —
but it was a risk taken, not an absence of one, and recording it as "gate green
per increment" would have been false.

## Unperformed verifications

| verification | why |
|---|---|
| CI | Blocked outside this repository (organizational billing). Every CI-only verification stays recorded as unperformed, never claimed green. This feature makes the LOCAL gate trustworthy; it does not repair CI. |
| A test that fails on an unreachable binary | See D-4: needs the gate to model its own target graph. Recorded as scope, not as done. |
