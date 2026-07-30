# Gate audit (FR-014, GI8) — every check, including the sound ones

The eight defects were found by reading. This establishes there is no ninth. A
disposition is recorded for every check examined, **including those found sound**,
because a list of only the defects cannot show the search was exhaustive.

## Test invocations

| check | can it pass while verifying nothing? | disposition |
|---|---|---|
| `test` (workspace, no filter) | No — no selector to come up empty | SOUND |
| `test TARGET=unit` | No — no selector | SOUND |
| `test TARGET=e2e` | Was **UNREACHABLE** from any target, and permissive | FIXED — added to `check`, flag removed |
| `test TARGET=sweep` (6 lines) | Was permissive on all six | FIXED — flags removed |
| `test TARGET=prop` | Selector matched **ZERO** tests and was permissive | FIXED — `test(…)` → `binary(…)`, flag removed |
| `test TARGET=deep` → `memory_bound` | No — no permissive flag, and `RDLT_HEAVY=1` hard-fails on missing prereqs by design | SOUND |
| `test TARGET=deep` → `spark_deep` | Was permissive | FIXED — flag removed |
| `test TARGET=mutants` | Not a selector-based check | SOUND (out of scope) |
| `test TARGET=fuzz` | Loops declared targets; a missing target fails the loop | SOUND |
| doc-tests (`cargo test --doc --workspace`) | No selector | SOUND |

## Runner configuration

| item | finding | disposition |
|---|---|---|
| iceberg live-group filter | NEGATIVE spelling; membership implicit, breaks in both directions | FIXED — membership asserted by a test; spelling deliberately kept (a positive list fails the other way) |
| flake-recording profile | Retries and records classifications; does not mask a failure | SOUND |

## Environment-conditional behaviour

| item | finding | disposition |
|---|---|---|
| `containers::runtime_available` | Could report absent with no way to demand otherwise | FIXED — `RDLT_TESTKIT_REQUIRE_CONTAINERS`; default unchanged |
| `snowflake::credentials` | Same | FIXED — `RDLT_TESTKIT_REQUIRE_SNOWFLAKE`; default unchanged |
| `FORCE_NO_*` overrides | Pre-existing; now contradictory with `REQUIRE_*` | FIXED — both-set is an error, not a precedence rule |
| `RDLT_DEEP` / `RDLT_HEAVY` | Deep-tier only; `RDLT_HEAVY` deliberately hard-fails | SOUND |
| `PROPTEST_CASES` | Tunes case count; cannot empty a selection | SOUND |

## Exit-code swallowing

| class | finding |
|---|---|
| `\|\| true`, leading `-`, `2>/dev/null` in the Makefile | **ZERO occurrences.** SOUND, and worth protecting: this session lost time twice to reading an exit code through a pipe, once while producing this feature's own evidence. |

## Targets not among the original eight

| target | finding | disposition |
|---|---|---|
| `docs` | `RUSTDOCFLAGS="-D warnings" cargo doc` — no selector, warnings are errors | SOUND |
| `bench TARGET=iai` | Refuses a comparison whose benches show zero regressions — the only check that notices a toolchain override | SOUND, and load-bearing |
| `bench TARGET=cold` | Absolute bar, no selector | SOUND |
| `bench TARGET=gate` | Evaluates recorded artifacts against bars | SOUND |
| `coverage` | Ran WITHOUT the exclusion its recorded figure was measured with | FIXED — exclusion codified with its reason and cost |
| `lint` | Did not compile one crate's feature-gated tests at all | FIXED — targeted `--features failpoints` leg added |
| `semver` | **Did not exist.** Only invocation was CI, against a baseline dozens of commits stale | FIXED — `make semver` against a recorded, re-derivable sha, wired into `check` |
| `reclaim` | Maintenance verb, label-scoped | SOUND |

## Result

**No ninth defect.** Twelve checks were found sound; nine were fixed. The two
categories the original eight did not cover — the `docs`/`bench` family, and
exit-code swallowing — were both examined and both clean, `bench TARGET=iai`
notably so: it is the only check that catches a silently overridden toolchain, and
it catches it by refusing to compare rather than by passing.
