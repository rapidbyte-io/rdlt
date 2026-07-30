# The nine permissive selectors, each with a disposition (FR-002)

Every selector that carried `--no-tests=pass` before this feature, its measured
match count at `6ee0e796`, and what it is now.

`--no-tests` defaults to `fail` in the runner (nextest 0.9.135), so making a
selector strict means DELETING the flag. Nothing was replaced with an explicit
`--no-tests=fail`: that would be nine tokens asserting the default.

| # | line | selector | matched before | disposition |
|---|---|---|---|---|
| 1 | 97 | `--workspace -E 'binary(/e2e/)'` | 2 | **STRICT** — flag removed |
| 2 | 99 | engine `binary(crash_sweep)` | 5 | **STRICT** — flag removed |
| 3 | 101 | postgres `binary(crash_sweep) or binary(dest_crash_sweep) or binary(cdc_crash_sweep)` | 13 | **STRICT** — flag removed |
| 4 | 102 | duckdb `binary(sweep)` | 1 | **STRICT** — flag removed |
| 5 | 103 | rest `binary(sweep)` | 1 | **STRICT** — flag removed |
| 6 | 104 | file `binary(sweep)` | 2 | **STRICT** — flag removed |
| 7 | 105 | iceberg `binary(sweep)` | 1 | **STRICT** — flag removed |
| 8 | 107 | engine `test(shred_property)` | **0** | **STRICT, and the SELECTOR ITSELF FIXED** — see below |
| 9 | 205 | iceberg `binary(spark_deep)` | 1 | **STRICT** — flag removed |

**Nine of nine are strict. None needed `warn`**, and that outcome is the finding:
eight of the nine flags were protecting against nothing, because their selectors
already matched. They carried no benefit and one cost — hiding a rename.

## Why none of them needed to stay permissive

The flags were added conflating two different things. `--no-tests` governs which
tests the runner **selects**. What these suites actually do is **skip during
execution** when a container runtime or credentials are absent — and a skipped
test is still a selected one, which the runner counts. So an empty selection
never meant "no resources"; it could only ever mean a binary was renamed,
deleted, or misspelled in the Makefile.

Verified for the riskiest case before removing anything: iceberg's `sweep` and
`spark_deep` each select 1 test on this machine and skip internally without
Polaris. Removing the flag cannot fail a contributor's build for lacking a
container runtime.

## Selector 8 was not merely permissive — it was dead

`test(shred_property)` filters on test NAMES. `shred_property` is the name of the
test **binary**; the single test inside it is `shred_invariants_hold`. So the
selector matched nothing, the flag turned that into exit 0, and the 4,096-case
property run had been reporting success while executing zero cases.

Two source comments cite that suite as the pin for shred's behavioral invariants
(`crates/rdlt-engine/src/shred/tape.rs`, `crates/rdlt-engine/tests/shred_identity_pin.rs`),
and `shred_property.proptest-regressions` sits beside it as evidence it once
found real failures. The suite works. Only the selector reaching it was wrong,
and it is now `binary(shred_property)`.
