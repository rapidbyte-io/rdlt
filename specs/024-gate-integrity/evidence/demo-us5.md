# Detection demonstration — US5 (FR-015, GI4, GI6, GI7)

## GI7 — `make semver` exists and runs

```text
Checked  196 checks: 196 pass, 57 skip   (rdlt-core)
Summary  no semver update required
Checked  196 checks: 196 pass, 57 skip   (rdlt-connector)
Summary  no semver update required
exit 0
```

Against the recorded baseline `34ccd379`, re-derivable with
`git merge-base main 024-gate-integrity`. Before this story the string
`semver-checks` appeared nowhere in the Makefile; the only invocation was in CI,
against a branch trailing local main by dozens of commits — a comparison whose
result mixes intended history with a genuine break and can be read neither way.

## GI6 — the live-group membership pin detects both directions

`crates/rdlt-connector-iceberg/tests/config_schema.rs::the_live_group_membership_is_pinned`
asserts the exact set of test binaries, partitioned into the ten that boot the JVM
fixture and the one that does not.

```text
1 test run: 1 passed
```

Add or rename a binary and it fails naming what changed and which side to put it
on. That covers both failure directions the negative filter has: dragging the
cheap test INTO the three-thread bound, and releasing a JVM cell OUT of it.

**A self-referential trap, recorded because it cost a cycle.** The pin also checks
that `config_schema.rs` does not declare the shared live fixture. The first version
used `contains("mod common")` — and failed immediately, because the file contains
that string **in the check itself**. It now matches at the start of a line. A test
that reads its own source has to be written as though its own text is data.

## GI4 — a file no gate compiled is now compiled

`lint` gained a targeted leg:

```make
cargo clippy -p rdlt-connector-snowflake --all-targets --features failpoints -- -D warnings
```

Before it, `crates/rdlt-connector-snowflake/tests/crash_sweep.rs` — which is
`#![cfg(feature = "failpoints")]` — was compiled by no gate command at all. During
an earlier feature it broke against APIs that feature had deleted, and the gate
reported green throughout. That is the miss this closes.

The feature is enabled for **that crate alone**. Adding it to the workspace-wide
clippy invocation was rejected: it changes what compiles in seven other crates and
risks inventing or masking warnings unrelated to the one being fixed.

Running the sweep in the gate remains out of scope — 101.5 minutes against live
credentials. Compiling it and running it are separate obligations, and only the
first was missing.
