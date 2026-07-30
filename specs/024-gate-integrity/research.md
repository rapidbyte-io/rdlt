# Research: Test-gate integrity (024)

Phase 0. Every finding below was measured against the tree at `b734e6a5`
(branch `024-gate-integrity`, off `main` at `34ccd379`) with
`env -u RUSTUP_TOOLCHAIN`. Where a claim from the spec's driver text turned out
wrong, the correction is stated with the measurement that overturned it.

Toolchain: `cargo-nextest 0.9.135`, rustc pinned 1.96.0 via
`rust-toolchain.toml`.

---

## R0 — A DEAD SELECTOR, FOUND WHILE RESEARCHING THIS FEATURE

The single most important finding, because it is the feature's premise
happening in real time rather than a hypothetical.

`Makefile:107` is the extended property-test run:

```make
PROPTEST_CASES=4096 cargo nextest run -p rdlt-engine -E 'test(shred_property)' --no-tests=pass
```

Measured:

| selector | tests matched |
|---|---|
| `test(shred_property)` | **0** |
| `binary(shred_property)` | 1 (`shred_property shred_invariants_hold`) |

`shred_property` is the name of the test BINARY. `test(...)` filters on test
NAMES, and no test is called `shred_property` — the one test inside that binary
is `shred_invariants_hold`. So the selector matches nothing, `--no-tests=pass`
converts that to exit 0, and **the 4,096-case property run has been reporting
success while running zero tests.**

Two source files cite that suite as the pin for shred's behavioral invariants
(`src/shred/tape.rs:8`, `tests/shred_identity_pin.rs:9`), and
`shred_property.proptest-regressions` sits in the tests directory as evidence it
once found failures. The suite exists and works; only the selector that reaches
it is wrong.

**Correction to an intermediate reading during this research:** a first pass
concluded the FILE was missing. It is not — a truncated directory listing caused
that. The file exists; the selector kind is wrong. Recorded because the wrong
diagnosis (delete-and-recreate) would have destroyed a working suite and its
regression corpus.

Scope note: `TARGET=prop` is reached from `deep` (`Makefile:206`), not from
`check`, so this is the deep tier that has been silently empty, not the routine
gate. That makes it less urgent and no less real.

**Decision**: fix the selector to `binary(shred_property)` and drop the
permissive flag. This is D1's fix producing an immediate, demonstrable win, and
it is the demonstration FR-015 asks for on the very first defect.

---

## R1 — `--no-tests` already defaults to `fail`, so D1 is mostly a deletion

`cargo nextest run --help` (0.9.135):

```text
--no-tests <ACTION>   Behavior if there are no tests to run [default: auto]
  auto: Automatically determine behavior, defaulting to fail
  pass: Silently exit with code 0
  warn: Produce a warning and exit with code 0
  fail: Produce an error message and exit with code 4
```

**Decision**: the fix for a selector whose target always exists is to DELETE
`--no-tests=pass`; the default is already correct. No wrapper, no counting
script, no new tooling.

For a selector whose target may legitimately be absent, `warn` is the honest
middle: exit 0, but say so on stderr. Silence is what `pass` buys and silence is
the defect.

**Three-way disposition rule** (satisfies FR-002):

| situation | disposition |
|---|---|
| target exists unconditionally | remove the flag — default `fail` |
| target legitimately absent in some environment | `--no-tests=warn` + a comment at the site saying WHY it is optional |
| anything else | not allowed; `pass` without a recorded reason is the defect |

**Alternatives considered.** A wrapper script parsing nextest's output for a
minimum count: rejected — it reimplements what the runner already does, adds a
parsing surface that can itself drift, and would have to be maintained against
nextest's output format. `--no-tests=fail` stated explicitly at every site
rather than relying on the default: rejected as noise, since the default IS
fail; the explicit spelling would be nine tokens asserting the status quo. The
default is relied upon and this decision records that reliance, so a future
nextest that changed the default has a documented expectation to violate.

---

## R2 — Which of the nine selectors are genuinely optional

Every selector probed with `cargo nextest list` at this commit:

| # | Makefile | selector | matched |
|---|---|---|---|
| 1 | 97 | `--workspace -E 'binary(/e2e/)'` | 2 |
| 2 | 99 | engine `binary(crash_sweep)` | 5 |
| 3 | 101 | postgres `binary(crash_sweep) or binary(dest_crash_sweep) or binary(cdc_crash_sweep)` | 13 |
| 4 | 102 | duckdb `binary(sweep)` | 1 |
| 5 | 103 | rest `binary(sweep)` | 1 |
| 6 | 104 | file `binary(sweep)` | 2 |
| 7 | 105 | iceberg `binary(sweep)` | 1 |
| 8 | 107 | engine `test(shred_property)` | **0 — see R0** |
| 9 | 205 | iceberg `binary(spark_deep)` | 1 |

**Eight of nine resolve non-empty**, which means eight of the nine flags are
pure risk with no current benefit: they protect against nothing today and hide
a rename tomorrow.

**Decision**: remove the flag from all nine. None of the nine targets is
conditionally COMPILED — every one of these binaries exists unconditionally in
its crate's `tests/` directory. What is conditional is whether the tests inside
them RUN (they self-skip without a container runtime or credentials), and that
is a skip, not an empty selection: nextest still counts them as selected. The
distinction is the crux — `--no-tests` is about SELECTION, not execution, and
these two things were conflated when the flags were added.

Verified for the riskiest case: iceberg's `sweep` and `spark_deep` binaries are
selected (1 each) on this machine, and their tests skip internally when Polaris
is absent. Removing the flag therefore cannot fail a contributor's build for
lacking a container runtime.

---

## R3 — D2: two suites reached by nothing, and why `check` should reach them

`Makefile:96` defines `TARGET=e2e` selecting `binary(/e2e/)`, which matches
exactly `crates/rdlt-connector-file/tests/e2e_copy.rs` and `e2e_duckdb.rs`.
`check` (line 263) runs `lint`, `docs`, `test`, `test TARGET=sweep`,
`bench TARGET=iai`, `bench TARGET=cold` — never `e2e`.

**Decision**: add `$(MAKE) test TARGET=e2e` to `check`, after `test` and before
`test TARGET=sweep`.

Cost measured before committing to it: both binaries' tests self-skip without a
container runtime, so on a runtime-less machine the added cost is a compile.
With a runtime present it is two suites of container-backed copies. The measured
figure is recorded at close-out per SC-010 rather than guessed here.

**Alternatives considered.** Leaving `e2e` out of `check` and documenting it as
a deliberate deep-tier suite: rejected, because nothing else in the repository
treats it that way — `deep` does not invoke it either (`Makefile:199-206`
invokes `prop`, `sweep`, `mutants`, `fuzz`), so it is currently invoked by NO
target reachable from any gate. It is not deliberately deferred; it is orphaned.

---

## R4 — D3: ten registries, six crates, and the count in the spec was wrong

The spec's driver text said "five connectors" and "five of six". Measured, it is
worse: **ten crash-point registries across six connector crates, none of which
can detect a dropped point.**

| crate | registries |
|---|---|
| rdlt-connector-postgres | `dest::FAIL_POINTS`, `CDC_FAIL_POINTS`, `source::FAIL_POINTS` |
| rdlt-connector-file | `dest::FAIL_POINTS`, `S3_FAIL_POINTS`, `source::FAIL_POINTS` |
| rdlt-connector-duckdb | `FAIL_POINTS` |
| rdlt-connector-rest | `FAIL_POINTS` |
| rdlt-connector-iceberg | `ICE_FAIL_POINTS` |
| rdlt-connector-snowflake | `FAIL_POINTS` |

Only `rdlt-engine` scans its own sources. Verified by searching for the scanning
idiom (`"crash_point!("` as a search literal inside test code): one hit,
`crates/rdlt-engine/tests/crash_sweep.rs`. A first pass using `read_dir` as the
signal produced false positives for `file` (3) and `snowflake` (1); those uses
are output verification — reading a destination directory to check what landed —
not source scanning. **Correction recorded because the false positive would have
left four registries unfixed while reporting the work complete.**

Engine's approach (`tests/crash_sweep.rs:196-233`) walks `src/` for
`crash_point!(` call sites, extracts the name literal, sorts, and asserts
set-equality with its registry, with the comment stating exactly why it does not
derive from the registry: *"comparing a const to itself (that would be
circular)"*.

**Decision**: extract that scanner into `rdlt-testkit` as a shared helper —
`crash::assert_registry_matches_sources(manifest_dir, registry)` — and call it
from one test per registry. `rdlt-testkit` is already a dev-dependency of all
eight crates that would use it, so no new dependency edge is created.

**Rationale for sharing rather than copying**: ten copies of a thirty-line
source scanner is ten places for the scanner itself to drift, and a scanner that
drifts fails open — it finds fewer sites and the assertion still passes. One
implementation, used ten times, is the only arrangement where fixing the scanner
fixes every user.

**Alternatives considered.** A build-script or macro that generates the registry
from the call sites, removing the possibility of divergence entirely: rejected
for this feature — it is a genuine improvement but it CHANGES how registries are
declared, which is product-adjacent and outside a gate-hardening scope. Recorded
as a candidate for a later feature. A workspace-level test in one crate scanning
all crates' sources: rejected — it would live in a crate that does not own the
registries, so a new connector would not automatically be covered, and the
failure would name the wrong crate.

**Feature-gate subtlety** (an edge case the spec names): a `crash_point!` inside
a file compiled only under `failpoints` is still TEXT in `src/` and the scanner
reads text, so it is found either way. This makes the scan robust to
configuration but means the scanner must not be fooled by a `crash_point!`
appearing in a comment or a doc example. Engine's implementation extracts the
first string literal after the macro name; the shared helper must document that
limitation and the sources must not contain a commented-out `crash_point!(`.

---

## R5 — D4: which semver baseline, and why CI's is unusable

`cargo semver-checks` appears **zero** times in the Makefile. Its only
invocation is `.github/workflows/ci.yml:100`, against `main`. Measured:
`origin/main` is **73 commits** behind local `main`.

So a CI run compares today's surface against a pre-001..023 surface. A red
result would be a wall of intended, already-shipped changes; a green result
would be impossible. Either way it carries no information about whether THIS
change broke the surface. And `rdlt-core` and `rdlt-connector` are declared
SEMVER-SACRED — this is their only mechanical guard.

**Decision**: add `make semver`, comparing against a baseline sha RECORDED in
the repository, and wire it into `check`.

Baseline choice: `34ccd379` — local `main` at this feature's start, verified
re-derivable: `git merge-base main 024-gate-integrity` →
`34ccd379b3f8c7adcd19ecf827fed3ed133073d9`. Rationale:
the question a gate should answer is "did MY change break the surface", so the
baseline must be the last state where the surface was known-good and agreed. A
moving `main` cannot serve, because a baseline that advances with every merge
silently forgives the break it just accepted; and `origin/main` cannot serve for
the 73-commit reason above.

The sha must be re-derivable rather than asserted: it is `main` at the commit
this feature branched from, recorded in the file that uses it, so a reader can
check `git merge-base main 024-gate-integrity`.

**Alternatives considered.** Comparing against the previous release tag:
rejected — nothing in this workspace has ever been published, so no such tag
carries meaning. Comparing against the working tree's own last commit: rejected
— it would only catch a break introduced in the very last commit, so a break
introduced early and preserved would read as clean forever.

**Recorded consequence**: pinning a baseline means the baseline goes stale by
design. The sha is a fact with an owner, and advancing it is a deliberate act
recorded in the diff — which is the opposite of `origin/main` advancing
invisibly.

---

## R6 — D5: the invisible sweep, and the cheapest honest fix

`crates/rdlt-connector-snowflake/tests/crash_sweep.rs` opens
`#![cfg(feature = "failpoints")]`. The string `snowflake` appears **zero** times
in the Makefile; the sweep target covers engine, postgres, duckdb, rest, file
and iceberg only (lines 99-105). No gate command enables that feature for this
crate, so the file is never compiled by any pipeline.

This already cost a real miss: during feature 023 the file failed to compile
against APIs that feature had deleted, and the standard gate did not notice.

**Decision**: add a type-check-only leg to `lint` —
`cargo clippy -p rdlt-connector-snowflake --all-targets --features failpoints
-- -D warnings`. Compile and lint, do not run.

**Explicitly NOT decided**: adding the sweep itself to `check`. It costs 101.5
minutes and needs live credentials (`specs/023-snowflake-put/close-out.md`
records the run of record at 6,092 s). It stays a separate manual run. The
defect was never that it does not run in the gate; the defect is that nothing
notices when it stops compiling.

**Alternatives considered.** `cargo check` instead of `clippy`: rejected —
`lint` already runs clippy with `-D warnings` for everything else, so a
check-only leg would hold this one crate to a lower standard than the rest.
Adding `--features failpoints` to the existing workspace clippy invocation:
investigated and rejected — it would enable the feature workspace-wide, which
changes what compiles in seven other crates and risks masking or inventing
warnings unrelated to this crate. A single targeted leg is the smaller change.

---

## R7 — D6: the negative filter, and why re-spelling it positively is the WRONG fix

`.config/nextest.toml:9`:

```toml
filter = "package(rdlt-connector-iceberg) and not binary(config_schema)"
```

This bounds the Polaris/JVM live group to 3 threads by EXCLUDING one binary.
Measured, that exclusion is exactly right today: of 11 iceberg test binaries, 10
use the shared live fixture (`mod common`) and only `config_schema` does not.

The driver text proposed re-spelling the filter positively. **Investigated and
rejected**: a positive filter naming 10 binaries fails in the other direction —
add an 11th live binary, forget to add it to the list, and it escapes the
3-thread bound and starts a JVM outside the limit. The negative form has the
opposite failure: rename or fold `config_schema` and a cheap test is dragged
INTO the bound. Neither spelling is safe, because the real problem is that
group membership is implicit either way.

**Decision**: keep the filter as-is and PIN the membership with a test.
`crates/rdlt-connector-iceberg/tests/config_schema.rs` (the one binary outside
the group, which is thematically the right home for a configuration invariant)
asserts that the set of test binaries in this crate matches an expected list,
partitioned into "in the live group" and "outside it". Drift in either direction
then fails, naming the binary and the direction.

**Rationale**: this converts an implicit constraint into an asserted one, which
is the same move R4 makes for crash registries and the same move the whole
feature makes for selectors. Re-spelling would have traded one silent failure
mode for another and felt like progress.

---

## R8 — D7: probe modes, building on machinery that already exists

`rdlt-testkit` already has the negative half of this:

- `containers.rs:46` — `RDLT_TESTKIT_FORCE_NO_CONTAINERS`, forcing
  `runtime_available()` to report false.
- `snowflake.rs:18` — `RDLT_TESTKIT_FORCE_NO_SNOWFLAKE`, same for credentials.

So the codebase can already force a probe to say "absent". What is missing is
the ability to demand the opposite.

**Decision**: add the symmetric assertive flags —
`RDLT_TESTKIT_REQUIRE_CONTAINERS` and `RDLT_TESTKIT_REQUIRE_SNOWFLAKE` — which
make the probe PANIC with a message naming the missing resource rather than
returning unavailable. Naming mirrors the existing pair, so the two halves read
as a set.

Precedence must be defined because both can be set: REQUIRE wins and the
conflict is itself an error, because a run that both forces absence and demands
presence is a mistake in the invocation, and silently honouring one would hide
it.

**Decision on the count baseline**: a committed file recording, per test binary,
the number of tests run and skipped, plus a `make` verb that produces the
current figures in the same shape for comparison. A count that differs is a
report, not automatically a failure — adding a test legitimately changes it —
but the difference must be visible in review, which a committed file achieves
and a runtime assertion does not.

**Alternatives considered.** Asserting counts inside the gate and failing on any
difference: rejected — it would fail every commit that adds a test, training
maintainers to update the baseline reflexively, which is exactly how a pinned
number stops being read. Deriving expected counts programmatically: rejected as
circular in the same way R4 rejects registry-against-itself.

---

## R9 — D8: the recorded practice that exists in no executable form

`specs/023-snowflake-put/close-out.md` records the coverage figure (87.22%) as
having been measured with the Snowflake sweep excluded:

```text
-E 'not (package(rdlt-connector-snowflake) and binary(crash_sweep))'
```

That string appears in no Makefile, script or config (verified). The `coverage`
target (`Makefile:260-261`) is `cargo llvm-cov nextest --features failpoints`
with no exclusion, so running it today would include the 101.5-minute sweep and
produce a figure not comparable with the recorded one.

**Decision**: codify the exclusion in the `coverage` target with the reason and
the measured cost at the site.

---

## R10 — The audit for a tenth defect (FR-014)

The nine flags and the negative filter were found by reading. FR-014 requires
establishing there is no further check that can pass while verifying nothing.
Scope of the audit, to be executed as a task and recorded item-by-item:

- every `cargo nextest` invocation in the Makefile — selector, and whether an
  empty match is possible;
- every `.config/nextest.toml` override — what it includes and excludes, and
  whether membership is asserted anywhere;
- every environment-conditional gate step (`RDLT_DEEP`, `RDLT_HEAVY`,
  `PROPTEST_CASES`, the container and credential probes) — what happens when the
  condition is unmet, and whether that outcome is announced;
- every `|| true`, `-` prefix, `2>/dev/null` or piped invocation in the Makefile
  that could swallow a non-zero exit. **Pre-audited: zero occurrences** —
  `grep -cE '\|\| true|2>/dev/null|^\t-' Makefile` returns 0, so this class is
  already clean and the audit records it as SOUND rather than silent. Worth
  stating: an earlier session in this project lost time to exactly this failure
  (a filtered command whose non-zero exit the filter could not see), so the
  absence is a property to protect, not an assumption;
- the `docs`, `bench` and `coverage` targets, which were not part of the
  original eight and have not been examined.

Each item ends with a disposition INCLUDING items found sound — a list of only
the defects cannot show that the search was exhaustive.

---

## Open questions carried into implementation

None blocking. Two judgement calls are deliberately deferred to the increment
that meets them, both recorded so they are decided rather than drifted:

1. **The exact shape of the count-baseline file.** Whether it is one file for
   the whole gate or one per target depends on how noisy the diffs turn out to
   be; decide after seeing the first real one.
2. **Whether the shared source scanner should reject a commented-out
   `crash_point!`.** Rejecting it is stricter and could annoy; accepting it
   risks a phantom registry entry. Decide when writing the helper, and state the
   choice in its doc comment.
