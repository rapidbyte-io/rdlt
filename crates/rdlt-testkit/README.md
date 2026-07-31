# rdlt-testkit

Memory connectors, conformance suites, and a crash-injection harness.
Public and shipped: connector authors certify against the suites here, and
"certified" means exactly "passes conformance".

It depends on the SPI only. If something in here needs engine internals, the
SPI is wrong — raise it rather than reaching around.

## Conformance suites

`assert_conformant` drives a `Source` or `Destination` through the behaviours
the SPI contract requires but the type system cannot express: cursor resume
without re-emission, staged-then-atomic publication, idempotence per commit,
schema evolution, and the discard policies.

## Memory connectors

`MemorySource` / `MemoryDestination` — deterministic, dependency-free
implementations for testing pipelines without a database. `MemoryDestination`
also exposes what it received, so a test can assert on rows rather than on
side effects.

## Crash injection

`CrashDestination` wraps any destination and fails at a chosen `FaultPoint`:

| Fault point | Models |
|---|---|
| `BeforeWrite(n)` | the WAL recorded the batch, the destination never got it — recovery must replay |
| `BeforeCommit(n)` | batches staged, not published — recovery must re-drive the commit |
| `AfterCommit(n)` | published, but the receipt was lost — recovery must hit idempotence, not double-publish |

## The gate: skip, never fail

This crate is connector-agnostic and feature-less: it carries the ONE
runtime probe (`gate::runtime_available`, std-only, always compiled) and the
reclaim label, while the
system-specific fixtures live with their connectors and route through the
probe (`rdlt_connector_postgres::fixtures::{PgFixture, CdcPgFixture}` behind
that crate's `fixtures` feature). A fixture's `start()` returns `Option` —
without a container runtime it prints a visible `SKIP` line and returns
`None`, and the caller returns early. A missing runtime NEVER panics, because
a panic there is indistinguishable from a real failure and trains people to
ignore red.

Set `RDLT_TESTKIT_FORCE_NO_CONTAINERS=1` to force the skip posture on a
machine that *does* have a runtime — that is how the skip path itself stays
tested.

Every container started here carries the label `rdlt-test=1` so leaked
containers and their volumes are reclaimable in one scoped command. A suite
killed mid-run never reaches `Drop`, and orphaned anonymous volumes fill
disks.

## Demanding that resource-gated suites actually run

Suites needing a container runtime or live credentials **skip rather than fail**
when those are absent. That default is deliberate and required — a contributor
without them must still be able to run the gate — but it has a cost: a suite that
wrongly skips is indistinguishable from one that passed.

Four environment overrides, in two symmetric pairs:

| variable | effect |
|---|---|
| `RDLT_TESTKIT_FORCE_NO_CONTAINERS` | report the runtime absent even when present — makes the skip path verifiable |
| `RDLT_TESTKIT_REQUIRE_CONTAINERS` | absence becomes a FAILURE naming what is missing |
| `RDLT_TESTKIT_FORCE_NO_SNOWFLAKE` | report Snowflake credentials absent even when present (gate lives in the snowflake connector's tests; the env names are kept verbatim) |
| `RDLT_TESTKIT_REQUIRE_SNOWFLAKE` | absence becomes a FAILURE naming them |

Setting a `FORCE_NO_*` and its matching `REQUIRE_*` together is an **error**, not
a precedence rule: a run that both demands a resource and pretends there is none
asked two contradictory questions, and answering one silently would hide the
mistake.

```sh
# On a machine where the resources ARE present, and you need to know the legs ran:
RDLT_TESTKIT_REQUIRE_CONTAINERS=1 RDLT_TESTKIT_REQUIRE_SNOWFLAKE=1 \
  env -u RUSTUP_TOOLCHAIN make check
```

### Reading a count difference

`make counts` reports tests run per binary. Interpret a change by direction:

| observation | reading |
|---|---|
| run-count up | a test was added — expected |
| run-count down, skips up by the same amount | a suite lost its resource, or a probe regressed. Investigate. |
| run-count down, skips unchanged | tests disappeared. This is the case the numbers exist for. |

It reports rather than fails, deliberately: a check that failed on every
legitimate test addition would train everyone to update it without reading it.

## Verifying crash-point registries

`assert_registry_is_armed(src_dir, &[registry, …])` checks that a crate's declared
crash points and the sites armed in its own sources agree. Every crate that arms
points calls it, from its existing sweep binary.

It reads the SOURCES rather than comparing the registry to itself, because
`fired == registry` stays true when a point is deleted from the code and the list
together — the sweep matrix shrinks and the assertion still passes.

Two things about it are worth knowing before adding a crash point:

- **Two arming spellings are recognised**, `crash_point!("…")` and
  `crash_at("…")`. A third requires updating `ARMING_PATTERNS`; the helper fails
  loudly when it finds no sites at all, so a missing spelling surfaces rather
  than agreeing.
- **A name supplied by a variable** is armed indirectly, and its literal lives at
  the constructor that supplies it. That is why the check has two directions —
  everything armed is declared, and every declared name appears somewhere that is
  not a declaration — rather than set equality, which reports indirect points as
  missing and invites shrinking the registry to "fix" it.
