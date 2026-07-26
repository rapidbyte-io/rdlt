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

## Container fixtures: skip, never fail

`PgFixture` / `CdcPgFixture` return `Option` — without a container runtime
they print a visible `SKIP` line and return `None`, and the caller returns
early. A missing runtime NEVER panics, because a panic there is
indistinguishable from a real failure and trains people to ignore red.

Set `RDLT_TESTKIT_FORCE_NO_CONTAINERS=1` to force the skip posture on a
machine that *does* have a runtime — that is how the skip path itself stays
tested.

Every container started here carries the label `rdlt-test=1` so leaked
containers and their volumes are reclaimable in one scoped command. A suite
killed mid-run never reaches `Drop`, and orphaned anonymous volumes fill
disks.
