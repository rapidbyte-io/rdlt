# rdlt-testkit

Memory connectors, conformance suites, and a crash-injection harness.
Public and shipped: connector authors certify against the suites here, and
"certified" means exactly "passes conformance".

It depends on the SPI only. If something in here needs engine internals,
the SPI is wrong — raise it rather than reaching around. Connector-agnostic
by the same rule: system-specific fixtures (a postgres container, a
credential convention) live with their connectors and route through this
crate's gate posture; this crate never names a connector.

## Conformance suites

`verify_source` / `verify_destination` drive a `Source` or `Destination`
through the behaviours the SPI contract requires but the type system cannot
express, and `assert_conformant` turns the verdict into a panic listing
every violated clause by id. The asserted clauses are exactly: **S1**
(the resume law), **S2** (checkpoint coverage), **S4** (closed channel =
cancellation) for sources; **D1** (staging invisibility), **D2** (atomic
state), **D3** (idempotent commits), **D4** (staging teardown), **D5**
(idempotent DDL), **D6** (fresh pipelines have no state), **D8** (merge
upserts, when declared) for destinations. A failure reads as
"violates D3", not "test failed".

`verify_source` also reports honest skips: a stream that declares no
`cursor_field` and never checkpoints is a snapshot stream by its own
declaration, so S2 is skipped with the reason rather than failed — or
vacuously passed. Suites that expect every clause exercised fold skips
back into failures with `expecting_no_skips()`; a stream that quietly
stops declaring its cursor then stays loud.

For a source that pushes Arrow batches, the S1 row comparison degrades to
row counts — payload content is opaque to the harness.

## Memory connectors

`MemorySource` / `MemoryDestination` — deterministic, dependency-free
implementations for testing pipelines without a database, certified by this
crate's own suites. `MemoryDestination` also exposes what it received
(committed rows, schemas, state, opens), so a test can assert on rows
rather than on side effects.

## Crash injection

`CrashDestination` wraps any destination and fails at a chosen `FaultPoint`:

| Fault point | Models |
|---|---|
| `BeforeWrite(n)` | the WAL recorded the batch, the destination never got it — recovery must replay |
| `BeforeCommit(n)` | batches staged, not published — recovery must re-drive the commit |
| `AfterCommit(n)` | published, but the receipt was lost — recovery must hit idempotence, not double-publish |

## The gate: skip, never fail

This crate is connector-agnostic and feature-less: it carries the ONE
runtime probe (`gate::runtime_available`, std-only, always compiled) and
the reclaim label, while the system-specific fixtures live with their
connectors and route through the probe. A fixture's `start()` returns
`Option` — without a container runtime it prints a visible `SKIP` line and
returns `None`, and the caller returns early. A missing runtime NEVER
panics, because a panic there is indistinguishable from a real failure and
trains people to ignore red.

Every container started here carries the label `rdlt-test=1` so leaked
containers and their volumes are reclaimable in one scoped command. A
suite killed mid-run never reaches `Drop`, and orphaned anonymous volumes
fill disks.

## Knowing that resource-gated suites actually ran

Suites needing a container runtime or live credentials **skip rather than
fail** when those are absent — the one posture, with no environment
override. That default is deliberate and required (a contributor without
them must still be able to run the gate), and it has a cost worth naming:
a suite that wrongly skips is indistinguishable from one that passed.

The net is **count discipline**, not a knob. The runner prints run/skip
counts for every binary; every gate of record states the expected ones
("N/N, 2 named instrument skips, 0 skipped with containers"); a leg that
quietly stopped running surfaces as a moved number against that record.

### Reading a count difference

`make counts` reports tests run per binary. Interpret a change by direction:

| observation | reading |
|---|---|
| run-count up | a test was added — expected |
| run-count down, skips up by the same amount | a suite lost its resource, or a probe regressed. Investigate. |
| run-count down, skips unchanged | tests disappeared. This is the case the numbers exist for. |

It reports rather than fails, deliberately: a check that failed on every
legitimate test addition would train everyone to update it without reading
it.

## Verifying crash-point registries

`assert_registry_matches_sources(src_dir, &[registry, …])` checks that a
crate's declared crash points and the sites armed in its own sources
agree. Every crate that arms points calls it, from its existing sweep
binary.

It reads the SOURCES rather than comparing the registry to itself, because
`fired == registry` stays true when a point is deleted from the code and
the list together — the sweep matrix shrinks and the assertion still
passes.

Two things about it are worth knowing before adding a crash point:

- **Two arming spellings are recognised**, `crash_point!("…")` and
  `crash_at("…")`. A third requires updating `ARMING_PATTERNS`; the helper
  fails loudly when it finds no sites at all, so a missing spelling
  surfaces rather than agreeing.
- **A name supplied by a variable** is armed indirectly, and its literal
  lives at the constructor that supplies it. That is why the check has two
  directions — everything armed is declared, and every declared name
  appears somewhere that is not a declaration — rather than set equality,
  which reports indirect points as missing and invites shrinking the
  registry to "fix" it.
