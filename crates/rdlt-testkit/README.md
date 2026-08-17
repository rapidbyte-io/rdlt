# rdlt-testkit

The connector certification kit: what connector and embedder authors
certify and demonstrate with. "Certified" means exactly "passes
conformance", and the suites that decide it live here.

The crate depends on the SPI only. If something in here needed engine
internals, the SPI would be missing a seam — raise that rather than
reaching around it. Engine-internal test doubles (crash injectors, the
memory source's failure knobs) live with the engine, not here.
Connector-agnostic by the same rule: a system-specific fixture (a
postgres container, a credential convention) lives with its connector
and routes through this crate's gate; this crate never names a
connector.

## Module map

| module | what it carries |
|---|---|
| `conformance` | the suites, their `Verdict`, and `assert_conformant` |
| `conformance::source` | `verify` for a `Source`; asserts S1, S2, S4 |
| `conformance::destination` | `verify` for a `Destination` plus the `TableProbe` the author implements; asserts D1–D6, D8 |
| `memory` | the in-memory `Source`/`Destination` pair (`Stream`, `Batch`, `Row`) |
| `fixtures` | the canonical one-column schema, its Arrow batch, and the commit envelope |
| `gate` | `runtime_available` and `RECLAIM_LABEL` — the skip-not-fail posture |
| `spawn` | `built_connector_bin`, locating a connector crate's built binary |
| `scanner` | `assert_registry_matches_sources`, the crash-point registry scanner |

Module paths are canonical; there are no crate-root re-exports.

## Certifying a connector

Certification runs anywhere — no network, no containers, no credentials.
Build the connector, hand it to the suite, and assert the verdict:

```rust
use rdlt_connector::source::StreamSpec;
use rdlt_testkit::conformance::{self, assert_conformant};
use rdlt_testkit::memory;
use serde_json::json;

#[tokio::test]
async fn certified() {
    let source = memory::Source::new(vec![memory::Stream::new(
        StreamSpec::new("events"),
        vec![
            memory::Batch::new(vec![json!({"id": 1}), json!({"id": 2})]).with_checkpoint(1),
            memory::Batch::new(vec![json!({"id": 3})]).with_checkpoint(2),
            memory::Batch::new(vec![json!({"id": 4})]).with_checkpoint(3),
        ],
    )]);
    assert_conformant(conformance::source::verify(&source).await.expecting_no_skips());
}
```

`conformance::source::verify` and `conformance::destination::verify`
drive a `Source` or `Destination` through the behaviours the SPI
contract requires but the type system cannot express, and
`assert_conformant` turns the verdict into a panic listing every violated
clause by id — a failure reads as "violates D3", not "test failed". The
asserted clauses are exactly:

- sources: **S1** (the resume law), **S2** (checkpoint coverage),
  **S4** (a closed channel is cancellation);
- destinations: **D1** (staging invisibility), **D2** (atomic state),
  **D3** (idempotent commits), **D4** (staging teardown), **D5**
  (idempotent DDL), **D6** (fresh pipelines have no state), **D8**
  (merge upserts, when the destination declares merge).

Destinations certify with a `TableProbe` the author implements so the
suite can read back what a warehouse query would see; a probe that
cannot read its store returns an error, never `Ok(0)`, because a zero it
cannot vouch for would certify the invisibility clauses vacuously.

The source suite also reports honest skips: a stream that declares no
`cursor_field` and never checkpoints is a snapshot stream by its own
declaration, so S2 is skipped with the reason rather than failed — or
vacuously passed. A suite that expects every clause exercised folds
skips back into failures with `expecting_no_skips()`, so a stream that
quietly stops declaring its cursor stays loud; `tolerating_skips()` keeps
them separate. For a source that pushes Arrow batches, the S1 row
comparison degrades to row counts — payload content is opaque to the
harness.

## The memory pair

`memory::Source` and `memory::Destination` are the "runs anywhere"
connectors an embedder wires a pipeline through, certified by this
crate's own suites. The source is scripted streams of JSON rows with
checkpoints; the destination is a warehouse under one mutex with
read-back oracles (`committed_rows`, `schema`, `snapshot`), so a test
asserts on rows rather than on side effects.

```rust,ignore
let pipeline = Pipeline::builder("demo")
    .source(memory::Source::default())
    .destination(memory::Destination::new())
    .write_mode(WriteMode::Append)
    .build()?;
pipeline.run().await?;
```

## The gate: skip, never fail

`gate::runtime_available` is the ONE container-runtime probe for the
workspace, std-only and always compiled. Skip-not-fail exists so a
contributor without containers or credentials can still run the gate.
A fixture's `start()` returns `Option`: without a runtime it prints a
visible `SKIP` line and returns `None`, and the caller returns early. A
missing runtime never panics,
because a panic there is indistinguishable from a real failure and trains
people to ignore red. There is no environment override — one posture.

Every container started in this workspace carries `gate::RECLAIM_LABEL`
(`rdlt-test=1`) so `make reclaim` removes leaked containers and their
volumes in one scoped command. A suite killed mid-run never reaches
`Drop`, and orphaned anonymous volumes fill disks; a label rather than a
name pattern, because volumes do not inherit their container's name.

Skipping has a cost worth naming: a suite that wrongly skips is
indistinguishable from one that passed. The net is count discipline, not
a knob — the runner prints run/skip counts for every binary, every gate
of record states the expected ones, and a leg that quietly stopped
running surfaces as a moved number. `make counts` reports tests run per
binary rather than failing on a moved number, deliberately: a check that
fails on every legitimate test addition trains people to update it
without reading it. Read a change by direction:

| observation | reading |
|---|---|
| run-count up | a test was added — expected |
| run-count down, skips up by the same amount | a suite lost its resource, or a probe regressed. Investigate. |
| run-count down, skips unchanged | tests disappeared. This is the case the numbers exist for. |

## The spawn scaffold

`spawn::built_connector_bin(env!("CARGO_MANIFEST_DIR"), "crate-name")`
locates a connector crate's built binary for spawn suites, in one place.
It honours an absolute `CARGO_TARGET_DIR` and refuses a relative one,
builds only when `RDLT_BUILD_CONNECTOR_BINS` is set (the Makefile's
spawn-bins lines set it), and otherwise fails a missing binary loudly
with instructions rather than building behind the runner's back or
quietly skipping — either would be a silent pass wearing a new hat.

## The crash-point registry scanner

`scanner::assert_registry_matches_sources(src_dir, &[registry, …])`
checks that a crate's declared crash points and the sites armed in its
own sources agree. Every crate that arms points calls it from its sweep
binary. It reads the sources rather than comparing the registry to
itself, because a constant compared against itself always agrees — a
declared name that arms nothing is caught only by looking outside the
declaration. It cannot see a point deleted from the code and the
registry together; that silent shrink is caught separately, by
`scanner::armed_crash_points` against a committed per-crate site count.

Do not simplify it. Three plausible designs each fail open: set equality
reports indirectly-armed points (a name supplied by a variable, its
literal at the constructor) as missing and invites shrinking the
registry to "fix" it; occurrence counting assumes the declaration lives
in the scanned tree, which is true only by coincidence, so declaration
blocks are located by shape and excluded; one assertion per registry
misses a crate that keeps several lists over one tree, so the check is
one per crate against their union. Two arming spellings are recognised,
`crash_point!("…")` and `crash_at("…")`; a third must be added to
`ARMING_PATTERNS`, and the vacuity guard is what makes a missing
spelling fail rather than agree.
