# ADR 0022: The kill clauses

Status: accepted, 2026-09-28; amended 2026-10-01 by ADR 0050: a kill reaches a spawned
connector's process group, and a clause passes only on a kill seen to have ended a connection.

## Context

ADR 0021 split M4g, leaving M4h the `K` clauses (§20.8: "connector killed at random points;
engine converges exactly-once") and `S-ARROW-JSON`. A kill clause needs an engine to load
through the connector, and a host that can kill what it placed. The owner ruled that
`rdlt-certify` depends on the engine only behind an optional `kill` feature, on in its binary.

## Decision

- **The crate map (§4.2, §4.3) is amended.** `rdlt-certify` depends on `rdlt-connector`,
  `rdlt-host` and `rdlt-wire`, and, with its `kill` feature, on `rdlt-engine` and
  `rdlt-connector-reference` too. The binary requires the feature. A library build without it
  still lists the `K` clauses in its reports and registry, each skipped as unbuilt, so a report
  has the same clauses whatever the build.
- **A host kills what it placed.** `rdlt_host::Kills` is a shared handle. `Local::kills`
  SIGKILLs each process the provider spawned; `Remote::kills` and the new `Connect::kills` cut
  each connection they opened, which is all a host can do to a connector it did not start. The
  supervisor then starts or reaches the connector again for the next call, as it does for one
  that crashed. `Connect` is a provider of connectors reached through a function that opens a
  stream, placed as `Placement::Connected`: certification places a served connector with it.
- **`K-SOURCE`**: the target source is loaded into a memory destination twice, placed and
  supervised as an engine's placement is: once never killed, and once killed at scheduled
  points. The second load's tables must equal the first's, row for row, without the engine's
  `_rdlt_` columns. A stream is read incrementally and appended when it can be, else read in
  full and replaced; a source with neither is skipped.
- **`K-DESTINATION`**: 2,000 generated rows are appended into the target destination while it is
  killed at scheduled points; the table read back through the probe must hold each row exactly
  once. It is skipped when nothing reads back what the destination published, and when the
  destination does not append.
- **Where the kills land.** A seed draws three points, counted in commits across the loads: at
  the first write after the second or third commit, before the third to fifth commit, and, for
  a destination, after the second to fourth commit, reporting its answer lost as a kill between
  a commit and its answer does. Each point follows a commit that published rows (a load's first
  commit may publish none), so a load killed there resumes from what it recorded, and a
  connector that breaks exactly-once only across a commit's rows is caught at every schedule:
  the tests run all eighteen against a destination that records state before rows, and all six
  of a source's against one that loses what it resumes.
- **The seed** is the time, mixed as SplitMix64 mixes, so a clock that ticks in microseconds, as
  macOS's does, still draws every point; or one chosen (`Target::kill_seed`, `--kill-seed`). A
  failure reports it, so it can be replayed.
- **The loads are shaped so the kills land in flight**: batches of 8 rows, a commit every 16
  rows, 1 MiB of engine memory, one event of partition buffer, one write in flight per lane, and
  a read's credit floor of 64 KiB, so a source runs little ahead of the commits: 64 KiB, or two
  of its frames where they are larger. A load retries 20 times, quickly, and up to three loads
  run until one succeeds.
- **A clause passes only on evidence.** When no kill interrupted a load (the load ended first,
  as a source smaller than about its credit floor does), the clause is skipped with the seed,
  not passed. A clause takes at most 300 s, or what `--kill-timeout` (`Target::kill_timeout`)
  gives it: a connector slower than about two seconds a commit needs more.
- **`S-ARROW-JSON` waits for M8.** The engine infers the types of JSON pushes where Arrow pushes
  carry their own, so the same data rendered both ways publishes the same values only once a
  schema pins both; certification has no way to ask a source for both renderings. The Python
  SDK (M8), which renders records as JSON, settles the clause with its first JSON source.

## Consequences

- M4's exit criterion is met:
  - the placement matrix is the engine's integration suite in process, spawned and remote
    (`rdlt-engine/tests/it/placement.rs`);
  - the `P` and `K` clauses are green against the reference connectors served, spawned and
    listening (`rdlt-certify/tests/it`);
  - P1–P4 and L3 are pinned by `rdlt-host/tests/it/process.rs` and `protocol.rs`.
- A connector author learns from the binary whether the connector survives being killed. A
  source must hold enough data for a kill to land mid-read, or the clause is skipped, and must
  read the same data both times, as a fixture does: `K-SOURCE` compares two loads.
- A killed spawned connector writes no coverage profile, so its killed runs go unmeasured; the
  processes that start again are measured.
