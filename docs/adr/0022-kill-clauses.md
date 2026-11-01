# ADR 0022: The kill clauses

Status: accepted, 2026-09-28.

## Context

The `K` clauses certify that a connector killed at random points still lets an engine converge
exactly once. A kill clause needs an engine to load through the connector, and a host that can
kill what it placed.

## Decision

- **`rdlt-certify` links an engine.** It depends on `rdlt-connector`, `rdlt-host`, `rdlt-wire`,
  `rdlt-engine` and `rdlt-connector-reference`, in every build: the kill clauses load through an
  engine, into a reference memory destination or from a reference generator source, so the library
  and the binary certify the same clauses.
- **A host kills what it placed.** `rdlt_host::Kills` is a shared handle. `Local::kills` sends
  `SIGKILL` to the process group of each connector the provider spawned, which the host owns
  (ADR 0050); `Remote::kills` and `Connect::kills` cut each connection they opened, which is all a
  host can do to a connector it did not start. The supervisor then starts or reaches the connector
  again for the next call, as it does for one that crashed. `Connect` is a provider of connectors
  reached through a function that opens a stream, placed as `Placement::Connected`: certification
  places a served connector with it.
- **`K-SOURCE`**: the target source is loaded into a memory destination twice, placed and
  supervised as an engine's placement is: once never killed, and once killed at scheduled
  points. The second load's tables must equal the first's, row for row, without the engine's
  `_rdlt_` columns. Each stream is read in the first of incremental, full and change reads it
  serves, and appended, replaced or merged as that read requires; a source with no stream read in
  any mode is inapplicable (ADR 0050).
- **`K-DESTINATION`**: 2,000 generated rows are written into the target destination, in the
  first of append, merge and replace it declares, while it is killed at scheduled points; the
  table read back through the probe must hold each row exactly once. It is unobserved when
  nothing reads back what the destination published or it keeps history alone, and inapplicable
  when it declares no write mode (ADR 0050).
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
- **A clause passes only on a kill that landed** (ADR 0050): for a spawned connector, `SIGKILL`
  to its process group, which the host then sees end the connection unless a process outside
  the group holds it open (`docs/certify/clauses.md`); for one reached at an
  endpoint or served in process, the host's cut of its connection, which lands by itself. The
  pass notes which (`killed`, `cut`). When no kill landed (the load ended first, as a source
  smaller than about one credit window does), the clause is unobserved, with the seed. So is
  `K-SOURCE` when no read of the source started after a kill: one that lands once the source was
  read to its end, as on a busy machine whose commits come late, leaves nothing to resume. A clause
  takes at most 300 s, or what `--kill-timeout` (`Target::kill_timeout`) gives it: a connector
  slower than about two seconds a commit needs more.
- **`S-ARROW-JSON` is not checked.** The engine infers the types of JSON pushes where Arrow
  pushes carry their own, so the same data rendered both ways publishes the same values only once
  a schema pins both, and certification has no way to ask a source for both renderings.

## Consequences

- The placement matrix is the engine's integration suite in process, spawned and remote
  (`rdlt-engine/tests/it/placement.rs`); the `P` and `K` clauses run against the reference
  connectors served, spawned and listening (`rdlt-certify/tests/it`); the host's process and
  protocol tests are `rdlt-host/tests/it/process.rs` and `protocol.rs`.
- A connector author learns from the binary whether the connector survives being killed. A
  source must hold enough data for a kill to land mid-read, or the clause is unobserved, and must
  read the same data both times, as a fixture does: `K-SOURCE` compares two loads.
- A killed spawned connector writes no coverage profile, so its killed runs go unmeasured; the
  processes that start again are measured.
