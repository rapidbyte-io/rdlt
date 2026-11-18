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
  any mode is inapplicable (ADR 0050). A source that cannot read again a stream the clause loads
  (`StreamSpec::with_replayable(false)`) leaves the clause unobserved, before any load: each load
  starts from no cursor, into a store of its own, and such a source refuses that read once
  anything told it a cursor, as `S-ACK` and the first load do (ADR 0034). One load alone would not
  certify it either: with nothing to compare against, a resume that lost rows passes.
- **`K-DESTINATION`**: 2,000 generated rows are written into the target destination, in the
  first of append, merge and replace it declares, while it is killed at scheduled points; the
  table read back through the probe must hold each row exactly once. It is unobserved when
  nothing reads back what the destination published or it keeps history alone, and inapplicable
  when it declares no write mode (ADR 0050).
- **Where the kills land.** A seed draws three points, counted in commits across the loads as
  each starts: at the first write once the second or third commit has started, before the third
  to fifth commit, and, for a destination, after the first commit from the second to fourth on
  that lands, reporting its answer lost as a kill between a commit and its answer does. So a lost
  answer drawn on a commit an earlier point failed is lost from the next that lands: with the
  points (2, 3, 2), where the kill at a write fails the second commit and the kill before the
  third fails it, the answer is lost from the fourth. Each point follows a commit that published
  rows (a load's first commit may publish none), so a load killed there resumes from what it
  recorded, and a connector that breaks exactly-once only across a commit's rows is caught at
  every schedule: the tests run all eighteen against a destination that records state before
  rows, and all six of a source's against one that loses what it resumes.
- **The seed** is the time, mixed as SplitMix64 mixes, so a clock that ticks in microseconds, as
  macOS's does, still draws every point; or one chosen (`Target::kill_seed`, `--kill-seed`). A
  failure reports it, so it can be replayed.
- **The loads are shaped so the kills land in flight**: batches of 8 rows, a commit every 16
  rows, 40 MiB of engine memory, one event of partition buffer, one write in flight per writer,
  and a read's credit floor of 64 KiB, so a placed source runs little ahead of the engine: 64 KiB,
  or two of its frames where they are larger. A load retries 20 times, quickly, and up to three
  loads run until one succeeds.
- **`K-DESTINATION`'s generator is paced by the commits.** An engine writes faster than a
  destination commits, and with one commit in flight it gathers whatever is written meanwhile:
  against a destination that takes tens of milliseconds a commit, an optimised engine gathers
  nearly all 2,000 rows into its second commit, before most points. So until as many commits
  have started as the latest point counts, each read of the generator holds its next push once it
  has sent 32 rows of its partition since the latest commit started, or since it started where
  that is later, as a read of a new attempt does, and sends it as the next commit starts. Every
  point then finds rows still to write, however slowly the destination commits, and while paced
  a commit takes about 64 rows. Writes still overlap the commit in flight. The engine took every
  seal of that commit before calling it, and a read holds only a push that follows a checkpoint,
  so the rows a holding read sent are sealed, none of them in that commit, and, being at least
  what makes a commit due, make the next one due. A read answers a barrier it owes while it holds
  by sending its last checkpoint again.
- **A clause passes only on a kill that landed** (ADR 0050): for a spawned connector, `SIGKILL`
  to its process group, which the host then sees end the connection unless a process outside
  the group holds it open (`docs/certify/clauses.md`); for one reached at an
  endpoint or served in process, the host's cut of its connection, which lands by itself. The
  pass notes which (`killed`, `cut`). When no kill landed, the clause draws new points and loads
  again, up to four draws (`DRAWS`), and is then unobserved, with the seed: a `K-SOURCE` load may
  end first, as one of a source smaller than about one credit window does, and a kill may reach
  nothing, as one of a launcher whose connector outlives it does. `K-SOURCE` is also unobserved
  when no read of the source started after a kill: one that lands once the source was read to its
  end, as on a busy machine whose commits come late, leaves nothing to resume. Pacing makes
  `K-DESTINATION` kill at every point it draws, in its first draw, though two kills may end one
  connection. A clause takes at most 300 s, or what `--kill-timeout` (`Target::kill_timeout`) gives
  it: a connector slower than about two seconds a commit needs more.
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
- A source that cannot read again certifies incomplete, `K-SOURCE` unobserved; the simulation,
  whose sources forget what they were told, and the engine's crash tests check how it resumes.
- A killed spawned connector writes no coverage profile, so its killed runs go unmeasured; the
  processes that start again are measured.
