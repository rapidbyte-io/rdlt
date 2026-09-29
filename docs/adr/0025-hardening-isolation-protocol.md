# ADR 0025: Hardening, isolation and the protocol

Status: accepted, 2026-09-28.

## Context

ADR 0024 fixed the data-correctness findings of the owner's adversarial review after M5a (H1a)
and left the rest for H1b. They are:
- two pipelines sharing a table, where one's replace wipes the rows the other committed;
- connector input that goes unchecked:
  - a checkpoint answering a barrier never asked for switches off checkpoints on demand;
  - a `retry_after` of any length parks a run;
  - a heartbeat interval of zero, or zero missed heartbeats, loses every connector;
- a connector receiving its configuration, secrets included, before the host checks which
  connector it is, and a binary's digest recorded but never compared;
- the files destination leaving new directories unsynced, and treating a lost file as transient;
- test gaps:
  - no fault between a commit's data and its state;
  - certification clauses that count rows, so publishing the wrong segment passes;
  - a merge clause passed by keeping the first row of a key;
  - the reference connectors outside mutation testing.

H1a's review deferred more:
- a served read losing whether its partition ends;
- identifier fitting that cuts into the reserved prefix below a 15-byte limit;
- JSON exactness across pushes, and big integers in the simulation.

## Decision

The findings split in two, as ADR 0024 split H1a from H1b:
- **H1b** (this ADR): isolation and the protocol.
- **H1c**: JSON exactness. It covers:
  - int-then-float columns;
  - integers beyond 64 bits in the simulation, and the test kit's JSON text of scaled decimals;
  - a decimal key changing precision at the same scale;
  - integers beyond 128 bits in `Json` columns.

Each needs value-level exactness kept in table state, and a model of it in the simulation. M5b
follows H1c.

- **A table belongs to the pipeline that created it.**
  - A schema change or writer of another pipeline's session fails with a `Config` error coded
    `table_owned`, before it changes anything (clause `D-OWNED`). A pipeline creating its own
    table again, as a retry does, succeeds.
  - Each destination records the owner:
    - memory and the simulation in the table;
    - sqlgen in a new catalog table, `_rdlt_owners`, keyed by the table's name;
    - the files destination in an `owner` file in the table's catalog, created exclusively.
  - The simulation's two pipelines never share a table. So once a phase converges, a third
    pipeline loads one of the first pipeline's tables. It must be refused as `table_owned` and
    change no table.
  - Rejected: scoping table names by pipeline. Names are what users query; two pipelines meeting
    at one name is a configuration mistake to report, not to hide. Handing a table over waits for
    reset (M5d).
- **Connector input is validated.**
  - A checkpoint answering a barrier greater than any requested fails:
    - in process, as `barrier_unrequested`;
    - over the wire, as `invalid_message`.

    The answered barrier only moves forward.
  - A connector's `retry_after` is waited for no longer than the retry policy's `max_delay`, as
    every other backoff is. The simulation now and then asks for ten years, which the cap keeps
    within the run's limit.
  - `Options::missed` is a `NonZeroU32`, and `Connection::handshake` refuses a zero heartbeat
    interval as `options_invalid` before any I/O.
- **The handshake answers who the connector is before it sees its configuration** (protocol 2).
  - `Handshake` agrees the version, role and features, and answers the spec without the
    capabilities that configuration decides.
  - A new `Configure` call carries the configuration, connects the connector, and answers the
    full spec. Its id and version must be the handshake's; otherwise it is refused as
    `invalid_message`.
  - Calls are refused as follows:
    - any call before the handshake, `Configure` included: `no_handshake`;
    - a call before `Configure`: `not_configured`;
    - a second `Configure`: `configure_repeated`.

    `P-ORDER` checks all three.
  - `HandshakeRequest.config_json` is reserved.
  - The protocol's major version is 2, so a peer of version 1 is refused at the handshake rather
    than misread.
  - The host checks the handshake's spec against the reference before configuring. On a respawn
    or redial, the spec must be the id and version first placed, or the connector is refused as
    `connector_changed` before it sees the configuration.
  - `ConnectorRef` gains an optional `digest`, which the binary must match before it spawns.
  - Every respawn hashes the binary again and refuses one changed since placement
    (`DigestMismatch`, and `connector_changed` to the engine).
  - Rejected: executing from a content-addressed private copy to close the window between
    hashing and executing. It costs a copy of every binary, and whoever can write the binary's
    path already controls the host.
- **A served read knows whether its partition ends.** `ReadStart.unbounded` carries it.
- **The files destination's directories are durable.**
  - A directory it creates is synced into its parent, and each new file's directory after the
    file.
  - A file its manifests or staging list, found missing, is a `Data` error coded `file_missing`:
    no retry finds it.
  - A manifest removed by garbage collection while listed stays transient, and so does a source
    file removed between listing and opening.
- **Tests close the gaps.**
  - The files destination's commit, the one reference commit that writes data before state, is
    failed between its merged files and its manifest. The table stays as it was, and the retry
    publishes once.
  - The simulation's destination commits atomically, as the contract requires. A fault inside
    such a commit is one before or after it, which the simulation already injects.
  - Certification clauses compare whole rows, ids and values: each segment's rows carry ids of
    their own. A destination publishing another staged segment now fails `D-COMMIT`,
    `D-REPLACE`, `D-MERGE` and `D-CHILDREN`, and one that loses a value fails every clause that
    reads its rows back.
  - `D-MERGE` puts one key's newest row first and another's last. A merge keeping either the
    first or the last row of a key fails it.
  - `rdlt-connector-reference` is mutated and its tests run against mutants. Its full runs left
    mutants alive in every reference connector:
    - the change source's defaults, validation, keys and draws, batches, and partitions;
    - the generator's hash, defaults and partitions;
    - merges of children and change rows, and the columns a change stream stores;
    - SQLite's error kinds, widenings and read-back, a replace that staged nothing, and a child
      table following a root where it staged nothing;
    - the files destination's read-back, formats, discards, manifest links, and unlistable
      manifests and catalogs, and the files source's reads;
    - the memory destination's read-back, tables, and staging a newer session discards;
    - the writers' and commits' statistics, and which pipeline's staging an open discards.

    Tests catch each now. Branches whose mutants changed nothing are written plainly:
    - a cast to a column's own type;
    - the newest sequence taken as a maximum;
    - batches kept when not empty;
    - an owner file created whenever it cannot be read;
    - a snapshot's changes skipped;
    - a directory's missing ancestors found by whether they exist;
    - a new file's link judged in one place, for manifests and catalogs alike;
    - and a `Debug` implementation nothing uses, and SQLite reading integers as reals where no
      column declared real ever holds them, removed.

    Mutants a loaded machine counts as caught, by a test timing out, prove nothing, so mutation
    results are taken from a quiet run. Mutated against its own and the engine's tests only, the
    crate showed which mutants only certification's tests had caught; its own tests catch them
    now.
- **Mutation testing runs crate by crate on a branch, and whole every night** (amends ADR 0003).
  - A milestone's loop had grown to hours, most of it the same checks run again. The branch keeps
    its gate, zero surviving mutants, but `just mutants-diff` now tests each changed crate's
    mutants against the packages whose tests can catch them:

    | Mutated crate | Tests it runs |
    |---|---|
    | `rdlt-engine` | `rdlt-engine` |
    | `rdlt-connector` | `rdlt-connector`, `rdlt-connector-reference`, `rdlt-engine`, `rdlt-host`, `rdlt-certify` |
    | `rdlt-connector-reference` | `rdlt-connector-reference`, `rdlt-engine` |
    | `rdlt-wire` | `rdlt-wire`, `rdlt-host` |
    | `rdlt-host` | `rdlt-host`, `rdlt-certify` |
    | `rdlt-certify` | `rdlt-certify` |

  - The nightly workflow runs the full pass, every crate's tests against every mutant, in eight
    shards on CI's runners, where the weekly workflow ran it once a week. A mutant it finds is
    fixed the next day; one the branch's packages missed adds the package that caught it.
  - `just ready` runs lint, tests and `mutants-diff`. CI's pull-request gate runs the rest:
    coverage, Miri, the simulation and macOS.
  - The reference connectors' tests sync files and SQLite to disk, and four mutants tested at once
    made a one-second test take ten, which counted mutants as caught by no assertion. Those tests
    get thirty seconds before the mutants profile counts them hung.
  - Rejected: a RAM disk for mutation runs, which the development machine's memory cannot spare.
- **Dialects' identifiers hold at least 30 bytes.** That is the shortest limit a supported
  database has, and it keeps a derived name's reserved prefix, its hash and part of the table's
  name. `SqlPlanner::try_new` refuses a dialect below it as `Unsupported`.

## Consequences

- Connectors built for protocol 1 must be rebuilt. Nothing is published, so none exists
  outside this repository.
- Spec §12.3 gains `Configure`, and §13.1's `ConnectorRef` gains `digest`. This ADR amends both.
- A table another pipeline created cannot be loaded until that pipeline releases it, which reset
  brings in M5d.
- The hash of a spawned connector's binary is computed again at every respawn. That is a cost for
  large debug binaries only.
