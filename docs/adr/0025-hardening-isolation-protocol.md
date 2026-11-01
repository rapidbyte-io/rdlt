# ADR 0025: Hardening, isolation and the protocol

Status: accepted, 2026-09-28.

## Context

Pipelines share destinations, and what a connector sends is not to be taken on trust. Without the
decisions below:
- two pipelines share a table, and one's replace wipes the rows the other committed;
- connector input goes unchecked:
  - a checkpoint answering a barrier never asked for switches off checkpoints on demand;
  - a `retry_after` of any length parks a run;
  - a heartbeat interval of zero, or zero missed heartbeats, loses every connector;
- a connector receives its configuration, secrets included, before the host checks which
  connector it is, and a binary's digest is recorded but never compared;
- a served read loses whether its partition ends;
- the files destination leaves new directories unsynced, and treats a lost file as transient;
- identifier fitting cuts into the reserved prefix below a 15-byte limit;
- tests miss a fault between a commit's data and its state, certification clauses that count
  rows pass a destination publishing the wrong segment, a merge clause passes one keeping the
  first row of a key, and the reference connectors escape mutation testing.

JSON exactness, which needs values judged and not only types, is ADR 0026's.

## Decision

- **A table belongs to the pipeline that created it.**
  - A schema change or writer of another pipeline's session fails with a `Config` error coded
    `table_owned`, before it changes anything (clause `D-OWNED`). A pipeline creating its own
    table again, as a retry does, succeeds.
  - Each destination records the owner:
    - memory and the simulation in the table;
    - sqlgen in a catalog table, `_rdlt_owners`, keyed by the table's name; the planner takes
      the owner for every statement that changes a table, child tables, swaps, drops and discarded
      staging included, and writes the record only where a table is created (ADR 0049);
    - the files destination in an `owner` file in the table's catalog, created exclusively.
  - The simulation's two pipelines never share a table. So once a phase converges, a third
    pipeline loads one of the first pipeline's tables. It must be refused as `table_owned` and
    change no table.
  - Rejected: scoping table names by pipeline. Names are what users query; two pipelines meeting
    at one name is a configuration mistake to report, not to hide. A table is handed over by a
    reset that drops it (ADR 0033).
- **Connector input is validated.**
  - A checkpoint answering a barrier greater than any requested fails:
    - in process, as `barrier_unrequested`;
    - over the wire, as `invalid_message`.

    The answered barrier only moves forward.
  - Only a rate limit's `retry_after` is kept, held between the retry policy's first and longest
    delays (ADR 0042), so the ten years the simulation now and then asks for stay within the
    run's limit.
  - `Options::missed` is a `NonZeroU32`, and `Connection::handshake` refuses a zero heartbeat
    interval as `options_invalid` before any I/O.
- **The handshake answers who the connector is before it sees its configuration.**
  - `Handshake` agrees the version, role and features, and answers the spec without the
    capabilities that configuration decides.
  - A `Configure` call carries the configuration, connects the connector, and answers the
    full spec. Its id and version must be the handshake's; otherwise it is refused as
    `invalid_message`.
  - Calls are refused as follows:
    - any call before the handshake, `Configure` included: `no_handshake`;
    - a call before `Configure`: `not_configured`;
    - a second `Configure`: `configure_repeated`.

    `P-ORDER` checks all three.
  - The protocol's major version is checked first, so a peer of another version is refused at
    the handshake rather than misread.
  - The host checks the handshake's spec against the reference before configuring. On a respawn
    or redial, the spec must be the id and version first placed, or the connector is refused as
    `connector_changed` before it sees the configuration.
  - `ConnectorRef` has an optional `digest`, which the binary must match before it spawns.
  - Every respawn hashes the binary again and refuses one changed since placement
    (`DigestMismatch`, and `connector_changed` to the engine).
  - On Linux a binary is hashed and executed as one open file, so what runs is what was hashed,
    and a binary another user may write is refused; a raw wire compares the digest too (ADR 0043).
    Rejected: executing from a content-addressed private copy, which costs a copy of every
    binary.
- **A served read knows whether its partition ends.** `ReadStart.unbounded` carries it.
- **The files destination's directories are durable.**
  - A directory it creates is synced into its parent, and each new file's directory after the
    file.
  - A file its manifests or staging list, found missing, is a `Data` error coded `file_missing`:
    no retry finds it.
  - A manifest removed by garbage collection while listed stays transient, and so does a source
    file removed between listing and opening.
- **Tests hold these rules.**
  - The files destination's commit, the one reference commit that writes data before state, is
    failed between its merged files and its manifest. The table stays as it was, and the retry
    publishes once.
  - The simulation's destination commits atomically, as the contract requires. A fault inside such a
    commit is one before or after it, which the simulation injects.
  - Certification clauses compare whole rows, ids and values: each segment's rows carry ids of their
    own. A destination publishing another staged segment fails `D-COMMIT`, `D-REPLACE`, `D-MERGE`
    and `D-CHILDREN`, and one that loses a value fails every clause that reads its rows back.
  - `D-MERGE` puts one key's newest row first and another's last. A merge keeping either the
    first or the last row of a key fails it.
  - `rdlt-connector-reference` is mutated, and its own tests, not only certification's, catch
    its mutants. A branch whose mutants change nothing is written plainly. Mutants a loaded
    machine counts as caught, by a test timing out, prove nothing, so mutation results are taken
    from a quiet run.
- **Dialects' identifiers hold at least 63 bytes.** A name too long is cut to a SHA-256 of the
  whole name under a prefix no table may take, which fits them; `SqlPlanner::try_new` refuses a
  dialect below that as `Unsupported` (ADR 0049).

## Consequences

- A table another pipeline created cannot be loaded until that pipeline releases it with a reset
  that drops it (ADR 0033).
- The hash of a spawned connector's binary is computed again at every respawn. That is a cost for
  large debug binaries only.
