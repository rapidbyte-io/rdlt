# ADR 0021: Certification complete for today's features

Status: accepted, 2026-09-27.

## Context

ADR 0020 left the `K` kill clauses to M4g. Certifying a connector over the wire also left gaps:

- the eleven destination clauses that compare what was published with what was committed were
  skipped for any destination reached by its binary or endpoint, since the protocol could not
  read published data back;
- §20.8 names clauses whose features already exist that no suite checked: `S-PARTITION`,
  `S-ARROW-JSON`, `D-NAMES` and `D-LANES`;
- `S-CHECK` and `D-CHECK` checked that a check succeeds, not, as §20.8 says, that it agrees with
  a read or an open;
- `P-MALFORMED` sent a well-formed message out of order, not a frame that cannot be decoded, and
  `P-LIMITS` checked the configuration's limit alone.

## Decision

- **M4g is split.**
  - M4g (this record) completes certification for the features that exist.
  - M4h is the `K` kill clauses and `S-ARROW-JSON`: both run an engine, the latter its
    shredder. The owner ruled that `rdlt-certify` gains an optional `kill` cargo feature, on in
    its binary, that depends on `rdlt-engine`; the §4 crate map is amended in M4h's record.
  - `S-PARTITION` ("planned partitions cover the stream exactly once") waits for M5: a plan takes
    no partition count, so nothing gives the clause a truth to compare the partitions with. M5
    re-plans partitions for streaming, and settles it.
- **A destination reads back what it published, for certification only.**
  - A connector opts in, type-checked: it implements `ReadBack`, and is served through
    `readable_destination_factory`, or `#[destination(id = "...", read_back)]`.
  - The host offers the handshake's `published` feature (§12.7); a destination that reads back
    accepts it, and then serves `ReadPublished`, which streams a table's published rows as a
    read's frames. Without the feature accepted, the call is refused as unsupported.
  - The engine never calls it: the reader is kept beside the served destination, not on the
    engine-facing `Destination`, and only `rdlt-certify`'s raw client calls it.
  - `rdlt-certify` reads back through its `ReadBackProbe` when the destination accepts the
    feature, and skips the clauses that read published data otherwise, as before. A read-back
    that fails, takes longer than 30 s, or sends more than 64 MiB of a table fails what needs
    it; so does every read-back of a destination whose handshake offering the feature failed.
  - Every handshake of certification also offers a feature no host defines: a connector ignores
    the features it does not know (§12.7), and `P-HANDSHAKE` fails one that accepts or refuses
    it.
  - The reference destinations (memory, SQLite, files) read back. The memory destination's store
    lives in its process, so its binary, spawned for each connection, reads back nothing another
    connection published; it is certified in process, or through its store's own probe.
  - A read-back certifies a destination against its own account of what it published. A probe
    that reads the store itself, as in-process tests use, remains the stronger check.
- **New and stronger clauses.**
  - `D-NAMES`: a table and a column named as long as the destination's rules allow, a column
    beyond ASCII when it allows any character, and one in mixed case when it keeps case, each
    folded as its rules fold, are published under their names. Skipped below 32 bytes.
  - `D-LANES`: as many writers of one table as the destination runs at once, up to four, stage
    at the same time, and a commit publishes what each staged. Skipped for one writer.
  - `S-CHECK`: check succeeds exactly when a read of the first partition starts, and both do.
  - `D-CHECK`: check succeeds exactly when opening a session does, and both do.
  - `P-MALFORMED` also sends a destination a batch frame that is no IPC message: it must be
    refused with `malformed_frame`, and the connection serve on.
  - `P-LIMITS` also sends a source a cursor, and a destination a batch frame, one byte beyond
    the limits the connector declares. A limit beyond the host's own is not exceeded, and a
    connector that declares no limits has none checked. The frame, 64 MiB by default, must be
    sent within the clause's 30 s, so a link slower than about 20 Mbit/s times it out.
- **What the new clauses found**: a served source took the host's cursor without its declared
  limit, and refused an oversized one as an invalid message. It is now refused with
  `limit_exceeded`, as §12.8 has every limit enforced on receive. A source built on an earlier
  `rdlt-connector` fails `P-LIMITS` until it is built again.
- **M4f's deferred minors are settled**: `P-HEARTBEAT` reads one answer per ping, whatever the
  answers' stream does after; a streaming call's refusal is read whether it comes before its
  answer's headers or as its first message; a clause that never ends is pinned to its bound;
  the binary refuses a URL other than `grpcs://`, TLS flags with a binary, and `--env` with an
  endpoint as usage errors, and a connector file that cannot be executed as an I/O error.

## Consequences

- A connector author makes a destination fully certifiable from its binary by reading back what
  it published; one that does not is certified as before.
- `D-NAMES` and `D-LANES` read what was published, so they run wherever that can be read; the P
  clauses send a destination a frame as large as its frame limit, 64 MiB by default.
- M4's exit criterion (`P` and `K` clauses green) completes with M4h.
