# ADR 0021: Complete certification

Status: accepted, 2026-09-27.

## Context

Certification over the wire (ADR 0020) must check every clause whose feature exists, as it does
in process:

- the eleven destination clauses that compare what was published with what was committed need
  the published data, which a destination reached by its binary or endpoint can send back only
  through the protocol;
- names as long as a destination allows and as many writers as it runs need clauses of their
  own;
- `S-CHECK` and `D-CHECK` must check that a check agrees with a read or an open, not only that it
  succeeds;
- `P-MALFORMED` must send a frame that cannot be decoded, not only a well-formed message out of
  order, and `P-LIMITS` every limit a connector declares, not only the configuration's.

## Decision

- **A destination reads back what it published, for certification only.**
  - A connector opts in, type-checked: it implements `ReadBack`, and is served through
    `readable_destination_factory`. Both exist only with `rdlt-connector`'s `certify` feature,
    and a binary serves the read-back only where its `main` names that factory (ADR 0044).
  - The host offers the handshake's `published` feature; a destination served reading back accepts
    it, and then serves `ReadPublished`, which sends a table's published rows as a read's frames, a
    batch at a time as its host takes them. Without the feature accepted, the call is refused as
    unsupported.
  - The engine never calls it: the reader is kept beside the served destination, not on the
    engine-facing `Destination`, and only `rdlt-certify`'s raw client calls it.
  - `rdlt-certify` reads back through its `ReadBackProbe` when the destination accepts the
    feature; otherwise the clauses that read published data are unobserved (ADR 0050). A
    read-back that fails, takes longer than 30 s, or sends more than 64 MiB of frames or
    100,000 rows of a table fails what needs it, and so does one whose rows expand past what a
    clause admits (ADR 0050); so does every read-back of a destination whose handshake offering
    the feature failed. The probe's own handshakes keep the connection deadline.
  - Only `P-HANDSHAKE`'s handshake offers a feature no host defines, named anew each time: a
    connector ignores the features it does not know, and `P-HANDSHAKE` fails one that accepts or
    refuses it (ADR 0050).
  - The reference destinations (memory, SQLite, files) read back, with the reference crate's
    `certify` feature. Their own binaries serve no read-back: they are certified in process, or
    served by a binary built for certification.
  - A read-back certifies a destination against its own account of what it published. A probe
    that reads the store itself, as in-process tests use, remains the stronger check.
- **The clauses.**
  - `D-NAMES`: a table and a column named as long as the destination's rules allow, a column
    beyond ASCII when it allows any character, and one in mixed case when it keeps case, each
    folded as its rules fold, are published under their names. Unobserved below 32 bytes.
  - `D-LANES`: as many writers of one table as the destination runs at once, up to four, stage
    at the same time, and a commit publishes what each staged. Inapplicable for one writer.
  - `S-CHECK`: check succeeds exactly when a read of the first partition starts, and both do.
  - `D-CHECK`: check succeeds exactly when opening a session does, and both do.
  - `P-MALFORMED` also sends a destination a batch frame that is no IPC message: it must be
    refused with `malformed_frame`, and the connection serve on.
  - `P-LIMITS` also sends a source a cursor, and a destination a batch frame, one byte beyond
    the limits the connector declares. A limit beyond the host's own is not exceeded, and a
    connector that declares no limits has none checked. The frame, 64 MiB by default, must be
    sent within the clause's 30 s, so a link slower than about 20 Mbit/s times it out.
- **A served source refuses a cursor beyond its declared limit** with `limit_exceeded`, as every
  limit is enforced on receive, not as an invalid message.
- **Clauses read the protocol as any transport delivers it**: `P-HEARTBEAT` reads one answer per
  ping, whatever the answers' stream does after; a streaming call's refusal is read whether it
  comes before its answer's headers or as its first message; a clause that never ends is pinned
  to its bound; the binary refuses a URL other than `grpcs://`, TLS flags with a binary, and
  `--env` with an endpoint as usage errors, and a connector file that cannot be executed as an
  I/O error.

## Consequences

- A connector author makes a destination fully certifiable from its binary by reading back what
  it published; one that does not is certified with those clauses unobserved, so its report is
  incomplete (ADR 0050).
- `D-NAMES` and `D-LANES` read what was published, so they run wherever that can be read; the P
  clauses send a destination a frame as large as its frame limit, 64 MiB by default.
- The `K` clauses and `S-ARROW-JSON`, which need an engine, are ADR 0022's, and `S-PARTITION`
  ADR 0032's.
