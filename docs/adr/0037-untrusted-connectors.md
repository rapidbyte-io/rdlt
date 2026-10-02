# ADR 0037: Connectors are untrusted code

Status: accepted, 2026-10-01.

## Context

Until now a connector's code was trusted and only its output was not: the operator chose which
binaries ran, and rdlt neither sandboxed them nor defended itself against one that misbehaved on
purpose. The product will run connectors written by third parties. An engine that trusts
connector code cannot do that: one connector could crash it, starve it, or reach another
pipeline's data.

A security review of `main` after M5 measured the distance. Output that passed every stated limit
could still exhaust, hang or abort the engine; control messages, state and receipts were taken at
their word; a spawned connector ran with the host's own access. The findings are fixed by a
hardening program, H2, whose milestones each have an ADR after this one. This record is the
decision they implement.

## Decision

- **The engine and the host are the trusted computing base. A connector is untrusted code**,
  unless it is compiled into the embedder's binary: an in-process connector registered in a
  `Registry` is trusted by construction, as any code the embedder links is.
- **Everything a connector sends is hostile input**, not only its data frames: its spec and
  capabilities, catalogs, plans, cursors, checkpoints, state records, receipts, errors and their
  text, deadlines, credit, its timing, and its silence.
- **The engine survives any connector.** No connector behaviour may crash, abort or hang the
  host, or take memory, disk, descriptors or processor time beyond the host's configured budgets.
  - Every input is validated before it is used.
  - Every input is bounded by a limit, as a whole and not only piece by piece.
  - Every input is charged to a budget before it is held, at what it costs to hold and to
    expand.
  - Every wait on a connector has a deadline.
- **Damage is confined to the connector's own pipeline.** A connector cannot read or change
  another pipeline's tables, state, log or secrets, and cannot make the engine do so.
- **Untrusted code never shares the engine's address space or its access.** Where a connector
  runs is policy:
  - In process: trusted connectors only.
  - Remote, over mutual TLS (ADR 0018): any connector. The container or machine it runs in is
    the isolation.
  - A local process: the host refuses to spawn a connector unless it is given a sandbox to spawn
    it in, or the operator states that the binaries are trusted. rdlt provides a sandbox for
    Linux built on bubblewrap: the connector sees no file it was not granted, no network unless
    granted, no other process, a cleared environment, and dies with its host. macOS has no such
    sandbox, so untrusted connectors run remotely there.
  - A placement the policy does not permit is refused with a typed error. Nothing degrades
    silently to a weaker isolation.
- **Whoever writes a pipeline's configuration is not trusted with the host's secrets.** In a
  service, a configuration's author may be a tenant, not the operator. A configuration
  reaches the host's environment variables, files and named secrets only as the operator
  lists them; by default it reaches none (ADR 0043). Amended 2026-10-02.
- **What a connector is granted of the host's files is bounded by the operator.** A pipeline's
  author may ask for paths on the reference that places its connector, and gets them only
  within the roots the operator names on the provider, read or write; by default the
  operator names none, and nothing is granted. A root that may be written may hold nothing
  that decides what the host runs or keeps (ADR 0043). Amended 2026-10-02.
- **A connector receives only its own configuration**, and only after the host has verified
  which connector it is: its id and version, the digest of its binary, or the name in its
  certificate. Secrets are never quoted in errors, logs or reports, and connector text is
  sanitized and bounded where the host receives it, not only where it is displayed.
- **A served connector trusts its host as far as the host's certificate says**: it accepts the
  hosts named to it, not every certificate its CA signed.
- **A production build has no test or development surface**: no read-back of published tables,
  no crash points, no test connectors.
- **Persistent state is checked when it is read.** State records, logs and manifests are bound to
  their pipeline, validated beyond their shape, and never written in a form their reader cannot
  read back.
- **What an untrusted connector can still do is stated.** A source can send wrong data. A
  destination can lose or corrupt the data it is given and claim commits it never made: it holds
  the destination's credentials. The engine guarantees its own survival and the confinement of
  the damage, not the honesty of a connector's data.

Rejected:
- **Keeping connector code trusted and isolating by deployment alone.** It leaves the engine one
  malformed message from a crash, and makes every third-party connector a decision to trust it
  completely.
- **A sandbox written in this repository** from namespaces and seccomp filters. It needs
  `unsafe` code between `fork` and `exec`, which the workspace forbids, and it is where sandboxes
  go wrong. A `Sandbox` is a trait: another launcher can be plugged in.
- **Refusing local placement of untrusted connectors altogether.** A sandboxed process is how a
  self-hosted deployment without containers runs them.
- **Authenticating the write-ahead log with a key.** Whoever can write the log's directory as the
  engine's user can already change the engine's state. The log is bound to its pipeline, private
  to its user, and validated against the destination when it is replayed; a sandboxed connector
  cannot reach it.

## Consequences

- A weakness that needs a connector to misbehave is a defect, not an accepted risk. Earlier
  records that accepted one on those grounds are reopened by H2: the host-certificate allowlist
  of ADR 0018, the items ADR 0025 deferred, and the acknowledgement order of ADR 0034.
- Limits are part of the contract. A connector that is refused for exceeding one was told the
  limit in the protocol's documentation, and certification checks it.
- An embedder who registers a connector in process vouches for it as for their own code.
- `SECURITY.md` states this model, and that it is being put in place: until H2 is complete, an
  operator should run only connectors they trust.
