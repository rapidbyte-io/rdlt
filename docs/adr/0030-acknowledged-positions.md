# ADR 0030: Certifying where a source stands

Status: accepted, 2026-09-30.

## Context

Spec §20.8 lists `S-ACK`, "CDC position advanced only in `committed`", among the source clauses.
Exactly-once delivery depends on it: a replication slot or consumer group that moves as the source
reads, or moves anywhere other than the cursor the engine committed, loses rows when a load fails
before the destination commits. It matters most for a source that cannot read again (ADR 0029),
where that position is the only record of what was delivered. The engine cannot see the position,
because the source keeps it outside the engine. ADR 0029 moved the clause and its probe to M5c2.

## Decision

- **The probe.** `ReadStream::acknowledged(source, partition)` answers where the stream stands for
  a partition outside the engine: the cursor it was last told is committed, as it keeps that
  cursor beyond a connection (a slot's confirmed position, a group's committed offset). It
  answers none where it keeps nothing, or was never told. A connector says it answers with
  `SourceConnector::ACKNOWLEDGES`, which `#[source(id = .., acknowledged)]` sets. Only
  certification asks: `SourceFactory::connect_acknowledging` connects the source together with an
  `AcknowledgedReader`, and a source that does not answer refuses with the `acknowledged` code.
- **Exact cursors.** A partition that keeps a position stands at exactly the cursor it was told:
  the SDK encodes the typed cursor the stream answers as it encodes checkpoints, so equal typed
  cursors compare equal. A source whose cursor holds more than its slot keeps rebuilds the whole
  cursor, or keeps the rest itself. A partition that keeps no position, as a snapshot partition
  of a database's change stream does, answers none before and after it is told.
- **The wire.** The handshake feature `acknowledged` is accepted where the client offers it and the
  source answers. `ReadAcknowledged(stream, partition)` answers the encoded cursor, or none, and
  is refused as unsupported on a connection whose handshake did not accept the feature.
  `rdlt-certify` asks each question over a connection of its own, as a slot's position is read
  apart from the connection that moved it. What it answers is therefore what the source keeps
  beyond a connection.
- **The clause** statement: a change stream's position outside the engine moves only when the
  engine tells it a cursor is committed, and then to that cursor. Where the source does not
  answer, or reads no stream as changes, the clause is skipped. Otherwise it takes the first
  stream read as changes, and:
  - before any clause reads, records where each partition of the stream's first phase stands;
  - runs last, so its commits disturb no other clause's reads (a source that cannot read again
    refuses reads from before its position), and first checks that those reads moved no
    partition;
  - reads the stream's phases as the engine does: each partition of a phase to its end, then
    plans again from where they ended, at most four phases, stopping at a phase with an
    unbounded partition. This reaches a database's changes after its snapshot;
  - in each partition, reads on from where it stands, since an earlier load may have moved it,
    and asks again at each checkpoint over a one-event channel, so the read waits while it is
    asked. It then tells the source the first two checkpoints ahead are committed, checks the
    partition stands at each, and reads on from each without committing, which must move nothing;
  - where nothing lies ahead of a partition that stands somewhere, reads it again from where its
    phase starts (only for a stream that can read again), which must move nothing;
  - stops a read of an unbounded partition after three checkpoints, or after a quiet second. A
    read asked to stop that is still quiet five seconds later waits for data, and is dropped.
    Any violation stops the read at once.

  The clause passes where some partition stood at a cursor it was told. It fails where a partition
  moved while read, stood elsewhere once told, or no partition kept any position it was told. It
  is skipped where nothing lay ahead anywhere.
- **The reference change source** keeps a slot: named by its `slot` configuration, shared by the
  sources of the process that name it, moving only in `committed` and never back. It passes
  `S-ACK` in process and through the protocol.

## Deviations from the spec

- **The clause reads a stream's first phases itself**, rather than reading a stream in the engine,
  so it certifies a source alone; the phases it reads follow the engine's rule.
- **A partition may keep no position**, as the spec's clause did not say. A source that keeps none
  anywhere it was told fails.

## Consequences

- Certifying a source that answers moves its positions: it is certified against a slot or consumer
  group of its own (`SourceConnector::ACKNOWLEDGES` says so).
- A source that cannot read again, read wholly by the clauses before `S-ACK`, has nothing ahead to
  commit, and the clause checks only that those reads moved nothing.
- A source's position is now tested, not only its data: M5d's streaming sources, which read without
  end and forget what they acknowledged, answer the probe and are certified by it.
