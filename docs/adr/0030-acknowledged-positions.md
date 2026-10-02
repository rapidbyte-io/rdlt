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
  certification asks, of `acknowledging_source_factory`, which `rdlt-connector`'s `certify`
  feature adds (ADR 0044): `SourceFactory::connect_acknowledging` connects the source together with an
  `AcknowledgedReader`, connected apart from it, and a source that does not answer refuses with
  the `acknowledged` code. Certification then treats it as a source that says nothing. A reader
  that fails to connect fails `S-ACK` alone.
- **Visibility.** `committed` returns once `acknowledged` answers the cursor it was told: a source
  that acknowledges in the background, as a replication slot's standby feedback does, waits
  there until its position has moved.
- **Exact cursors.** A partition that keeps a position stands at exactly the cursor it was told:
  the SDK encodes the typed cursor the stream answers as it encodes checkpoints, so equal typed
  cursors compare equal. A source whose cursor holds more than its slot keeps rebuilds the whole
  cursor, or keeps the rest itself. A partition that keeps no position, as a snapshot partition
  of a database's change stream does, answers none before and after it is told.
- **The wire.** The handshake feature `acknowledged` is accepted where the client offers it and the
  source is served by the factory that answers. A report that a position is committed is heard
  for a checkpoint a read sent the reporting host, and for where that host's latest read of
  the partition started, once the source accepted the read; the engine reports every partition
  a commit covers, moved or not, so a report that failed is made again by the next attempt
  (ADR 0044). `ReadAcknowledged(stream, partition)` answers the encoded cursor, or none, and
  is refused as unsupported on a connection whose handshake did not accept the feature.
  `rdlt-certify` asks every question over one connection of its own, which never reads or
  commits, as a slot's position is read apart from the connection that moved it. What it answers
  is therefore what the source keeps beyond a connection. A served read the host leaves is
  aborted, so a read that waits for data does not hold the source after its host is gone.
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
  - records a partition's end as the engine does: rows after its last checkpoint leave a bounded
    partition done, so a snapshot read without checkpoints still moves the stream on;
  - in each partition, reads on from where it stands, since an earlier load may have moved it.
    At each checkpoint it asks where every partition it has seen stands, over a one-event
    channel, so the read runs at most an event ahead of the questions. It then tells the source
    the first two checkpoints ahead are committed, checks the partition stands at each and every
    other partition where it stood, and reads on from each without committing, which must move
    nothing;
  - where nothing lies ahead of a partition that stands somewhere, reads it again from where its
    phase starts (only for a stream that can read again), which must move nothing;
  - asks a stream that checkpoints on demand for a checkpoint as each read starts and after each
    it sends;
  - stops a read of an unbounded partition after three checkpoints, or once five seconds pass
    without one, whatever else it sends. A read asked to stop that is still quiet five seconds
    later waits for data, and is dropped. Any violation stops the read at once.

  The phase read last decides. The clause passes where each partition told a checkpoint there
  stands at the last it was told. It fails where a partition moved while read, stood elsewhere
  once told, or keeps no position there. It also fails where, in any phase, some partitions keep
  what they were told and others keep nothing. It is skipped where nothing lay ahead in the last
  phase.
- **The reference change source** keeps a slot: named by its `slot` configuration, shared by the
  sources of the process that name it, moving only in `committed` and never back. It passes
  `S-ACK` in process and served through the protocol. A slot of its process is not one a spawned
  change source shares, so such a source is certified served, not spawned.

## Deviations from the spec

- **The clause reads a stream's first phases itself**, rather than reading a stream in the engine,
  so it certifies a source alone; the phases it reads follow the engine's rule.
- **A partition may keep no position**, as the spec's clause did not say, where no partition of
  its phase keeps one and a later phase is read. A source whose last phase keeps none fails.

## Consequences

- Certifying a source that answers moves its positions: it is certified against a slot or consumer
  group of its own (`SourceConnector::ACKNOWLEDGES` says so).
- A source that cannot read again, read wholly by the clauses before `S-ACK`, has nothing ahead to
  commit, and the clause checks only that those reads moved nothing.
- A source's position is now tested, not only its data: M5d's streaming sources, which read without
  end and forget what they acknowledged, answer the probe and are certified by it.
