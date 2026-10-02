# ADR 0042: The control plane bounded

Status: accepted, 2026-10-02.

## Context

ADR 0037 makes everything a connector sends hostile input, and ADR 0038 bounds its Arrow frames.
The rest of what crosses between the engine, its host and a connector, the control plane, was
still taken largely on trust:

- every message of every call was decoded within the limit sized for a frame, 64 MiB, and a
  decoder builds what a message's fields become before anything checks them, 30 to 90 times the
  bytes for empty repeated entries;
- catalogs, plans, identifier rules and coordinator bookkeeping were unbounded in count and
  checked in quadratic time;
- names took any character but controls, and destination identifiers from the wire none at all;
- some waits had no deadline: a call into a connector in process, a read asked to stop, a write
  whose credit trickled in, and a served connection whose peer took nothing;
- what a connector said could change what was committed, or how the engine retried: a receipt
  for another commit settled the log, a catalog's new key re-keyed a merge table, a connector's
  error claimed a fence or set any wait, and a partition that lost its place looped without end;
- tonic panicked on status details that were not base64.

## Decision

- **Each call's messages are decoded within the limit of what the call carries.** `Limits` gains
  `catalog_bytes` (4 MiB), `state_bytes` (16 MiB: an open's answer, a plan and its request, a
  commit's request, a report of committed positions) and `control_message_bytes` (256 KiB),
  carried in the handshake as fields 12 to 14. A handshake and its answer, which come before
  either end knows the other's limits, are bounded by `HANDSHAKE_BYTES` (4 MiB). A configuration,
  a schema change and a read's start are bounded by their field's limit and 64 KiB more; only
  reads and writes take frames.
  - The host decodes each answer with a client of its class, over one channel. A served
    connector decodes each request with a server of its class, chosen by the call's path. Each
    decoder refuses a length beyond its class before it reserves it.
  - A message beyond its class is refused by tonic's own length check, as an `OutOfRange` status
    the host reports as a transport failure naming the sizes; a body-level parser of its own
    would duplicate that check.
  - A served connection holds at most 200 open calls, set explicitly.
- **Lists are bounded and checked in linear time.** A catalog holds at most 65,536 streams and a
  plan 16,384 partitions, in every placement; a plan names each partition once and starts only
  those it names. A destination's identifier rules hold at most 4,096 reserved words and 64
  reserved prefixes of 256 bytes each, and an identifier at least 16 bytes long. Naming folds the
  rules once; the coordinator tracks partitions by id and counts what has ended.
- **Names are what a reader sees.** Stream names, namespaces, partition ids and table path
  segments refuse every character that shown text escapes (ADR 0037's classifier); naming
  replaces them for a destination that takes any character. Every destination identifier the
  wire carries is non-empty, within 65,535 bytes and free of them, and a table's schema version
  is at least one. Column paths are source data and stay as they are.
- **Every wait on a connector has a deadline.**
  - The engine ends every call into a source or a destination but a read at
    `EngineConfig::connector_wait` (30 minutes), in every placement.
  - A read asked to stop is dropped after `EngineConfig::stop_wait` (60 s).
  - A remote write's whole wait for credit, a flush's stats or an ending write's error is bounded
    by the write-ack deadline, however many answers arrive; a credit of no bytes is refused.
  - A served connection whose writes make no progress for its send wait (60 s) is closed, and a
    stopping connector's drain ends at its drain wait (30 minutes), both `ListenLimits` fields.
  - A failed attempt, and a failed replay, close the session they opened.
- **What a connector says cannot silently change what was committed.**
  - A receipt must answer its own commit, by load and sequence, or the commit fails as
    `receipt_mismatch`; receipt counters are summed with checked arithmetic.
  - A table's state records the merge key and change time it was loaded by; a keyed write by
    another key or change time, or by an empty catalog key, is refused until the table is reset.
  - Opened state holds each key once; a recorded name map is injective; a recorded table name
    must be one the destination's rules could have given, under no reserved prefix and no other
    table's. An epoch never passes its largest value.
- **A connector's failure steers retries only as far as the engine allows.** Only a rate limit
  keeps the wait it asks for, held between the retry policy's first and longest delays. A fence
  is a destination session's alone, and a stop ends a read cleanly only where the engine stopped
  it; otherwise each is the side's failure. A retention reset whose read loses its place again
  before sealing a row fails the attempt as retryable, so backoff and attempts bound it.
- **The commit policy is due by rows a commit can take.** Rows a partition wrote and has not
  sealed count until a commit passes them by, and again once sealed, so a partition that never
  checkpoints keeps no commit due.
- **Machine strings and the protocol are the host's to check.** A code a connector's error
  carries must be `[a-z0-9_.-]`, within the code limit, and none of the host's own codes
  (`connector_lost`, `deadline_exceeded`, `tls`, `transport`); otherwise it is `invalid_code`.
  The handshake answer carries the protocol the connector speaks, and the host refuses another
  major, a spec whose id does not parse or whose version is not one, and a feature it did not
  offer. Status details that do not decode as base64 are dropped by the host's transport before
  tonic reads them.
- **A read's frames that carry no event are bounded by count**: at most a schema and a dictionary
  for each column between events. A read's data credit stays unchecked by the host: it grants
  credit back as the engine takes each frame, so an overrun is undetectable at the receiver
  (ADR 0016).

## Consequences

- A catalog over 4 MiB, state over 16 MiB or a plan of more than 16,384 partitions is refused
  until the operator raises the host's limit or the source plans fewer.
- A pipeline whose stream, partition or recorded identifier holds a hidden or reordering
  character, or whose recorded table name falls under a prefix its destination reserves, must be
  renamed or reset.
- A destination's identifier rules beyond their limits, or with a maximum length under 16, are
  refused at the handshake and before any run or reset.
- A connector built before the protocol's version was answered is refused; none is published.
- A failed attempt's close waits at most the connector wait, 30 minutes by default, before the
  retry begins, for a destination that never answers it.
- H2b's state limits must fit within `state_bytes`, and its dictionary limit takes field 11.
