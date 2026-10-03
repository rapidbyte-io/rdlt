# ADR 0045: A write-ahead log that is bound, private, checked and charged

Status: accepted, 2026-10-03.

## Context

ADR 0029 made a load keep a write-ahead log where its source cannot read again what it
acknowledged. The log is the only copy of those rows until their commit lands, and replay applies
what it holds with the engine's authority. Yet a log was found to be much less than that:

- Its directory was named by a sanitized pipeline id and a 32-bit hash, so two pipelines could
  share one, and replay never compared the pipeline a chunk named. The store followed links,
  checked no mode and listed any name, so a stray file could be read as a second copy of a chunk.
- A chunk was appended to in place and synced at each commit, so damage anywhere in the last
  chunk read as a crash's tear: a durable commit behind it was dropped and its log removed. A
  chunk missing, a seal cut out or a batch frame doubled went unnoticed, and the format's
  version sat inside the framing it versioned.
- A frame's length was read before anything bounded it, and batches were decoded with no limit
  on rows or lengths.
- Replay applied a logged commit's fields verbatim: a session newer than any, reset markers,
  another commit's receipt, positions no seal set, table drops; and a partial replay of a commit
  that had landed long ago dropped its tables again.
- What replay staged was never charged to the memory budget, nothing bounded what a log held on
  disk, and a full disk ended the run for good, leaving the log behind it.
- A reset matched a stream's tables by its displayed name, and could discard rows a log held
  that its source had been told were committed.
- Destinations kept every receipt for ever; the files destination kept those of its sixteen
  latest loads, and so forgot receipts a replay could still ask for.

The owner ruled (2026-10-03) that the log's store must be one an object store could implement,
with the semantics the local store has, though no object-store backend is built.

## Decision

### The store is shaped as an object store is

- `WalStore` stages a chunk, appends to the staged chunk, and publishes it whole where no chunk
  of its name exists, which is its durability point; publishing over an existing name fails with
  `AlreadyExists` and leaves that chunk as it was. It lists a pipeline's loads and a load's
  chunks, reads a chunk by range, deletes a chunk, and deletes a load's log in ascending chunk
  order, so a crash part way leaves its highest chunks. Nothing is appended to a published chunk,
  renamed over another, or locked, and nothing relies on a directory's order or on an operation
  over several objects.
- `LocalWal` stages a chunk as a file named `{chunk:08}.{token:016x}.part` in the load's
  directory, created exclusive and 0600; publishing syncs it, links it to `{chunk:08}.wal` with
  `linkat`, which fails where that name exists, unlinks the staged name and syncs the directory.
  Each deletion syncs the directory it removed from.
- One durable write per commit: what was logged since the previous commit, the commit's frame
  and an `End` frame go into the chunk published at that commit.
- Claims are gone. A replay takes a log over by publishing a fence chunk at the number after its
  highest, which a load still running then finds taken when it publishes: it fails as
  `wal_fenced` before it answers its commit. A replay that finds the next number taken eight
  times leaves the log to a later replay, its load still running. A log that its load closed,
  or that a replay released, ends in a chunk that needs nothing, and is deleted by whoever
  finds it.
- A conformance suite, generic over `WalStore`, holds the memory and local stores to this
  contract, and the simulation's store loses what was staged and not published at a crash.

### Every directory and file of a local log is its user's alone

- The engine cannot depend on the files connector's `rooted` (ADR 0047), so the local store
  implements its rules over open directory descriptors (`rustix`): every name is reached with
  `openat` and `O_NOFOLLOW | O_CLOEXEC | O_NOCTTY`, one component at a time beneath the base,
  which is opened as the embedder wrote it and created 0700 where missing.
- The base must belong to the process's user and be writable by no other (`0o022` clear);
  everything beneath it must be the user's alone (`0o077` clear), of the kind expected, and on
  the base's file system. This is checked at every open, listing and read, not at creation
  only, and refused as `wal_not_private`.
- A pipeline's directory is `p.<id>` for an id with no upper-case letter, and `x.<base32 of the
  id>` otherwise: injective, and distinct where a file system folds case.
- A listing refuses, as `wal_stray`, every name the store never writes in a directory it keeps
  logs in: a stray file is never read, and no chunk is listed twice.

### Log format 3: every chunk says whose it is, and the highest says what is needed

- Every chunk begins with a 14-byte preamble, `rdltwal\0`, the format as a u16, and a CRC32C of
  both, so a chunk of another format is refused rather than read as torn. A frame's checksum
  covers its kind and length as well as its payload.
- A writer's chunk holds a header naming its pipeline, load, number, session epoch and what the
  load opened on, then schemas, batches, seals and phases, then a commit or a close, then an
  `End` naming the chunks still needed and the commits received in them. A fence chunk holds a
  `Fence` naming its pipeline, load and number, then an `End`. A receipt is no longer a frame of
  its own: it is durable in the next chunk's `End`.
- A chunk is published whole, so no visible chunk is ever torn and any damage is refused as
  `wal_unreadable`. A scan reads the highest chunk, then exactly the chunks its `End` names, any
  of them missing refused; chunks a crash left between a publish and its deletions are not read.
  A header naming another pipeline or load is refused as `wal_foreign`; a chunk number, epoch or
  opened commit that disagrees, a chunk not ending in one `End` after its commit or close, and a
  frame announcing more than a frame may hold, before it is read, as `wal_unreadable`.
- A frame may hold at most what one request for lowering may take of the memory budget, a
  quarter of it, and batch frames decode within that same limit for rows, lengths and nodes.

### A logged commit applies only whole, and only as the engine writes one

- A seal records how many batch frames and rows its segment logged; a commit frame records how
  many seals and phases it takes. A pending commit missing a seal, a segment of it without one, a
  seal whose frames or rows are missing or doubled, and a commit that drops tables, which the
  engine never logs, are refused.
- Replay refuses a commit whose session is not older than the replaying one, whose state records
  a reset, any receipt but its own, or a partition position no seal of it or phase it begins set.
  A replayed commit never drops a table, and names child tables only where it stages rows again.

### Replay is charged

Replay reserves each frame's bytes before it reads the frame, and what the decoded batch holds
before the frame goes, keeps that charged until the writer it went to flushes, and flushes every
writer once the budget has no room for the next batch, as a lane does. A batch beyond what one
request for lowering may take, logged under more memory than replays it, is refused as
`replay_exceeds_budget`.

### Disk is bounded, and a full disk is retried

- `GrowthLimits::log_bytes`, 4 GiB by default, is what a load's log may hold on disk, the staged
  chunk included. At half of it a commit is due, and again at each eighth more; a batch whose
  frame would pass it fails its write as `log_bytes_exceeded` before its source hears of it.
- A failed write fails every batch after it at once. A full disk or quota is `wal_storage_full`
  and retryable: the next attempt's replay deletes the failed load's log, writing nothing first.

### Receipts have a horizon the engine declares

- `CommitMeta::horizon`, wire field 9, is the oldest commit any replay may still repeat:
  commits order by their load's id, then their sequence, and a destination may forget the
  receipt of every commit before the horizon, of whichever load. With none, it forgets nothing.
- Each commit declares the earliest of: the oldest commit its own load's log still holds, or the
  commit itself where it holds none; and the first commit of every other load of the pipeline
  whose log the store lists. A load whose log the listing misses opened its session after this
  attempt's, so this commit fails as fenced and lets no receipt go. A replayed commit and a reset
  declare no horizon.
- The memory, files and SQLite destinations, the simulation's destination and certification's
  vault forget the receipts before it within the commit that declares it. The files
  destination's rule of sixteen loads is gone. `D-IDEMPOTENT` re-commits a commit that a later
  commit names as its horizon.

### A reset goes by the stream's own name

- A stream is recorded only by state keyed by itself. A stream whose displayed name another
  recorded stream shares is refused as `stream_ambiguous`: their tables cannot be told apart.
- A reset reads the pipeline's logs first, and refuses as `reset_unreplayable` a stream whose
  source cannot read again with rows in a log that no commit has received: a run of the pipeline
  lands them, after which the reset goes ahead.

### The kill matrix draws only what a run reaches

A run that ends before the read or commit drawn to kill it must have loaded every row once; the
draw is then taken again among the reads and commits that run told, so every draw kills a run.

## Rulings

- **No authentication.** A log is bound to its pipeline and load, private, and checked for the
  shape the engine writes, but not signed: whoever can write as the engine's user holds its
  authority already. A session's epoch between a stream's reset and the replaying session's own
  is one an honest log may hold, so a forged one there cannot be told apart.
- **No quarantine.** A log that cannot be read or replayed stops every later run of its pipeline,
  typed, until an operator removes it, as ADR 0029 says: setting it aside would drop rows its
  source was told were committed, without a word.
- **Acknowledgement before the commit stays.** A source that cannot read again is told once the
  commit's chunk is durable, as ADR 0029 and ADR 0034 say; a log lost with its disk loses those
  rows. The log's privacy and durability are what this ruling rests on.
- **Tombstones are not pruned by the horizon.** A tombstone guards a key against a change
  sequenced before its delete, which a source sends again whenever it resumes past it, not only
  a replay: no commit horizon shows one unneeded. ADR 0027's
  acceptance stands.
- **No owed report.** A report owed for a partition recorded done is not kept, in state or in
  memory. No later attempt reads such a partition, and a served source accepts a report only of
  a position its connection sent or started a read from (ADR 0044): a connector process started
  since would refuse the owed report for ever, failing runs whose rows all landed once. A
  durable entry would also need a commit that publishes nothing to clear it. ADR 0044's residual
  stands: no row is lost or doubled, and the position that trails is the refusing source's own.
- **No check of a staged batch's schema against its table's.** A logged batch holds the columns
  its lane wrote, which the table's schema does not describe one for one, so the comparison
  refused honest logs; a batch the destination cannot take fails its write as a live one does.
- **An `End` frame is not charged.** It names chunk numbers and commit sequences, and a scan
  reads it within the frame limit like any other frame.

## Consequences

- A log's format changed: a log written before this is refused as `wal_unreadable`, and an
  operator removes it after landing its rows by other means. rdlt is not released, so no
  migration is kept.
- A replay of a log whose load still runs is fenced and its load fails, rather than both
  writing.
- Each commit lists the pipeline's logs once more, to declare its horizon.
- A destination that predates the horizon keeps every receipt, as before.

ADR 0029 (Store, Writer, Claims, Replay, Damage), ADR 0033 (refusals), ADR 0006, ADR 0047 and
ADR 0027 are amended by this one.
