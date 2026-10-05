# ADR 0045: A write-ahead log that is bound, private, checked and charged

Status: accepted, 2026-10-03. Amended 2026-10-04 (ADR 0051): `ObjectStoreWal` keeps logs in S3
and stores that answer as it does, under this contract; a batch the log cannot hold waits for a
commit to free room, and what a store stages in memory is charged to the budget.

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

- A load opens its log before anything else of its attempt, replay included, by creating an
  object where none of its name exists (`open_log`); the log is open until its removal begins,
  and never again. `WalStore` stages a chunk of an open log, appends to it, and publishes it
  whole where no chunk of its name exists and the log is still open, which is its durability
  point: a name taken is refused with `AlreadyExists`, a log removed with `NotFound`, and a
  chunk published as its log is removed is never listed. A staging can be discarded, and a
  log's stagings deleted by anyone (`remove_staged`), which needs no room. It lists a pipeline's
  open logs, the leftovers of removals a crash interrupted, a load's chunks; reads a chunk by
  range; deletes a chunk; and removes a log, closing it first, then listing and deleting what it
  holds. A publish creates the chunk where its name is free first and asks whether the log is
  open after, deleting the chunk where it is not: asked first, a removal could close and list
  the log in between. Nothing is appended to a published chunk, renamed over another, or locked,
  and nothing relies on a directory's order or on an operation over several objects.
- A store has an identity, written once by the first to ask (`identity`) and kept with it.
- `LocalWal` marks a load's log open with a file `open` in the load's directory, removed, and
  made durable, first when the log is removed. It opens a log in a directory `.<load>.opening`,
  which no listing reads as a log, with its mark in it, then renames it to the load's name with
  `RENAME_NOREPLACE`, so a load's directory is never seen without its mark; one a crash left
  half opened is a leftover. Its identity is a file `store` in the base, written whole and linked
  in where no file of the name exists. A relative base is taken against the working directory
  once, as the store is made. It stages a chunk as a file
  `{chunk:08}.{token:016x}.part`, created exclusive and 0600; publishing syncs it, links it to
  `{chunk:08}.wal` with `linkat`, which fails where that name exists, checks the name is the file
  it staged (device and inode) and the log still open, unlinking it again otherwise, unlinks the
  staged name and syncs the directory. Each deletion syncs the directory it removed from.
- One durable write per commit: what was logged since the previous commit, the commit's frame
  and an `End` frame go into the chunk published at that commit.
- Claims are gone. A replay takes a log over by deleting what its load staged, then publishing
  a fence chunk at the number after its highest, a log that needs nothing included: a load still
  running then finds that name taken while the log is open, and the log gone once the replay
  removes it, and fails as `wal_fenced` before it answers its commit, whatever was removed. A
  replay that finds the next number taken eight times fails its attempt as `wal_running`,
  retryably, rather than open a session that fences a load whose log takes rows on. Because a
  load opens its log before it lists the others, of two attempts starting at once at least one
  lists, and fences, the other's log. A take, a release or a scan that finds the log closed
  since it was listed leaves it to the replay that took it; a fence whose staging a rival
  deleted is tried again; an open a rival's replay removed is `wal_running`. So every race
  between attempts ends in a typed, retryable code, or in `wal_fenced` for a load that lost.
- One store per pipeline and destination: the first commit of a pipeline at a destination that
  names no store records its store's identity (`StateEntry::LogStore`), and an attempt, a
  replay or a reset whose store is another is refused, `wal_store_other`, before it reads or
  tells a source anything; a logged commit may name only its own store. Logs another store
  holds are never replayed against a destination they did not log for, and a load never reads
  past rows they hold.
- A load that logs nothing removes the log it opened, and so does an attempt that fails having
  published nothing. Leftovers of a removal are removed before anything else of a replay.
- A conformance suite, generic over `WalStore`, holds the memory and local stores to this
  contract, and the simulation's store loses what was staged and not published at a crash.

### Every directory and file of a local log is its user's alone

- The engine cannot depend on the files connector's `rooted` (ADR 0047), so the local store
  implements its rules over open directory descriptors (`rustix`): every name is reached with
  `openat` and `O_NOFOLLOW | O_CLOEXEC | O_NOCTTY` from the descriptor of the directory it is in,
  and every owner, mode and kind check is made on the descriptor then used.
- The base is reached once, the first time the store is used, from the root one directory at a
  time, each opened without following a link from the directory checked before it and checked
  on its own descriptor: it must belong to the user or to root and be writable by no other
  unless it is sticky; in a directory others may write, a link is followed only where it is the
  user's or root's, as the kernel's rule for such directories has it. A link is read only out of
  a directory that passed and its target walked
  the same way, at most forty links; a missing directory is created there, 0700, and made
  durable. The directories the base lies in are then walked up from it by descriptor and
  checked the same way. The base must belong to the user and be writable by no other (`0o022`
  clear). The store keeps the base open: each call checks it is still linked and still the
  user's alone, and a base removed is refused, never made again.
- Everything beneath the base must be the user's alone (`0o077` clear), of the kind expected,
  and on the base's file system, checked at every open, listing and read, and refused as
  `wal_not_private`.
- A pipeline's directory is `p.<id>` for an id with no upper-case letter, and `x.<base32 of the
  id>` otherwise: injective, and distinct where a file system folds case.
- A listing refuses, as `wal_stray`, every name the store never writes in a directory it keeps
  logs in: a stray file is never read, and no chunk is listed twice. A name that begins with a
  dot, which the store never writes, as NFS and file browsers make, is passed over, never read,
  and removed with the log where the system lets it.

### Log format 4: every chunk says whose it is, and the highest says what is needed

- Every chunk begins with a 14-byte preamble, `rdltwal\0`, the format as a u16, and a CRC32C of
  both, so a chunk of another format is refused rather than read as torn. A frame's checksum
  covers its kind and length as well as its payload.
- A writer's chunk holds a header naming its pipeline, load, number, session epoch, what the
  load opened on and the destination it is written for, then schemas, batches, seals and
  phases, then a commit or a close, then an `End` naming the chunks still needed and the commits
  received in them. A fence chunk holds a `Fence` naming its pipeline, load and number, then an
  `End`. A receipt is no longer a frame of its own: it is durable in the next chunk's `End`.
- A chunk is published whole, so no visible chunk is ever torn and any damage is refused as
  `wal_unreadable`. A scan reads the highest chunk, then exactly the chunks its `End` names, any
  of them missing refused; chunks a crash left between a publish and its deletions are not read.
  A header naming another pipeline or load is refused as `wal_foreign`; a chunk number, epoch,
  opened commit or destination that disagrees, a chunk not ending in one `End` after its commit
  or close, and a frame announcing more than a frame may hold, before it is read, as
  `wal_unreadable`.
- A frame may hold at most what one request for lowering may take of the memory budget, a
  quarter of it, and batch frames decode within that same limit for rows, lengths and nodes. A
  scan reads a batch frame 64 KiB at a time, summing its checksum, and keeps only its header.

### A log replays only into its destination, only whole, and only as the engine writes one

- A destination's state records the first load whose commit reached the pipeline there
  (`StateEntry::Origin`), which that commit records; a log's header names it, or the load itself
  where no load had committed there when it opened. Replay refuses, as `wal_foreign`, a log that
  names another, and a logged commit that records any load but its own as the first.
- A seal records how many batch frames and rows its segment logged; a commit frame records how
  many seals and phases it takes. A pending commit missing a seal, a segment of it without one, a
  seal whose frames or rows are missing or doubled, and a commit that drops tables, which the
  engine never logs, are refused.
- Replay refuses a commit whose session is not older than the replaying one, whose state records
  a reset, any receipt but its own, or a partition position no seal of it or phase it begins set.
  A replayed commit never drops a table, and names child tables only where it stages rows again.
- A replay that fails, wherever it fails, keeps every chunk of the log it read: nothing is
  released or removed before every pending commit is committed again, and what it learned of
  the destination is dropped with it.

### Replay is charged

Replay reserves each frame's bytes before it reads the frame, and before it decodes the frame's
batch what the decoder will allocate for its buffers, which the wire's decoder measures without
making it (`Decoder::held`); what else the batch holds is reserved once it is decoded. What a
batch holds stays charged until the writer it went to flushes, and every writer flushes once the
budget has no room for the next batch, as a lane does. A batch beyond what one request for
lowering may take, logged under more memory than replays it, is refused as
`replay_exceeds_budget`.

### Disk is bounded, and a full disk is retried

- `GrowthLimits::log_bytes`, 4 GiB by default, is what a load's log may hold on disk, the staged
  chunk included. At half of it a commit is due, and again at each eighth more; a carry of an
  open segment's frames that would pass it is left undone, the old chunks kept.
- A batch is counted while the log holds it beside room for a carry of what one chunk pins:
  the frames of open segments in a chunk that holds another segment's frames too, which a carry
  copies before that chunk can go. A batch that finds no room first has the writer publish its
  chunk between commits, closed by a relief frame where a commit's chunk has its commit: the
  chunks whose receipts arrived go, and a chunk holding committed frames beside open ones goes
  once its open frames are carried, one chunk at a time as the room allows. Then, while a commit
  can free room, as a checkpoint sealed that no commit took or a commit under way can, the batch
  waits, and a commit is due at once. Where none can, the batch takes the room left without the
  carry's, as it may be what brings its partition's checkpoint, and is refused,
  `log_bytes_exceeded`, before its source hears of it, only where even that is too little: the
  log then holds the frames of segments its partitions have not sealed, the chunks of commits
  waiting for receipts, and those chunks' own frames. A load whose partitions' unsealed frames
  fit the bound beside a chunk's header, end, seal and commit frames loads through it. A wait
  ends with the attempt's deadlines, and the frame it holds stays reserved from the memory
  budget.
- Batch frames never take the log past the bound, each counted by one compare and swap. The
  frames a commit cannot do without, a chunk's header and end and the schema, seal, phase,
  commit and relief frames, are counted as they are written, and may pass it by what they take.
- What a store stages in memory beside the log, a part for `ObjectStoreWal`
  (`WalStore::staging_bytes`), is reserved from the budget's share for logs when a load's log
  starts; a store staging more than half that share is refused (`wal_staging_exceeds_budget`).
- A failed write fails every batch after it at once and discards what its chunk staged. A full
  disk or quota is `wal_storage_full` and retryable. Where opening its own log finds the disk
  full, the next attempt removes what removals a crash cut short left and deletes what every load
  of the pipeline staged and did not publish, which needs no room, then opens it once more; its
  replay deletes a log's stagings before it fences it. A retry needs room for its log's
  directory and its fences alone, a block and a few hundred bytes.

### Receipts have a horizon the engine declares

- `CommitMeta::horizon`, wire field 9, is the oldest commit any replay may still repeat:
  commits order by their load's id, then their sequence, and a destination may forget the
  receipt of every commit before the horizon, of whichever load. With none, it forgets nothing.
- Each commit declares the earliest of: the oldest commit of its own load a replay of its log
  may repeat, one waiting for its receipt or whose receipt no published chunk records yet, or
  the commit itself where there is none; and the first commit of every other load of the
  pipeline whose log is open. An open log of another load exists only while that load runs, or
  until the next attempt's replay takes it, so the horizon moves on as loads end; a segment kept
  open for long holds back no commit whose receipt is recorded. A load whose log the listing
  misses opened its session after this attempt's, so this commit fails as fenced and lets no
  receipt go. A replayed commit and a reset declare no horizon.
- The memory, files and SQLite destinations, the simulation's destination and certification's
  vault forget the receipts before it within the commit that declares it. The files
  destination's rule of sixteen loads is gone. `D-IDEMPOTENT` re-commits a commit that a later
  commit names as its horizon.

### A reset goes by the stream's own name

- A stream is recorded only by state keyed by itself. A stream whose displayed name another
  recorded stream shares is refused as `stream_ambiguous`: their tables cannot be told apart.
- A reset opens its session, which fences every load opened before it, then takes every log of
  the pipeline over as a replay does, so no load logs rows after it reads them, and refuses as
  `reset_unreplayable` a stream whose source cannot read again with rows in a log that no commit
  has received, or as `wal_running` while a load keeps publishing: a run of the pipeline lands
  the rows, after which the reset goes ahead.

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
  a replay: no commit horizon shows one unneeded. ADR 0027's acceptance stands.
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
- **A seal moved past by a newer load is still left to it.** Replay keeps ADR 0029's rule: a
  seal of a stream that cannot read again applies only where its partition stands where the seal
  started. A fenced load can no longer publish once fenced, and every attempt replays the logs it
  lists before it reads, so a commit its source was told of is replayed before any newer load of
  the pipeline reads, unless the newer load could not fence its log, in which case it fails as
  `wal_running` instead of reading. It rests on every load of a pipeline and destination keeping
  its log in one store, which the store binding holds them to.
- **Two destinations that no commit of the pipeline ever reached cannot be told apart.** A log
  whose load opened on a destination with no origin names itself, and so may be replayed into
  another destination no commit reached either; once any commit lands, the destination is named.
- **The store binding begins with the first commit.** A destination no commit of the pipeline
  reached names no store, so two stores' first loads may each begin; the first commit to land
  names its store, and the other's next attempt is refused. Moving a pipeline's logs keeps its
  store's identity file with them.
- **A full log frees what it can before it waits or refuses.** A source that checkpoints,
  sending more than the log holds between commits, loaded until it failed `log_bytes_exceeded`,
  which no retry mends; so did several partitions whose segments each spanned chunks the others
  kept, and a partition whose checkpoints lay past half the bound apart. A batch now has the
  log publish a chunk between commits, which records the receipts that arrived and carries open
  frames out of chunks it then deletes, waits for a commit where one can free room, and is
  refused only where the frames of unsealed segments leave it none. Cost: a full log publishes
  chunks between commits, a write each, and copies open frames out of the chunks it frees; while
  a commit can free room, a batch keeps room for the largest such copy, so a load of many
  partitions commits more often. A chunk published between commits is closed by a frame an
  engine before this does not know, which it refuses as `wal_unreadable`.
- **A staged file's name is told apart by process id and a counter.** Two processes of different
  process namespaces may take one name once a staging was deleted; a publish compares the file it
  linked with the file it holds, by device and inode, and refuses another's.

## Consequences

- A log's format changed, to 4: a log written before this is refused as `wal_unreadable`, and
  an operator removes it after landing its rows by other means. rdlt is not released, so no
  migration is kept.
- Every attempt of an engine that keeps logs opens one, and removes it where it logs nothing:
  two durable directory changes more an attempt.
- A replay of a log whose load still runs is fenced and its load fails, rather than both
  writing.
- Each commit lists the pipeline's logs once more, to declare its horizon.
- A destination that predates the horizon keeps every receipt, as before.

ADR 0029 (Store, Writer, Claims, Replay, Damage), ADR 0033 (refusals), ADR 0006, ADR 0047 and
ADR 0027 are amended by this one.
