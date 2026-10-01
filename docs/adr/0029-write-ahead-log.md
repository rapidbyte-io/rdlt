# ADR 0029: The write-ahead log

Status: accepted, 2026-09-30.

## Context

Spec §15.6 makes a load keep a write-ahead log when a stream's source cannot read again what it
acknowledged, as a message queue that forgets what its consumer committed cannot: the engine
acknowledges such a source before the destination commits, so the log is the only copy of those
rows until the commit lands. Until M5c the engine had no log, and a source had to be replayable.

The owner ruled (2026-09-28) that logs are kept locally only: in the worst case a pipeline reads
its data from the source again, so an object store backend is not built.

## Decision

- **When.** A load keeps a log where its pipeline asks for one (`PipelinePlan::with_wal`) or a
  selected stream's source declares `replayable: false`; the log's store comes from the
  environment (`Env::wal`, `SystemEnv::with_wal`). Without a store, a stream that cannot read again
  is refused as `wal_required`, and a pipeline asking for a log as `wal_store_missing`. Such a
  stream is read incrementally: a full read starts again from the beginning, which its source no
  longer holds (`full_read_unreplayable`), and a change stream's phases drop the positions replay
  goes by (`change_read_unreplayable`; M5d's streaming sources take this up). Amended 2026-09-30:
  ADR 0034 logs phase transitions and lifts `change_read_unreplayable`, so such a stream is read
  incrementally or as changes.
- **Source contract.** A source that cannot read again serves each row to at most one reader: two
  loads of one pipeline must not both be served, and acknowledged for, the same rows, which no
  log could tell apart (`StreamSpec::with_replayable` says so).
- **Store.** `WalStore` keeps each load's log as numbered chunks, and `LocalWal` keeps them in a
  local directory: `<base>/<sanitized pipeline>-<hash8>/<load>/<chunk:08>.wal`, directories created
  0700 and refused where another user owns them. The embedder chooses the base.
- **Frames** are spec §15.6's, `[kind u8][len u32 LE][crc32c u32 LE][payload]`: metadata as JSON,
  a batch as a JSON header then its Arrow data in the wire's framing (`rdlt_wire::codec`), whose
  decoder checks each message against its body and contains Arrow's panics. A raw Arrow reader
  panicked on data a garbled log held (the fuzz target found it), so the engine depends on
  `rdlt-wire` for its codec, which every binary shipping the engine links already. Beyond the
  spec:
  - the header names the last commit the destination had received when the load opened;
  - a schema frame holds the table's reference and schema, which replay creates the table from;
  - a seal frame names where the destination held its partition just before the segment's
    commit, as well as where the segment leaves it;
  - a commit frame holds the whole `CommitMeta`, state changes included, so a replay commits
    what the load meant to;
  - a `closed` frame ends a log whose load stopped appending.
- **Writer.** One task per load appends frames in the order they are sent, through a bounded
  channel; each batch frame's bytes are charged to the memory budget until appended.
  (ADR 0039 charges seal and commit frames too, and writes a state value as base64 text.) A commit's
  frame is appended, made durable, then answered, and the log moves to a new chunk; a chunk goes
  once every segment and commit in it has a receipt. Every chunk starts with the header and the
  schema frames its batches name, so it reads alone. After a failed append or sync every later
  command fails: what the chunk holds is unknown.
- **Order.** A batch's frame is queued before its partition can seal its segment, and a commit's
  frame is queued after the seals it takes; the channel keeps their order, so a commit's frame
  follows every batch of its segments. A test runs a whole load on a slow disk and checks it.
- **Acknowledgement.** A source that cannot read again is acknowledged once the commit frame
  covering its segments is durable, before the destination commits; the others after the receipt,
  as before.
- **Claims.** A log has one claimant at a time: its load, which claims it before anything of it
  exists and holds it while writing, then whoever replays it once that load is gone.
  `LocalWal` locks a file beside the load's directory (`std::fs::File::try_lock`, which a process's
  death releases). The lock is advisory, and unreliable on network filesystems: the directory is a
  local one.
- **Replay.** Before each attempt opens, in a session of its own, every log the attempt can claim
  is read (one frame in memory at a time, each chunk up to its first torn frame), and each commit
  without a receipt is committed again under its original `(load_id, commit_seq)`:
  - where the destination stands exactly where the load left it (its last receipt is the load's
    previous commit, or what the load opened on), the whole commit applies;
  - where a newer load committed since, only seals of streams that cannot read again apply, each
    where its partition stands where the seal says it started: elsewhere, the newer load committed
    that partition, or the commit itself landed and only its receipt was lost, and the destination
    answers the commit's idempotence key with its stored receipt. A stream that reads again is left
    to the next load: its positions cannot tell whether its segments landed, as a completed full
    read leaves every partition without one. Positions of a stream that cannot read again only move
    forward, since its full reads are refused, but for a change stream's phase transitions, which
    ADR 0034 logs and replays (amended 2026-09-30);
  - the staged segments' batch frames are written again through writers of the logged tables,
    created first, and the log is removed.
- **The simulation** keeps logs in a store of the world that outlives runs; a run's crash keeps
  what its worker's logs made durable and a drawn part of the rest, torn or garbled, and leaves
  other pipelines' logs to their own workers (seed 7331 found a crash tearing another's live log). A swarm feature, drawn apart
  from the seed's generator, has the pipelines log and every other incremental stream forget what it
  acknowledged: a read from before it fails, retryably, until the rows land. A phase converges only
  once no log is left to replay, and then every offset such a stream acknowledged is committed.
  Seed 8746 found the phase ending with a failed load's log of a new full read, which the next
  phase replayed as the rows the load had read. So the simulation never replays a log across a
  phase's end, where the source's rows change. With replay disabled, seed 61
  fails: its rows were only in the log.
- **Fuzzing.** `wal_log` reads any bytes, and valid logs cut and garbled, their checksums
  rewritten or not, as replay does.
- **Damage.** Only a log's last chunk can end torn: every other ends with a commit's frame, made
  durable, so a chunk before the last that ends early or garbled makes the log unreadable rather
  than losing the commits past the damage. A load that claimed its log and failed before its first
  frame leaves only the claim's mark, which replay lists and removes.

## Deviations from spec §15.6

- **No object store backend** (owner's ruling above).
- **Acknowledged after the commit frame, not the seal frame.** Seals are logged with their commit,
  in one durable write, so a seal frame is never durable alone; sealed segments in no commit frame
  are never replayed. Their source was never acknowledged, so it serves them again.
- **Replay precedes the attempt's open**, in a session of its own, so the attempt plans from state
  that includes the replayed commits. Both opens discard unpublished staging.
- **A partition moved by a newer load is left to it** (the seal rule above): a fenced load's frame
  must not publish rows the load that fenced it committed.
- **A crashed load's log is replayed by the next attempt that can claim it**, not only by its own
  load; logs are chunked, and committed chunks removed as the load goes, so a continuous run's log
  stays small.

## Consequences

- A pipeline can load a source that forgets what it acknowledged, exactly once through crashes,
  failed commits and fenced workers, as long as its logs' directory survives.
- A commit frame the destination refuses on replay fails every later run until an operator
  removes the log: the price of acknowledging before the commit.
- A load that keeps a log writes each batch twice, once to the log, and makes one sync per commit.
- `S-ACK`, a certification clause checking that a source advances only once acknowledged, and its
  probe of what a source acknowledged, move to M5c2 (ADR 0030).
