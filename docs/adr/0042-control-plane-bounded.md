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
  carried in the handshake as fields 12 to 14. A handshake and its answer, and a configuration's
  answer, which carries a destination's identifier rules, are bounded by `HANDSHAKE_BYTES`
  (4 MiB). A configuration, a schema change and a read's start are bounded by their field's
  limit and 64 KiB more; only reads and writes take frames. A connector's configuration schema
  is bounded on its own, at 1 MiB (`MAX_CONFIG_SCHEMA_BYTES`).
  - Each end passes a message on to be decoded only once it has arrived whole
    (`rdlt_wire::bounded`), to tonic for a generated call and to the data plane's reader
    (`rdlt_wire::plane`) for a read, a read-back or a write, which decodes it from those bytes:
    within its class's bytes on the wire, refused from its prefix, so no decoder reserves a
    length before its bytes arrive; and, every message, frames among them, counted by a scan of
    its encoding (`rdlt_wire::scan`) within what its class may hold decoded: `Limits::decoded`,
    2 times the wire bound for a frame, 4 for a handshake, a configuration and a read's start, 16
    (`DECODED_PER_BYTE`) for a catalog, a schema change and any other control message: 128 MiB
    for a frame and 64 MiB for a catalog by default. State is bounded on what it holds decoded:
    `state_bytes` is that bound, and its bytes on the wire are bounded by the same. A
    message arriving is held in room that doubles, and goes straight to the message's end once
    within twice what it holds, so a frame at its limit holds less than twice its bytes.
  - The scan walks the encoding by forms `cargo xtask codegen` generates from the `.proto`
    files, each message's size and the kind of each field. It counts each message its size, four
    times for an entry of a repeated field and eight for one of one-byte numbers, as a vector
    first allocates room for four and holds its old entries beside twice as many as it grows;
    each string or bytes its length, or the eight bytes a vector of bytes first allocates; and a
    field the form does not know, groups among them, its bytes. Tests hold this to the decoder:
    for repeated messages, nested single entries, strings and numbers, packed and not, the heap
    peak of decoding stays within the count; a property test and a fuzz target (`scan`) decode
    random encodings as every message the calls carry, and whatever decodes, the scan takes and
    counts no less than its peak. Amended 2026-10-06: the fuzz target found a string set twice,
    which the decoder reads into the room the first took, grown to twice that beside it; a string
    its message, or a message it is decoded into, set before counts twice its length.
  - A message the scan cannot walk, which protocol buffers would not decode either, fails its
    call as `InvalidArgument` and never reaches the decoder; so does a call whose body ends within
    a message.
  - A message beyond either bound fails its call as `OutOfRange`, which the host reports as a
    non-retryable transport failure naming the sizes, before anything decodes it. Measured at the
    class limits, a refused 4 MiB catalog of empty entries holds 9 MiB at its peak, a refused
    16 MiB plan or open answer 27 MiB, a served commit of empty child tables 21 MiB, each within
    its bytes and its decoded bound together, and a 64 MiB frame, read or written, of empty path
    segments 103 MiB, within the 128 MiB of its bound.
  - On a served connection, the requests still arriving hold at most four of its largest
    messages together. A request finding no room is not refused: its body is not read until
    room comes back, so HTTP/2 flow control holds its sender on that stream alone, the
    connection's window holding every call's stream window and one more (ADR 0016). Room is
    taken as a message's prefix arrives and given back as the whole message is passed on, and
    the window serves bodies in turn, so a commit behind writes holding the window gets room as
    soon as their messages arrive; a body waiting holds none, and none waits on another body to
    finish its message.
  - A served connection holds at most 200 open calls (`MAX_CALLS`), set explicitly.
  - A served connector takes the state one request may carry from `--max-state-bytes`, spawned or
    listening; a host spawning a connector passes its own `state_bytes` where it raises the
    protocol's, so raising the host's limit raises both ends', and a host whose budget takes less
    still sends as much. Either end's decoder and encoder are set to the largest of any class, so
    a state limit above a frame's takes effect.
- **What decoding a remote connector's answers holds is charged to the run's memory budget
  before it is decoded** (ADR 0039). Every call of a run or a reset into a connector, reads too,
  is made within a charge (`rdlt_wire::bounded::charging`); the host's transport charges each
  message of its answer at what the scan counts before it is decoded, and holds the charge
  until it is decoded: the data plane's reader holds a read's frame's until the read has decoded
  it, which releases it before its event waits for room, and a write's answer's until the writer
  has decoded it; a generated call's ends with its next message or the call.
  - A read's frame is charged to what pushes may take, which the batch it carries is admitted to
    next: a frame decodes into twice its bytes at most, 8.5 MB at the least memory, where a read's
    part of the reads' share is 516 KiB, so the reads' share could not hold one; the row bound
    already keeps a frame within half of what pushes may take.
  - Every other answer is charged to a share of its own, a sixteenth of the budget, which waits
    for no push. The catalog, state and control message limits a connector is told are what
    that share holds of one answer decoded: a catalog and a control message at sixteen times
    their bytes, and state, bounded decoded, the whole share; so a connector that keeps to them
    is refused nothing: 1 MiB, 16 MiB and 256 KiB at the default budget, 129 KiB, 2 MiB and
    129 KiB at the least.
  - An open's answer is the engine's own committed state echoed back, so the engine keeps it
    within the bound: a commit after which the state would hold more decoded than the state
    limit, as the scan of an open's answer carrying it counts it, is refused before it is logged
    or sent, with `state_bytes_exceeded`. A pipeline can always open what it committed. Amended
    2026-10-03 (ADR 0041): this check and the stored state's are one, measured decoded.
  - A charge waits as any request of the budget does, in turn, and fails its call at the
    budget's deadline; one larger than its share fails it at once. No charge is held while
    another is waited for, and none while its holder waits for anything but the answer's bytes.
  - A served connector has no engine budget: its window bounds what it holds of requests
    arriving, and HTTP/2's stream windows what waits for them in transport, a stream window a
    call. A write holds its staged bound and one decoded frame waiting for its writer, beside
    the frame it received next, which decodes once the writer takes the frame before it.
- **Lists are bounded and checked in linear time.** A catalog holds at most 65,536 streams and a
  plan 16,384 partitions, in every placement; a plan names each partition once and starts only
  those it names. A destination's identifier rules hold at most 4,096 reserved words and 64
  reserved prefixes of 256 bytes each, and an identifier at least 16 bytes long. Naming folds the
  rules once; the coordinator tracks partitions by id and counts what has ended. An attempt reads
  no more than 16,384 partitions at once, whatever the streams it selects and the phases it
  begins.
- **Names are what a reader sees.** Stream names, namespaces, partition ids and table path
  segments refuse every character that shown text escapes (ADR 0037's classifier); naming
  replaces them for a destination that takes any character. Every destination identifier the
  wire carries is non-empty, within 65,535 bytes and free of them, and a table's schema version
  is at least one. Column paths are source data and stay as they are. A table's name, its hash
  appended, never falls under a prefix the destination reserves: one that would is passed over
  and the name escaped once more, the same way every time.
- **Every wait on a connector has a deadline.**
  - The engine ends every call into a source or a destination but a read at
    `EngineConfig::connector_wait` (30 minutes), in every placement.
  - A read asked to stop is dropped after `EngineConfig::stop_wait` (60 s).
  - A remote write's whole wait for credit, a flush's stats or an ending write's error is bounded
    by the write-ack deadline, however many answers arrive; a credit of no bytes is refused.
  - A served connection whose writes make no progress for its send wait (60 s) is closed, and a
    stopping connector's drain ends at its drain wait (30 minutes), both `ListenLimits` fields.
  - A failed attempt, and a failed replay, close the session they opened, waiting for the close
    no longer than `EngineConfig::close_wait` (60 s).
- **What a connector says cannot silently change what was committed.**
  - A receipt must answer its own commit, by load and sequence, or the commit fails as
    `receipt_mismatch`; receipt counters are summed with checked arithmetic.
  - A table's state records the merge key and change time it was loaded by; a keyed write by
    another key or change time, or by an empty catalog key, is refused until the table is reset.
  - Opened state holds each key once; a recorded name map is injective; a recorded table name
    must be one the destination's rules could have given, under no reserved prefix and no other
    table's. An epoch never passes its largest value. State written before a table recorded its
    key and change time is refused as `state_invalid`, naming the missing field: nothing
    published keeps such state.
  - A reset is how a pipeline recovers, so it checks no recorded name: it forgets what it resets,
    and drops a table only under a name the rules admit, never one under a reserved prefix.
- **A connector's failure steers retries only as far as the engine allows.** Only a rate limit
  keeps the wait it asks for, held between the retry policy's first and longest delays. A fence
  is a destination session's alone, and a stop ends a read cleanly only where the engine stopped
  it; otherwise each is the side's failure. A retention reset whose read loses its place again
  before sealing a row fails the attempt as retryable, so backoff and attempts bound it.
- **The commit policy is due by rows a commit can take.** Rows of a partition that seals when a
  barrier asks count until they are sealed, as only a barrier, raised by rows that are due, seals
  them; but those a partition held when it did not answer a barrier within `barrier_wait` count
  no more, so it is asked again only once it has written as much again, and a partition that
  never answers makes no later event wait a barrier out. A barrier that stops waiting because a
  cursor waits for room asks again those still owing: their answer may be that cursor. Rows of a partition that seals on its
  own count until a commit passes them by, and again once sealed, so a partition that
  checkpoints only at its end keeps no commit due.
- **Machine strings and the protocol are the host's to check.** A code a connector's error
  carries must be `[a-z0-9_.-]`, within the code limit, and none of the host's own codes
  (`connector_lost`, `deadline_exceeded`, `tls`, `transport`); otherwise it is `invalid_code`.
  The handshake answer carries the protocol the connector speaks, and the host refuses another
  major, a spec whose id does not parse or whose version is not one, and a feature it did not
  offer. Status details that do not decode as base64 are dropped by the host's transport before
  tonic reads them.
- **A read's frames the engine takes for nothing are bounded by count** between two events of its
  data, rows or a checkpoint: schemas and dictionaries to a schema and a dictionary for each
  column, and log lines, metrics, lags and replans to 1,024 (`MAX_FREE_FRAMES`), neither kind
  ending the other's run. A read's data credit stays unchecked by the host: it grants credit back
  as the engine takes each frame, so an overrun is undetectable at the receiver (ADR 0016).
- **A served connection's send wait counts any byte as progress**: a host taking a byte a minute
  keeps its connection, holding only its own share, and a stop's drain ends it.

## Consequences

- A catalog over 4 MiB, state over 16 MiB or a plan of more than 16,384 partitions is refused
  until the operator raises the host's limits (`Options::limits`) and, for a listening
  connector, its `--max-state-bytes`, or the source plans fewer. A message within its bytes may
  still be refused for what it decodes to, when its fields are far smaller than any a connector
  sends.
- A host's decoded bounds are what one message may hold before it is decoded, and a run charges
  what each holds to its budget. A run whose commit would leave more state than the state limit
  fails with `state_bytes_exceeded` until the memory is raised or its state shrinks; nothing of
  that commit lands. The handshake, a configuration's answer and calls made outside a run are
  not charged.
- An engine reading one partition at once needs 53.7 MB rather than 33.8 MB, and pushes may take
  a sixteenth of the budget less.
- A pipeline whose stream, partition or recorded identifier holds a hidden or reordering
  character, or whose recorded table name falls under a prefix its destination reserves, must be
  renamed or reset.
- A destination's identifier rules beyond their limits, or with a maximum length under 16, are
  refused at the handshake and before any run or reset.
- A connector built before the protocol's version was answered is refused; none is published.
- A failed attempt's close waits at most the close wait, a minute by default, before the retry
  begins, for a destination that never answers it.
- H2b's state limits must fit within `state_bytes`, and its dictionary limit takes field 11.
