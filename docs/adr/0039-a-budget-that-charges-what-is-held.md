# ADR 0039: A budget that reserves what is held and is never passed

Status: accepted, 2026-10-01.

## Context

The memory budget charged a push by an estimate of its decoded rows (ADR 0024). The estimate
was one idea with four failures:

- It undercounted what the engine materializes: views and list views naming the same bytes or
  items many times, a null dictionary key that still takes its value's slot, a dictionary or run
  multiplying a value that takes no bytes, a column typed null widened to its table column's
  type, and the names of a struct's fields written into every row's JSON text.
- It ignored what a batch keeps alive: a few rows sliced from a frame, a file block or a
  dictionary were charged for the rows.
- Computing it allocated a vector a value.
- Much of what the engine holds was never charged: the decoder's dictionaries, the cursors of
  checkpoints waiting as seals, signals queued while a commit is in flight, the ranges of a JSON
  push's records, the copies row identity made, the log's seal and commit frames, and what a
  served destination stages.

ADR 0037 has the engine survive any connector: everything a connector sends is charged to a
budget before it is held, or bounded by a limit with a typed refusal.

## Decision

- **The budget is divided into shares, and no share is ever passed.** The engine reserves bytes
  of its memory budget for everything it holds of what connectors send. A reservation is made
  only where it fits its share; none is cut down to fit, none is made without asking, and none
  grows.

  | Share | Of the budget | At the default 256 MiB | Holds |
  |---|---|---|---|
  | cursors | 1/64 | 4 MiB | the cursors of seals waiting for a commit |
  | log | 1/16 | 16 MiB | seal and commit frames from before they are encoded until they are appended |
  | reads | 1/4 | 64 MiB | what reads keep beside their events: a decoder's schema and dictionaries |
  | data | the rest, 43/64 | 172 MiB | pushes waiting to be lowered, and what lowering makes of them |

  - Within the data, one request for lowering takes a quarter of the budget at most, and
    pushes never take the last quarter: a request for lowering always fits once the pieces
    before it are written.
  - **A request that could never fit is refused, typed.** A push that keeps alive more than
    pushes may take fails its read with `push_exceeds_budget`; one row that takes more to lower
    than a request may fails its write with `row_exceeds_budget`; a cursor beyond the cursors'
    share and what a read keeps beyond its part fail the read with `limit_exceeded`, naming
    `cursor bytes` and `read kept bytes`; a seal's or commit's frame beyond the log's share
    fails the commit with `log_frame_exceeds_budget`.
  - **Control never waits behind data.** A checkpoint's cursor is reserved from the cursors'
    share, which no push can use. A commit is due once the waiting cursors take half of it, and
    as soon as a cursor waits for room in it.
  - **A read keeps a bounded part.** Each read may keep the reads' share divided by the
    partitions read at once, 4 MiB at the defaults, so all reads together never pass the share.
    A read that would keep more fails at the frame that would pass it. Sixteen reads keeping
    all they may leave pushes and checkpoints flowing, which tests hold on the ledger, through
    the engine's admission and through a host's read of a served source.
  - **No hold and wait.** A task waits on the budget only while it holds nothing that only
    its own progress releases. A partition waits for lowering's bytes while it holds pushes,
    and nothing waits for pushes while it holds anything; whoever holds what lowering reserved,
    a piece on the compute pool, on its lane or in the log's writer, needs no budget to release
    it. A partition lowers and hands to its lane whatever it reserved before it waits for more.
  - Requests of one share are admitted in arrival order, lowering before pushes.
  - The shares are limits of the engine, with the names above, in `limits.rs`.
- **One cost model** (`rdlt_connector::cost`). A batch has two measures.
  - **Held**: the bytes of every allocation the batch reaches, validity, data, child and
    dictionary buffers alike, each counted once however many arrays or slices share it. A slice
    costs the buffer it was cut from; a frame's batch costs the allocation the decoder made for
    it and the dictionaries its keys use.
  - **Expanded**: what its rows become in whatever of the engine does most with them.
    - A dictionary or run-end row is the value it names; a null key is a slot of the value type,
      whatever the dictionary holds.
    - A view is the bytes it names, and a list view the items it names, each time it names them.
    - A value inside a nested value is its JSON text: every field's name in every row, strings
      with their escapes, bytes as hex.
    - A scalar the destination does not store as it is, is its text. The destination's
      capabilities say which, so a column stored as it is costs its Arrow bytes.
    - A fixed-width value is its width, nulls included.
  - Every Arrow type has a cost: the match over types has no default.
  - A batch holds its schema too: each field, nested ones too, with its name, its metadata and
    its time zone, counted once for the batches that share it. A decoder returns the schema it
    holds for a schema message the same, byte for byte, as the one that schema came from, so the
    batches of a sender that sends its schema before each keep one schema alive between them.
    Schemas that only compare equal are not shared: they may name their dictionaries by other
    ids.
- **A push is admitted for what it holds, and lowered for what it becomes.** A push of Arrow
  batches reserves what it keeps alive when it is admitted, before its table is known. Once
  its plan is found the cost model takes, for each column, the type of the table's column and
  whether the destination stores it as text, and measures what lowering holds at once, which
  each piece reserves before it is lowered.
  - A value costs itself decoded, itself converted where the column's type differs, and its
    text where the column is stored as text: a byte of an 8-bit integer is 33 in a column of
    256-bit decimals.
  - A column of JSON holds each value decoded and its JSON text beside, a string with its
    escapes counted as they are, not at their worst.
  - A nested column converts field by field and item by item; a field the table's struct has
    and the batch's lacks costs a null a row. Stored as text it costs its JSON text beside.
  - A conversion between dates, times and instants costs four slots a value: it holds each as
    an optional 64-bit integer twice beside its result.
  - Every row costs the nulls of the table's columns the batch holds nothing in.
  - The engine's text builders reserve what the model says the text takes at most, so they
    never grow beyond what was charged.
  - The invariant is a test here too: every numeric, temporal, decimal, text, bytes and nested
    type, plain, behind keys and as runs, is lowered into every type its table may hold it in,
    as it is and as text, and the heap's peak is held to the charge.
- **What is lowered at once is what was reserved.** A unit's plan is found first, and the unit
  is then cut by that cost, a piece at a time, on the compute pool.
  - A piece is at most a sixteenth of the budget, or one row where a row takes more, up to the
    quarter a request may take. Where the load keeps a log a piece is half as large, and
    reserves as much again for its frame there.
  - Each piece reserves its cost before it is lowered. A partition waits for one piece's bytes
    only while it holds no piece it has not handed to its lane; beside that piece it lowers as
    many more as the budget has room for at once, eight at most. How many pieces are lowered
    at once follows from what the budget admits.
  - Once lowered a piece holds what it kept alive that its unit did not, in place of what was
    reserved, and its frame what the frame takes. The unit's own reservation stays with its
    last piece until that is written.
  - **Normalized streams follow the same rule.** A unit's tables are known only once it is
    split, so it is measured twice. It is first cut, as it arrives, into pieces whose split makes
    no more than a piece: each row with its lineage, each item an array holds with its own, for
    the two copies a split may hold. Each such piece then asks the budget once for all a
    request may take, is split, and each of its parts is cut by its own table: its columns as
    the table stores them, the nulls of the columns it lacks and the metadata lowering adds.
    What the piece asked for beyond what its split made and an allowance for two of its parts'
    pieces is given back. Its parts' pieces are lowered inside the allowance: the partition
    waits for its own pieces to be written, never for the budget, while it holds them.
  - A change stream's unit is judged and cut where it lies: it is split into its data and its
    change columns, and loses the rows its stream ignores, only as each piece is lowered.
  - A JSON push reserves three times its text when it is admitted: the text, and twice it for
    the batches it is shredded into, which are paid for before they are built. Once shredded
    the push holds what its batches keep alive, where that is less.
- **No request waits on the budget for ever.**
  - Every wait ends at a deadline on the engine's clock, `memory_wait`, an hour by default,
    longer than any call of a destination may take by default. The attempt then fails with
    `ErrorKind::Memory`, coded `memory_budget_wait_exceeded` and retryable, saying what the
    request was for, how long it waited and what each share held.
  - That kind and code are the engine's own. A read whose event waited until the deadline
    fails as the budget's whatever error its source ends with, and no connector's error is
    given the kind or keeps the code.
  - An admission may refuse an event (`Admission::admit` returns the refusal): the send fails
    with it, and so does the read, in process and through a host. It may refuse what a read
    keeps too (`Admission::charge`).
  - A request nobody waits for any more leaves the queue at once.
  - A read's schema and dictionaries are released before their replacement is charged, and
    however the read ends: returned, failed, stopped or dropped.
- **A measure bounded by the rows and by what they name once.** What rows expand to is measured
  without a vector of rows or values.
  - A stretch of fixed-width values, of strings or bytes by offsets, or of lists and structs of
    those is measured from its widths and offsets, however long it is.
  - A value that dictionary keys or runs name is measured once, apart and against the whole
    limit, and remembered where measuring it took more than sixteen steps: within the limit its
    bytes, exactly, and beyond it only that it is beyond, a state of its own and no number.
    Rows naming it again add what was remembered.
  - What is remembered is an entry of a few words for each such value named. Nothing is sized
    by a dictionary's length or by the greatest key, and each entry stands for more than
    sixteen steps, each a byte charged at least: what measuring holds is in proportion to what
    it charges, a few bytes a byte at the most, and for values of any size far less.
  - Every step adds at least a byte to a meter that stops at its limit, the budget's size, so
    the work is bounded by the limit too, where list views or a union name the same items
    again: those are charged each time they are named, and the wire and the emitter bound what
    they name by a frame.
  - A piece is cut from a running sum: stretches of rows are measured each twice as long as
    the last while they fit, and half as long once one did not, each no further than what the
    piece has left. A piece costs about one measuring of its rows and of half as many again,
    and what it was measured to take is what it reserves. The cut runs on the compute pool, a
    piece at a time.
  - No more values are remembered than take an eighth of the limit measured against; a value
    beyond those is measured each time it is named.
  - Tests count the steps and the values remembered for dictionaries of every key type, nested
    dictionaries, runs of keys of lists, keys into one run, views sharing a buffer, list views
    naming the same items and null spans, and the heap measuring takes.
- **One measure a question.** The wire crate weighs what a frame holds (ADR 0038); what a batch
  expands to and what it keeps alive are measured here, and nowhere twice.
  - *What would a frame holding these rows hold?* `rdlt_wire::Weigher`: values, view bytes and
    bytes, a dictionary's values apart. The emitter's admission and the bytes a write reports
    use it.
  - *What does holding this keep alive?* `cost::Allocations`: each allocation once. For a batch
    decoded from a frame it equals the decoder's `Shape::held_bytes` and the dictionaries its
    keys name, which a test holds it to.
  - *What do these rows become?* `Rendering::expanded` and `Rendering::measure` before their
    table is known, and `Rendering::lowering` once it is, whose `Measure::piece` says where a
    piece of that size ends. A cut of this kind bounds what the engine lowers at once; the
    wire's `Cut` bounds a frame.
  - *How much text do these values render to?* `cost::text_bytes`, the same widths, for the
    engine's builders.
  - *What is a push charged?* By the engine's admission, what it keeps alive, and three times
    its text for JSON. By certification's source clauses, `Rendering::charge`: the larger of
    what a batch keeps alive and what it expands to, for a holder that lowers nothing.
  - *Do a schema's columns and depth fit?* The emitter counts them on the Arrow schema; the wire
    counts them on the schema's message.
- **The engine materializes only what rows name.** Before anything converts a column, its
  dictionaries and runs are taken by the values their rows name, its list views become lists of
  the items they name, and every list's items are cut to the ones its rows hold. Arrow's casts
  convert a nested array's items and a dictionary's values whether or not a row names them, which
  no cost a row could bound.
- **The invariant is a test.** For every type and encoding the test kit draws, behind keys of
  every key type and as runs of every run-end type, whole and sliced, each consumer the engine
  has (decoding, the plain type, conversion, text, JSON text, listing) makes no more than the
  batch expands to. What a consumer makes is measured by the rows of its output, or by the
  allocations it does not share with its input where those are fewer: a builder's spare capacity
  stays the overdraft ADR 0024 documents.
- **Admission takes the event.** `Admission::admit` is given the event, and the engine costs it:
  a push by what it keeps alive, a checkpoint by its cursor's bytes.
- **A unit's pieces share what is charged.** A piece lowered from a unit is charged for the
  allocations it keeps alive that the unit did not hold; the unit's permits hold its source
  until its last piece is flushed. The constant columns of a load count for none.
- **A row that fits no request is refused.** A row that alone takes more to lower than a request
  may take of the budget fails the write with `row_exceeds_budget`, naming the limit, before it
  is lowered.
- **In-process pushes meet the wire's limits** at the `Emitter`, each at the wire's default.
  - Rows, nesting depth and nested columns are checked for every batch a source pushes, the
    last two on the schema, without recursion, before anything walks the batch.
  - A served read's batch meets no more there: it is cut to its host's frames as it is sent
    (ADR 0038), and its sink says so. Every other sink holds a batch as it is.
  - A batch held as it is, is then weighed as a frame of its rows would be: its values, the bytes its views
    name and its bytes, and each dictionary's values as the frame of their own they would go
    in. A slice is weighed by what its rows name, and rows naming one value through list views
    or a dense union weigh it each time, so what such a batch multiplies is bounded where it
    enters. A batch is not cut in process: one beyond a limit is refused.
  - The allocations it keeps alive are bounded by the same byte limit.
  - The connector crate depends on the wire crate for this whatever its features.
- **Certification charges by the same model** (ADR 0050). A source clause charges a push with
  `Rendering::charge`, for a holder that keeps each value as it is; a read-back is admitted by
  what its batches expand to; a kill clause's load is charged the allocations each batch keeps
  alive.
- **Nothing waits uncharged.**
  - A cursor copies its bytes when it is built, and the host copies a JSON push out of its frame:
    neither keeps the message it arrived in alive.
  - A checkpoint's permit stays with its seal until the commit that takes it has landed. A seal
    of no rows only moves its partition's position, so it replaces the one waiting: one cursor a
    partition, however many a source sends. A seal with rows starts a new epoch of its partition,
    and a waiting seal reaches the coordinator only through a message queued in its own epoch,
    so no commit records a position past rows still on their way.
  - A commit is due once waiting cursors hold half the cursors' share, and once one waits for
    room in it.
  - How far a read is behind, and that a stream's partitions changed, are each partition's
    newest state, with one message queued a partition. Signals heard together plan a stream
    once (ADR 0032).
  - A decoder's dictionaries are within one frame's bytes together, and the host charges them
    and the decoder's schema, with the message it came from, to the read's part of what reads
    keep while the decoder holds them.
  - A JSON push's records are found by a scan that keeps a span a chunk, not a range a record.
  - Row identity reads integers, temporal values and decimals where they lie.
  - A state record's value is base64 text wherever the record is JSON, and the log's seal and
    commit frames are reserved from the log's share before they are encoded, for the cursors
    and state they record twice over, and hold what they take until they are appended
    (ADR 0029).
  - A served write refuses more than four frames' bytes between two flushes, and the host's
    writer flushes before it would send more.
  - A column a batch holds nothing in is built as the destination stores it. Its nulls are part
    of what each row costs, so a piece is cut by them and reserves them before they are built:
    rows times the table's width is bounded by the budget, through a plan that normalizes too.
- **The bound is tested as the system runs.** Every run of the engine's integration tests fails
  where it reserved more than its budget. Loads that widen columns, fill wide tables and
  normalize into several tables run with sixteen partitions on a compute pool of four threads,
  which takes turns between pieces, one of them logged and replayed after a commit its
  destination missed: each holds its heap's peak to the bound below. Schedules on a paused
  clock hold the deadline, for every share that waits.

## The bound

A run never reserves more than its memory budget, and its heap stays within the budget, a fifth
of it and 32 MiB, beside three things that are not reserved: the push each read's source holds
before it is admitted, what a JSON push's records become beyond three times their text, and
what replay stages.

- The first is one push or one frame a partition, within the wire's limits.
- The second is the shredder's to bound, by its limit on cells: a megabyte of records of one
  key each, three hundred keys in turn, becomes 241 MB under a budget of 16 MiB, of which
  10 MiB are reserved.
- The fifth and the 32 MiB are measured, not derived: what the runtime, the channels and the
  builders' spare capacity take. The loads above peak far below them: 5 MB of heap where 300 KB
  pushed fill a table of two hundred 256-bit columns, 9 MB where sixteen million small integers
  widen to 256 bits, and 22 MB with sixteen partitions doing so at once, under 16 MiB.

This supersedes ADR 0024 where it charges memory at its decoded size.

## Consequences

- A push that keeps more alive than its rows take is charged for all of it, each push for
  itself: a source that pushes slices of one large buffer loads slower than one that pushes
  whole batches, and a slice of a buffer beyond 64 MiB is refused in process.
- Nested data is measured by its JSON text whether or not its table stores it natively, so it
  is lowered in smaller pieces.
- A unit whose every column is converted is charged for its source and what it was lowered to
  until its last piece is flushed.
- A push that keeps alive more than pushes may take of the budget, 108 MiB of the default, is
  refused; so is JSON text beyond a third of that, 36 MiB, though the wire carries a JSON push
  of 64 MiB: such a push needs a budget of 456 MiB. What a push keeps alive is bounded by the
  frame, batch-bytes and dictionary limits besides.
- What lowering reserves is a sum, the value decoded, converted and rendered: a column stored as
  text is lowered in smaller pieces.
- A budget is useful only where its shares hold what a load sends: a cursor needs a 64th of
  it, a commit's frame a 16th, a read's schema and dictionaries a 64th at the default sixteen
  partitions. The simulator and the crash tests run with a quarter of a mebibyte for that
  reason, where they ran with a kilobyte.
- A read through a host whose dictionaries take more than its part of the reads' share fails,
  though the wire would carry them: at the defaults 4 MiB a read, where a frame may hold
  64 MiB. More memory or fewer partitions raise the part.
- A normalized piece holds a quarter of the budget from before its split until its parts are
  cut, so a little over two normalize at once while pushes fill their share; and a unit of a
  few rows takes three trips to the compute pool before its parts are lowered in one.
- A waiting request of an hour is the last resort: nothing but a destination that stops
  writing, or a commit that never lands, makes one wait.
- A destination whose calls may take longer than an hour needs `memory_wait` raised with them.
- `ErrorKind` gained `Memory`, and `Admission::admit` and `Admission::charge` return a
  `Result`.
- A cut costs a trip to the compute pool a piece.
- The dictionary and staged limits follow the frame limit: one and four frames' bytes.
- An array-form JSON push is scanned for its elements twice: once to chunk it, once as each
  chunk is parsed.
- Checkpoints wait for room in the cursors' share. A source whose cursors are megabytes commits
  more often, and one whose cursor is beyond the share cannot be loaded under that budget.
- A batch decoded from a frame is charged for the dictionaries its keys name, which the host
  also charges while the decoder holds them: both hold them, and for as long as both do they
  are charged twice.
- The bytes a write reports count a validity bit a value, as a frame holds one.
- A read-back at the edge of what certification admits is measured a row at a time, with each
  row's offset and key: it admits a little less than rows times the widest value did where
  every value is as wide, and more where few are.
- Weighing an in-process batch stops at the limits between stretches of a thousand rows. The
  wire's weigher can stop within a row, but only for the wire crate's own cut, so one row
  whose list views nest and name the same items is weighed to its end before it is refused.
- Not bounded here: how much one push may expand to in total, which costs CPU and destination
  storage in proportion; the nulls of a nested column's fields the shredder builds, and what a
  JSON push of sparse records becomes beyond three times its text, which the shredder's limit
  on cells bounds; and what replay stages.
