# ADR 0039: A budget that charges what is held and what it becomes

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

- **One cost model** (`rdlt_connector::cost`). A batch costs the larger of two measures.
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
- **One pass, no vector.** The cost of a range of rows is computed from offsets where the type
  allows, and a row at a time where it does not. Every step adds at least a byte to a meter that
  stops at its limit, the budget's size, so the work is bounded by the limit however an encoding
  multiplies. Cuts are found by searching the rows' running cost, which only grows.
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
  a push by the model, a checkpoint by its cursor's bytes.
- **A unit's pieces share what is charged.** A piece lowered from a unit is charged for the
  allocations it keeps alive that the unit did not hold; the unit's permits hold its source
  until its last piece is flushed. The constant columns of a load count for none.
- **A row that fits no budget is refused.** A row that alone expands beyond the whole budget
  fails the read with `row_exceeds_budget`, on the wire and in process.
- **In-process pushes meet the wire's limits** at the `Emitter`: nesting depth and nested
  columns, measured without recursion before anything walks the batch, the values a batch holds,
  the bytes its views name, and the bytes it keeps alive, each at the wire's default.
- **Nothing waits uncharged.**
  - A cursor copies its bytes when it is built, and the host copies a JSON push out of its frame:
    neither keeps the message it arrived in alive.
  - A checkpoint's permit stays with its seal until the commit that takes it has landed. A seal
    of no rows only moves its partition's position, so it replaces the one waiting: one cursor a
    partition, however many a source sends. A seal with rows starts a new epoch of its partition,
    and a waiting seal reaches the coordinator only through a message queued in its own epoch,
    so no commit records a position past rows still on their way.
  - A commit is due once waiting cursors hold a quarter of the budget.
  - The budget keeps bytes only a commit releases apart from bytes in flight: they wait for room,
    but neither keep a push of the whole budget out nor press writers to flush.
  - How far a read is behind, and that a stream's partitions changed, are each partition's
    newest state, with one message queued a partition. Signals heard together plan a stream
    once (ADR 0032).
  - A decoder's dictionaries are within one frame's bytes together, and the host charges them
    while the decoder holds them.
  - A JSON push's records are found by a scan that keeps a span a chunk, not a range a record.
  - Row identity reads integers, temporal values and decimals where they lie.
  - A state record's value is base64 text wherever the record is JSON, and the log's seal and
    commit frames are charged until they are appended (ADR 0029).
  - A served write refuses more than four frames' bytes between two flushes, and the host's
    writer flushes before it would send more.
  - A column a batch holds nothing in is built as the destination stores it, and charged before
    it is built.

This supersedes ADR 0024 where it charges memory at its decoded size.

## Consequences

- A push that keeps more alive than its rows take is charged for all of it, each push for
  itself: a source that pushes slices of one large buffer loads slower than one that pushes
  whole batches, and a slice of a buffer beyond 64 MiB is refused in process.
- Nested data is charged its JSON text whether or not its table stores it natively, so less of
  it is in flight at once.
- A unit whose every column is converted is charged for its source and what it was lowered to
  until its last piece is flushed.
- A push larger than the budget still takes the whole budget and is worked through a slice at a
  time. What it keeps alive is bounded by the frame, batch-bytes and dictionary limits, not by
  the budget.
- The dictionary and staged limits follow the frame limit: one and four frames' bytes.
- An array-form JSON push is scanned for its elements twice: once to chunk it, once as each
  chunk is parsed.
- Checkpoints wait for room in the budget. A source whose cursors are megabytes commits more
  often.
- Not bounded here: how much one push may expand to in total, which costs CPU and destination
  storage in proportion; the null fill of rows times a table's width; and what replay stages.
