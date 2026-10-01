# ADR 0038: Frames bounded as a whole

Status: accepted, 2026-10-01.

## Context

ADR 0015's decoder checked a frame piece by piece: each buffer against the body, each node's
length against a bound of its own, the schema's columns and depth once Arrow had converted it.
Nothing bounded what the pieces imply together, so a frame within every limit could ask far more
of its receiver than its bytes:

- columns naming one misaligned region of the body, which Arrow copies once a column;
- views naming one data buffer, which Arrow validates as text once a view;
- values that take no bytes (nulls, the items of a list of nulls, struct and run-end parents),
  bounded a node at a time, so a few bytes describe billions of values for what reads the batch
  next;
- a schema whose fields share one long name, copied once a field before the column limit is
  checked.

The typed error for a frame Arrow panics on rested on unwinding, which no build checked, and the
panic was printed.

ADR 0037 (untrusted connectors) makes everything a connector sends hostile input, bounded before
it is held. This ADR is that rule for Arrow frames, on every receiver: the host reading from a
connector, a served connector reading from a host, certification's read-back and the write-ahead
log.

## Decision

- **A batch's shape is walked against its schema before Arrow reads it.** The walk follows
  Arrow's own reader, column by column in the schema's order, taking the field nodes, buffers and
  counts of data buffers each type's layout has, for every Arrow type, dictionaries and their
  values' frames included. A message with fewer or more of them than its schema needs is
  refused.
  - A dictionary whose values are themselves a dictionary is no part of that: a field of the IPC
    format has one dictionary, so no schema message describes it, and no receiver is sent one.
    Arrow's writer would send a schema of its inner dictionary and then fail on the batch; the
    encoder refuses the schema by name instead (`Problem::DictionaryOfDictionaries`), so nothing
    of such a batch is sent, and its sender fails with `unsendable_type`. A dictionary nested in
    a dictionary's struct or list values has a field of its own and decodes.
- **Buffers are ordered, disjoint and padded.** Each starts at or after the end of the buffer before
  it, at a multiple of eight bytes, as the IPC format lays them out, within the body. Their
  total is then at most the body.
- **Each buffer is long enough for its node**: a bit a value of validity where the node has
  nulls, the width of a value times the node's length, one offset more than values, sixteen
  bytes a view. A node's length and null count are not negative, and it has no more nulls than
  values.
- **A frame's values have a limit of their own, `batch_values`**, 67,108,864 by default: the
  one-byte values a frame at the frame limit holds. Every node's length counts, nested ones too,
  and every list view's size, since list views may name the same items. It replaces the bound of
  a node at a time.
  A dictionary batch's entries are values of its frame, bounded by this limit; the limit on a
  batch's rows applies to record batches alone.
- **The bytes a frame's views name are bounded by the frame limit.** Each view of more than
  twelve bytes names bytes of one of its column's data buffers; the lengths are summed over the
  frame's view columns, once a view. Views may share bytes, as a well-behaved sender's do after a
  take or a cast from a dictionary, but together no more than a frame may hold, so Arrow
  validates no more text than that. A list view names items within its child.
- **The decoder copies a frame's buffers once, into one allocation of its own**, each at the
  alignment its column's type needs, and Arrow reads them there with alignment required, so it
  never copies a buffer to align it.
  - The format pads to eight bytes; decimals of 128 and 256 bits and views need sixteen. A
    sender padding to eight puts such a buffer where no copy of the body as it is aligns it, so
    the buffers are laid out anew, and Arrow is handed a message describing them there.
  - A transport hands a body over at any address. One path that always copies costs the same
    whatever the address, and what is decoded holds that one allocation, of at most the body's
    size, not the transport's message.
  - `Decoder::shaped` returns a frame's `Shape` beside its batch: its values, the bytes its
    views name, and the bytes of that allocation, for whoever charges the batch to a budget.
    `held_bytes` is the batch's own allocation only: the dictionaries its keys name were decoded
    from frames of their own and are held by the decoder, and by each batch that names them.
- **A sender cuts a batch a frame at a time, to the lesser of its receiver's limits and its
  own.** A served connector does so for what its source pushes, and the host for what the engine
  writes; the write-ahead log, whose limits are lifted, encodes a batch whole.
  - Each end cuts to the lesser, limit by limit (`Limits::lesser`), of what its peer advertised
    at the handshake and what it was configured with. A peer advertising more than a sender's
    transport carries, or than the sender may hold, then gets frames the sender can send.
  - **A piece is self-contained: it holds what its rows name and nothing else.** Arrow's writer
    sends the data buffers of views and the children of list views, dense unions and run-end
    columns whole however few rows name them, so a piece is first narrowed: views keep copies of
    their bytes, list views and unions the items they name, run-end columns the runs reaching
    into their rows, at every nesting and under the column's own type. Bytes that rows share are
    therefore sent once for each piece naming them, and for each row naming them where list
    views overlap: the pieces of a batch take about the logical bytes of the batch, not the
    bytes it holds, and a batch whose rows alias one buffer crosses at its logical size. A
    dictionary is the exception: it goes whole, in a frame of its own, ahead of the first piece
    that needs it, and is not sent again while it is unchanged.
  - **Cuts come from one weighing of the rows.** `Weigher` walks the batch's own buffers once,
    in order, copying nothing, and gives for each row what it adds to a narrowed frame: its
    values and view bytes exactly as the receiver's walk counts them, and its bytes before
    padding. A piece is the longest run of rows within the limit on rows, on values, on view
    bytes, and whose bytes with a bound on the frame's overhead (each buffer's padding, an
    offset more than rows, the header: computed from the schema) fit a frame.
  - **Each piece then costs one narrowing and one encoding**, of at most a frame, and the batch
    is never encoded or narrowed whole to learn its size. A batch of n rows cut into p pieces
    costs n rows weighed, p narrowings and p encodings; tests count all three, for plain columns
    and for views, list views and dense unions. The limit on rows is applied before anything is
    encoded.
  - The weight in bytes is an upper bound, so the encoded piece fits. Were it ever to fall
    short, the piece is encoded again with half its rows, at most the logarithm of its rows
    times, and one row that still does not fit is refused; a test forces this by leaving the
    overhead out.
  - A batch that goes as one piece is narrowed too when its buffers hold more than twice what
    its rows weigh, or more than a frame, as a slice of a larger batch does; a batch holding
    little else goes as it is, uncopied.
  - A batch of no rows goes as one frame of empty columns of its schema, whatever it was sliced
    from: Arrow's writer and reader disagree on empty slices of some layouts.
  - `Encoder::piece` hands over the frames of the next rows of a `Cut`. A sender encodes the
    next piece only once the last was sent, and yields to its runtime between them, so beyond
    the batch itself it holds one piece, of at most the frame limit it cuts to.
  - Every frame is then measured by the receiver's own walk before it is sent, so no frame a
    sender cut is refused by its receiver.
  - The pieces are consecutive rows in order. On a read they are pushes of the segment the batch
    was in, and nothing else the source sends, a checkpoint included, is taken until the last
    piece went; the host's controls wait for the credit window or the batch's end, both bounded.
    On a write they are writes of the batch's segment. Neither a push nor a write is a unit to a
    checkpoint or a commit: a segment is. A write that fails, a connection lost or a run stopped
    between pieces ends the attempt, whose staging the next attempt discards; a logged batch is
    logged uncut, and cut again when its commit is replayed.
  - One row beyond a limit, or a dictionary beyond one, cannot be cut: its sender refuses it with
    `limit_exceeded` naming the limit, where the cut reaches it, after the pieces before it went.
    - A refused write leaves the rows of the pieces before it staged under the batch's segment.
      The writer itself stays usable, in a schema epoch of its own, but its caller must not
      commit that segment: the engine ends the attempt, and the next discards its staging.
    - A read ends there: the rows already pushed are in a segment no checkpoint closes, and go
      with the attempt.
    - A batch the sender itself cannot narrow or encode fails with `unencodable_batch`, which
      blames the sender's batch and no peer.
  - `Weigher` and `Weight` are public, for whoever must charge a batch by what it will take
    without copying it. Beside values, view bytes and frame bytes a weight carries the rows'
    expanded bytes: what they take once each dictionary key and run is replaced by the value it
    names.
  - Certification's read-back queues every piece of the table it already holds whole; the
    minimums below bound that to about twice the table.
- **What the engine writes is plain.** Lowering stores every column as the Arrow type of its
  logical type: fixed-width values, `Utf8`, `Binary`, structs and lists, with list views cast to
  lists, views to offsets, and dictionaries and run-end encodings decoded. Its two constant
  metadata columns are dictionaries of one entry under sliced keys. Batches of no rows are
  dropped before they are queued or logged. So the host's writer meets no view, list view,
  union or run-end column and no empty batch from the engine, though it does meet slices that
  share their parent's buffers. The writer is a public trait all the same, and a served
  connector and read-back send whatever a source built, so the cut is correct for every layout
  and for empty slices, and is tested on each.
- **A peer's limits have minimums.** A sender cuts to the limits its peer advertised at the
  handshake, and every frame costs it each buffer's padding whatever its rows hold, so a peer
  asking for tiny frames would have it send mostly padding, many times the batch. A peer's
  `frame_bytes` is at least 4 MiB, its `batch_rows` at least 1,024 and its `batch_values` at
  least 1,048,576: one row of a schema at the column limit fits such a frame, and a hundred of
  its rows by their values. The host refuses a connector whose limits are below them, and a
  served connector such a host, at the handshake, with `limit_below_minimum` naming the limit.
  The other limits have no minimum: one too low only refuses what exceeds it. Certification
  fails a connector that declares a limit below a minimum.
- **Only what both ends speak is decoded**: metadata version V5, little-endian schemas, no
  compression, no delta dictionaries, and a dictionary batch whose id a field of the schema
  names.
- **A schema message is bounded before it is converted.**
  - It has a byte limit of its own, `schema_bytes`, 4 MiB by default, far below a frame's.
  - The flatbuffer verifier's budgets follow the message: a table every four bytes, and sixteen
    times its length once every table and string counts wherever it repeats; messages from Arrow's writer
    appear under twice their length. A message beyond them is refused as inflated. A batch's
    header is verified the same way.
  - Then its fields are counted on the flatbuffer, stopping at the first beyond a limit: columns
    and depth, each field's name against the control string limit, and its names, metadata and
    time zones together against `schema_bytes`, counted wherever a field repeats them. Only then
    does Arrow build the schema.
- **Containment of Arrow's panics is enforced and quiet.**
  - `rdlt-wire`, `rdlt-engine` and a served `rdlt-connector` do not compile with
    `panic = "abort"`: each turns a panic into an error by unwinding.
  - The decoder's first use wraps the process's panic hook. A panic it contains skips the hook;
    any other reaches the hook installed before.
  - The panic's text enters the error cut to 256 bytes, with everything but printable ASCII
    escaped.
- **The children of a run-end column of no values are not read.** Arrow's writer describes one
  run ending at zero for a run-end column sliced to nothing, as under a list whose rows are all
  empty, and Arrow's reader refuses that. The walk checks those children's buffers as any
  others, counts none of their values, and hands Arrow empty ones.
- **The write-ahead log lifts the numeric limits and keeps the rest.** Its batches are the
  engine's own and may exceed what a connector may send, so frame bytes, rows, values, columns,
  schema bytes and name lengths are unlimited there. Order, disjointness, counts, buffer lengths
  and views within their buffers hold for a log as for a connector.

This supersedes ADR 0015 where it says buffers are checked against the body, bounds a node by the
larger of the row limit and eight values a byte, has contained panics printed, and has the
`ipc_frame` fuzz target quiet the hook.

## Consequences

- A receiver refuses, with a typed error naming why, some frames a sender could build:
  - more than `batch_values` values in one frame, which only columns of under a byte a value
    reach before the frame limit: booleans, nulls and the parents of nested columns, over many
    columns and rows;
  - views that name more than a frame's bytes between them;
  - buffers out of order or padded to less than eight bytes, and messages of metadata version 4.
- rdlt's senders never send the first two: they cut a batch to its receiver's limits (above).
- A cut batch crosses at its logical size: bytes its rows share are sent for each piece that
  names them.
- Each frame costs one copy of its buffers. The body is not kept: a decoded batch holds its own
  allocation, of at most the body's size.
- The wire does not bound what a dictionary or a run-end encoding multiplies: keys or runs that
  each name a large value. What a batch expands to is the cost model's to charge.
- Left for the cost model, which charges what is held:
  - A batch's `held_bytes` leaves out the dictionaries its keys name.
  - The decoder keeps each dictionary until its schema epoch ends, uncounted and uncharged: at
    most a frame for each dictionary a schema names, so up to its columns times the frame limit.
  - Arrow validates a batch's keys against their dictionary, and each column naming a shared
    dictionary does so in each batch. What that costs was not measured.
- An embedder that installs a panic hook after the decoder's first use replaces the decoder's,
  and contained panics are printed again by theirs.
