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
- **The write-ahead log lifts the numeric limits and keeps the rest.** Its batches are the
  engine's own and may exceed what a connector may send, so frame bytes, rows, values, columns,
  schema bytes and name lengths are unlimited there. Order, disjointness, counts, buffer lengths
  and views within their buffers hold for a log as for a connector.

This supersedes ADR 0015 where it says buffers are checked against the body, bounds a node by the
larger of the row limit and eight values a byte, has contained panics printed, and has the
`ipc_frame` fuzz target quiet the hook.

## Consequences

- Some well-formed frames are refused, with a typed error naming why:
  - more than `batch_values` values in one frame, which only columns of under a byte a value
    reach before the frame limit: booleans, nulls and the parents of nested columns, over many
    columns and rows. A million rows of more than 64 such columns need two batches;
  - views that name more than a frame's bytes between them;
  - buffers out of order or padded to less than eight bytes, and messages of metadata version 4.
- Each frame costs one copy of its buffers. The body is not kept: a decoded batch holds its own
  allocation, of at most the body's size.
- A sender still checks only a frame's bytes against its receiver's limits; one that sends a
  batch of too many values or view bytes learns it from the receiver's refusal.
- The wire does not bound what a dictionary or a run-end encoding multiplies: keys or runs that
  each name a large value. What a batch expands to is the cost model's to charge.
- An embedder that installs a panic hook after the decoder's first use replaces the decoder's,
  and contained panics are printed again by theirs.
