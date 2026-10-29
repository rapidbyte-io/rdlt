# ADR 0039: A budget that reserves what is held and is never passed

Status: accepted, 2026-10-01.

## Context

An estimate of a push's decoded rows (ADR 0024) cannot bound what the engine holds; it fails
four ways:

- It undercounts what the engine materializes: views and list views naming the same bytes or
  items many times, a null dictionary key that still takes its value's slot, a dictionary or run
  multiplying a value that takes no bytes, a column typed null widened to its table column's
  type, and the names of a struct's fields written into every row's JSON text.
- It ignores what a batch keeps alive: a few rows sliced from a frame, a file block or a
  dictionary are charged for the rows.
- Computing it allocates a vector a value.
- Much of what the engine holds is not charged by it: the decoder's dictionaries, the cursors of
  checkpoints waiting as seals, signals queued while a commit is in flight, the ranges of a JSON
  push's records, the copies row identity made, and what a served destination stages.

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
  | tables | 1/32 | 8 MiB | what a commit records of each changed table, its schema and names, from the change until the commit lands |
  | reads | 1/4 | 64 MiB | what reads keep beside their events: a decoder's schema and dictionaries |
  | answers | 1/16 | 16 MiB | what decoding a remote connector's answers holds, but a read's frames, from before each is decoded until it is (ADR 0042) |
  | data | the rest, 41/64 | 164 MiB | pushes waiting to be lowered, and what lowering makes of them; a read's frame from before it is decoded until it is |

  - Within the data, one request for lowering takes a quarter of the budget at most, and
    pushes never take the last quarter: a request for lowering always fits once the pieces
    before it are written and the requests holding them are given back.
  - **A request that could never fit is refused, typed.** A push that keeps alive more than
    pushes may take fails its read with `push_exceeds_budget`; one row that takes more to lower
    than a request may fails its write with `row_exceeds_budget`; a cursor beyond the cursors'
    share and what a read keeps beyond its part fail the read with `limit_exceeded`, naming
    `cursor bytes` and `read kept bytes`; a table whose records pass the tables' share
    fails its schema change with `table_exceeds_budget`, before the destination or any commit
    sees it.
  - **Control never waits behind data.** A checkpoint's cursor is reserved from the cursors'
    share, which no push can use. A commit is due once the waiting cursors take half of it, and
    as soon as a cursor waits for room in it; a barrier raised for that commit ends its wait for
    answers then, and the commit takes what is sealed.
  - **A read keeps a bounded part.** The reads' share is divided into a part for each
    partition read at once, or into as many parts of twice the protocol's least dictionary as it
    holds where those are fewer: 4 MiB a part at the defaults, and 512 KiB from 128 partitions
    up at the default memory. No read keeps more than a part.
    - A read that is not followed holds a whole part while it reads, so it is refused nothing
      its limits admit, and all reads together never pass the share. One beyond the parts waits
      for one until one frees, with no deadline.
    - Followed unbounded reads hold no part of their own. What they keep, together, holds as
      many of the parts no other read holds as it takes, a part more as they keep more and
      fewer as they keep less, and never the last, so a read that is not followed always comes
      to hold one. A followed read whose frame would need a part where none is free fails at
      that frame, `limit_exceeded` naming `followed reads kept bytes`. Where the share holds a
      part for each partition, followed reads that keep to their limits never meet it: they
      hold no more parts than slots, and the other reads no more than the slots left.
    - Every read holds one of the partitions' slots while it reads, a followed unbounded read
      included, so the cursors' share is divided among them. A source whose calls are bounded,
      as a served one's are, has fewer slots where its calls, less the engine's one other call
      into it, are fewer than the partitions (ADR 0042). A followed unbounded read holds its
      slot as long as the run, so a run reads fewer of them than it has slots: one more is
      refused as it starts, `partitions_too_few`, or `source_calls_exceeded` where the source's
      calls set the slots, rather than leaving the other reads no slot. A read beyond the slots
      waits for one until one frees, with no deadline: a bounded read's end frees its slot, and
      the followed reads leave one, so a slot always comes round. The waits for a slot and a
      part are counted with the reads' share.
    - A read that would keep more than a part fails at the frame that would pass it. Sixteen
      reads keeping all they may, and seventeen followed reads through eighteen slots, leave
      pushes and checkpoints flowing, which tests hold on the ledger, through the engine's
      admission and through a host's read of a served source. Tests hold followed reads to the
      parts they may hold, on the parts and through the engine's admission, beside a read that
      is not followed keeping its whole part, and run 256 followed reads through 257 slots
      within the default memory.
  - **No hold and wait.** A task waits on the budget only while it holds nothing that only
    its own progress releases. A partition waits for lowering's bytes while it holds pushes,
    and nothing waits for pushes while it holds anything; whoever holds what lowering reserved,
    a piece on the compute pool or on its lane, needs no budget to release
    it. A partition lowers and hands to its lane whatever it reserved before it waits for more.
    What lowering reserves that a partition holds while it lowers, the batches a JSON flush is
    built to beyond its pushes and a normalized piece's split, is reserved together with room
    for all that lowering them holds at once, within one request: the partition then waits for
    its own pieces to be written, never for the budget, so all that lowering holds is released
    without the budget, and a request fits once it is.
  - Requests of one share are admitted in arrival order, lowering before pushes.
  - The shares are limits of the engine, with the names above, in `limits.rs`.
- **What a connector is told it may send is what the budget admits.** The limits a host
  advertises at its handshake, and those the `Emitter` holds an in-process source to, are each
  the lesser of the configured limit and what the shares hold (`EngineConfig::limits`). Each is
  derived so that the worst case it admits, all at once, fits the share it draws on:

  | Limit | Derived from | Least memory, 16 partitions | Default 256 MiB, 16 partitions | Wire default |
  |---|---|---|---|---|
  | frame bytes | what pushes may take less what a read keeps, `MESSAGE_ROOM` and the arrays of a batch of every column, two a column, and half a request less what a row of the widest table takes beside its values | 4,194,304 | 28,505,370 | 64 MiB |
  | json push bytes | a third of what pushes may take less `MESSAGE_ROOM` | 5,149,072 | 34,952,448 | 64 MiB |
  | cursor bytes | the cursors' share over twice one more than the partitions | 18,173 | 123,361 | 4 MiB |
  | dictionary bytes | half of what a read keeps | 308,949 | 2,097,152 | 64 MiB |
  | schema bytes | a fifth of the other half, for the message and the schema it decodes to | 61,789 | 419,430 | 4 MiB |
  | schema columns | the tables' share over half a kilobyte a column, and schema bytes over 56 a column | 1,103 | 7,489 | 10,000 |
  | catalog bytes | the answers' share over sixteen, what a catalog may hold decoded for each byte | 154,474 | 1,048,576 | 4 MiB |
  | state bytes | the answers' share: state is bounded on what it holds decoded, and a commit leaves no more | 2,471,595 | 16,777,216 | 16 MiB |
  | control message bytes | the answers' share over sixteen | 154,474 | 262,144 | 256 KiB |

  - **Cursors.** Every partition may hold a cursor waiting for a commit and a barrier's answer
    at once, with room for one more: a barrier's answers always fit, so a source that keeps to
    its cursor limit never waits on a barrier for room.
  - **Rows.** A frame is at most half a request less a row's null in each column, unchanged
    flags over every column as text, the metadata lowering adds and the arrays of the piece the
    row is alone, two a column and sixteen of metadata, so its decode, twice its bytes, is charged
    within a request, and a row as long as a frame lowers within one: a row within the frame
    limit is never `row_exceeds_budget`. That refusal is left to a row its table widens beyond
    what it arrived as, and to one of a JSON flush whose batches take more than half a request
    beyond their text, which leaves the row less of the request beside them.
  - **Tables.** A table as wide as a schema may be records its schema and names within the tables'
    share, at names of up to a hundred and fifty bytes; a schema message carries that many columns
    of short names. What a commit records of the tables is reserved from that share at each schema
    change, the latest change of a table in place of the one before, and held until a commit records
    it. A table wider than a schema may be is refused where the source pushes it, and, across
    pushes, at the schema change that would make it so (ADR 0041).
  - **Dictionaries.** The sender and the decoder hold dictionaries to the limit by one count of
    the wire crate: a dictionary's frame, header and body, and `MESSAGE_ROOM` beside it, at least
    the allocation a receiver holds it in. The sender counts what its receiver holds from every
    batch since the last schema, and a batch whose dictionaries would pass it is refused before
    it is sent.

  Rows, values and nesting are not the budget's: a batch is lowered a piece at a time,
  whatever it holds. Where a wire default is above what the budget admits, the derived value is
  what crosses the handshake and what a connector sees. A served connector cuts and bounds what
  it sends to it, and refuses its source what it cannot cut; the host's decoder refuses what
  passes it. The budget's own refusals, `push_exceeds_budget` and `limit_exceeded` for `read
  kept bytes`, stay as the backstop for a connector that ignores what it was told, or a host
  told the wire's defaults. What a push becomes is not the limits' to bound, so a push within
  them is still refused where it becomes more than one request may take: a row its table widens
  (`row_exceeds_budget`, Rows above) and JSON whose batches take more than a request beyond
  twice its text (`json_exceeds_budget`, ADR 0040). A megabyte of JSON records, each naming one
  of three hundred keys, is refused at 40 MiB, where a JSON push may be 5.5 MB.
- **The least memory.** A budget whose derived frame limit is below the protocol's least frame
  (`MIN_FRAME_BYTES`, 4 MiB), or whose dictionary limit is below the protocol's least
  (`MIN_DICTIONARY_BYTES`, 256 KiB), is refused when the engine's configuration is built, with
  `memory_below_minimum` naming the least memory from which every memory up admits it:
  39,545,528 bytes at the default sixteen partitions, 38,780,592 from eighteen up and 50,060,952
  at one (`EngineConfig::least_memory`). Half a request for lowering must hold a frame and the
  row beside it, so the least is about nine frames at sixteen partitions; fewer partitions each
  keep more, so a schema may have more columns and the row beside a frame takes more, which
  raises the least to 50,060,952 from one to seven partitions. A part holds twice the least
  dictionary wherever the share holds one, so the dictionaries never set it, and more partitions
  need no more memory: at the least, the share holds eighteen parts, and the reads that are not
  followed read no more at once than the parts. Partitions above 16,384, the most an attempt
  reads at once (`MAX_PLAN_PARTITIONS`), are refused when the configuration is built, with
  `config_invalid`.
- **A peer's dictionary limit has a least.** A handshake whose limits set a dictionary limit
  below `MIN_DICTIONARY_BYTES`, none included, is refused at either end with
  `limit_below_minimum`, as any limit below its least is; a handshake that sets no limits at
  all keeps the protocol's defaults.
- **One cost model** (`rdlt_connector::cost`). A batch has two measures.
  - **Held**: the bytes of every allocation the batch reaches, validity, data, child and
    dictionary buffers alike, each counted once however many arrays or slices share it. A slice
    costs the buffer it was cut from; a frame's batch costs the allocation the decoder made for
    it and the dictionaries its keys use. A validity is found through the arrays that hold it,
    since Arrow's data of an array leaves out one holding no null, which the array keeps alive.
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
  batches reserves what it keeps alive when it is admitted, before its table is known, and its
  arrays: `ARRAY` (below) for each, a field at every depth and a dictionary's values beside its
  keys, which a batch holds and a slice of it shares. Once its plan is found the cost model
  takes, for each column, the type of the table's column and whether the destination stores it
  as text, and measures what lowering holds at once, which each piece reserves before it is
  lowered.
  - A value costs itself decoded, itself converted where the column's type differs, and its
    text where the column is stored as text: a byte of an 8-bit integer is 33 in a column of
    256-bit decimals.
  - A column of JSON holds each value decoded and its JSON text beside, a string with its
    escapes counted as they are, not at their worst.
  - A nested column converts field by field and item by item; a field the table's struct has
    and the batch's lacks costs a null a row. Stored as text it costs its JSON text beside.
  - A conversion between dates, times and instants holds its result beside the value: one slot
    a value, as its column's type takes.
  - Every row costs the nulls of the table's columns the batch holds nothing in.
  - A piece costs, whatever its rows, the arrays of each batch lowering holds of it: its parts,
    each a slice of a batch of its unit; where it has several, the batch they are joined into;
    and the batch it is lowered to, the table's columns and their metadata. An array is charged
    `ARRAY`, 320 bytes, at every depth of its batch's fields: itself, a handle on each of its
    buffers and its place among its batch's columns, as much as a column of text with its
    validity takes. Lowering builds no other batch of a piece's columns: a lone part is lowered
    as it is, a merge's batch keeps its own schema where no key column changes, and a history
    table's versions hash its columns under fields built once with its plan.
  - A change stream's row costs its unchanged flags as its table keeps or merges by them,
    remapped over the table's columns: a bit a column as far as the furthest the batch writes,
    with an offset and its validity, and their hex text beside them where the destination
    stores bytes as text. The remap takes room for every row's flags at once, so it never grows
    past that.
  - The engine's text builders reserve what the model says the text takes at most, so they
    never grow beyond what was charged.
  - The invariant is a test here too: every numeric, temporal, decimal, text, bytes and nested
    type, plain, behind keys and as runs, is lowered into every type its table may hold it in,
    as it is and as text, and the heap's peak is held to the charge. A piece of a table of
    integer columns, each charged exactly what joining its batches copies, holds no more than
    it reserved, slicing its parts included, and keeps no more once lowered than it is charged:
    two thousand rows in two parts of a table of a thousand columns, and one row in one part and
    two in two of a table of a thousand and of the widest a schema may have at the defaults,
    7,489: a change stream's, as a log and merged, stored as it is and as text, with its flags
    and the deletes it ignores, a merge's of repeated keys, a history table's, and one dropping
    rows. At both widths a push of a row of integers or of dictionaries, or of a thousand rows of
    booleans whose validity holds no null, holds no more than it is admitted for, built in its
    source's process or decoded from its frame; a JSON push's batches no more than they keep
    charged once shredded; a normalized split, in one part and in two, keyed or not, no more than
    it keeps charged once its parts are planned; and cutting a unit keeps nothing a column beside
    it between its pieces.
  - Judging which columns of 64-bit integers a batch holds a value a float would round in reads
    each column where it lies, in any encoding, and holds nothing a row, so nothing is charged
    for it; a normalized flush is judged so before any of it is split, and tests hold its peak
    beside long dictionary and run-end encoded columns and every column a normalizing plan
    splits.
- **What is lowered at once is what was reserved.** A unit's plan is found first, and the unit
  is then cut by that cost, a piece at a time.
  - A piece is at most a sixteenth of the budget, or one row where a row takes more, up to the
    quarter a request may take.
  - Each piece reserves its cost before it is lowered. A partition waits for one piece's bytes
    only while it holds no piece it has not handed to its lane; beside that piece it lowers as
    many more as the budget has room for at once, eight at most. How many pieces are lowered
    at once follows from what the budget admits.
  - A piece of several batches is joined into one batch, a copy its cost counts as each
    value decoded. A step that keeps some of its rows (those a change stream ignores, those a
    schema policy drops, those a merge keeps) copies them a column at a time and lets each
    column it copied from go once it has, so the piece never holds its rows twice over.
  - Once lowered a piece holds what it keeps alive beyond its unit's batches and its batch's
    arrays, in place of what was reserved, and its frame what the frame takes. The unit's
    batches and its own reservation stay with its last piece until that is written.
  - **Normalized streams follow the same rule.** A unit's tables are known only once it is
    split, so it is measured twice. It is first cut, as it arrives, into pieces whose split makes
    no more than a piece: each row with its lineage, each item an array holds with its own, for
    the two copies a split may hold. Each such piece then asks the budget once for all a
    request may take, is split, and each of its parts is cut by its own table: its columns as
    the table stores them, the nulls of the columns it lacks and the metadata lowering adds.
    What the piece asked for beyond what its split made and an allowance for two of its parts'
    pieces is given back. What the split made is counted beyond the unit's batches, with the
    arrays of the piece's slices of them, of its parts' batches and of their lineage, and the
    piece and the parts it planned are held until the last of their pieces is written. The
    allowance is lent to its parts' pieces: each takes the bytes lowering it holds and gives
    them back as it is written, so the partition waits for its own pieces to be written, never
    for the budget, while it holds them, and the allowance goes back with the last of them.
  - A change stream's unit is judged and cut where it lies: it is split into its data and its
    change columns, and loses the rows its stream ignores, only as each piece is lowered.
  - A JSON push reserves three times its text and `MESSAGE_ROOM` when it is admitted: the text and
    the room of the message a host receives it in, and twice the text for the batches it is shredded
    into, which are paid for before they are built. Once shredded the push holds what a push of its
    batches would be charged, where that is less. Parsing a record holds no document of it: the
    fast parse streams it and the exact parse walks its tokens, each holding of it no more than a
    string whose escapes it reads.
  - What a JSON flush's batches take beyond twice its text is reckoned before they are built,
    reserved from the data share, holding only its pushes, and held with the batches; beyond
    one request it is refused, `json_exceeds_budget`. The partition then holds it while the
    batches are lowered, so their lowering asks nothing of the budget it might wait for:
    - The flush asks, with what its batches take, about what lowering them holds at once, what
      they take and twice their text, within what a request leaves: one request, which the
      partition waits for holding only its pushes. Their units are planned and cut before any
      is lowered, each row within what a request leaves beside the batches, and what lowering
      the largest piece holds is kept of it, or reserved beside it without a wait. Where the
      budget has no room for it, the batches are let go, their pieces never lowered, and built
      again once what they take and that room fit together.
    - A normalized flush's splits each ask for a request, so it asks for a whole request
      before its pushes are observed, holding only them, and is observed and built inside it,
      whatever its batches take.
    - That room is lent to the pieces, or the splits: each is reserved from it and goes back
      to it as it is written, so the partition waits for its own pieces, never for the
      budget, while it holds the batches. A piece or a split may also take what the budget
      has free without a wait. Once the last piece or split has its room, what is left goes
      back to the budget, and each piece's part as it is written.
  - A null converted to a type of no fixed width is charged that type's null slot, inside
    structs and lists too. A piece and a row take at most `i32::MAX` bytes whatever the budget,
    as far as a text array's offsets reach (ADR 0040).
- **No request waits on the budget for ever.**
  - Every wait for bytes ends at a deadline on the engine's clock: twice the deadline of a
    connector's call (`connector_wait`), and an hour at least, an hour at the defaults. The
    attempt then fails with `ErrorKind::Memory`, coded `memory_budget_wait_exceeded` and
    retryable, saying what the request was for, how long it waited and what each share held.
  - The deadline is not a bound on latency but a backstop against a defect in the budget's own
    protocol, whose freedom from deadlock this record argues in prose: such a defect fails an
    attempt, which is retried, rather than hanging the pipeline. A call that hangs fails at its
    own deadline, naming the call, before any wait that began less than one connector deadline
    before the call reaches the budget's; a chain of calls each near its deadline is not
    healthy, and the backstop may end it. No bound on a wait follows from the engine's other
    deadlines: a commit is a barrier, each lane's queued writes, the commit itself and an
    acknowledgement a stream, each a call of its own, and a cursor sealed late waits for the
    commit in flight and the next.
  - That kind and code are the engine's own. A read whose event waited until the deadline
    fails as the budget's whatever error its source ends with. No connector's error is given
    the kind, and a code a connector's error carries stays the connector's
    (`Error::connector_code`), never one of the engine's (`Error::code`).
  - An admission may refuse an event (`Admission::admit` returns the refusal): the send fails
    with it, and so does the read, in process and through a host. It may refuse what a read
    keeps too (`Admission::charge`).
  - A request nobody waits for any more leaves the queue at once.
  - A partition waiting for its own pieces to be written, for room in what it was lent, a JSON
    flush's room or a normalized piece's allowance, waits on its lanes, not on the budget: no
    deadline of the budget's ends it, as none ends a wait for a lane.
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
    piece at a time, where measuring reads values. Where every column is measured by its rows
    and offsets alone (a scalar, or strings and bytes as they are, converted to no JSON and read
    from none), a piece takes as many steps however many rows it has, and is cut on the
    partition's task.
  - A measure borrows the batch it measures, how its table stores each column and the
    rendering, and copies none of them. The cutter builds one for each piece it cuts, and one
    for a row beyond a piece, and reads the unit's batches through what holds the unit: what
    measuring learns of the values keys and runs name lasts the piece, and cutting keeps nothing
    a column beside the unit between its pieces.
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
    decoded from a frame it equals the decoder's `Shape::held_bytes`, the allocation the frame
    arrived in, and the allocations of the dictionaries its keys name, which a test holds it to;
    the decoder's count of those dictionaries is at least that.
  - *What do these rows become?* `Rendering::expanded` and `Rendering::measure` before their
    table is known, and `Rendering::lowering` once it is, whose `Measure::piece` says where a
    piece of that size ends. A cut of this kind bounds what the engine lowers at once; the
    wire's `Cut` bounds a frame.
  - *How much text do these values render to?* `cost::text_bytes`, the same widths, for the
    engine's builders.
  - *What is a push charged?* `cost::push_charge`, by the engine's admission and by
    certification's source clauses alike: a batch what it keeps alive and its arrays
    (`cost::batch_charge`), and JSON `cost::json_charge`, `JSON_CHARGE` (three) times its text and
    `MESSAGE_ROOM`.
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
  is held, and charged by the capacity of its buffers, until the piece is written.
- **Admission takes the event.** `Admission::admit` is given the event, and the engine costs it
  before it waits: a push by what it keeps alive, a checkpoint by its cursor's bytes. The wait
  borrows nothing of the event, which the sink holds beside it.
- **A unit's pieces share what is charged.** A piece lowered from a unit is charged for the
  allocations it keeps alive beyond the unit's batches, each once, and for its batch's arrays;
  the pieces share what holds the unit, so its batches and permits hold until its last piece is
  flushed. The constant columns' buffers of a load count for none.
- **A set of allocations names only what is alive.** `cost::Allocations` tells allocations
  apart by where they start, and the allocator hands a freed address out again, so a set is kept
  only while its holder keeps alive what it names. A unit's set is built once from its batches,
  which what holds the unit keeps until its last piece goes, and is never added to; a piece's
  growth, and a split's, is counted in a set of its own, dropped once counted. A piece made where
  an earlier piece of its unit was freed is charged in full: tests build one in the very
  allocation an earlier piece freed, and a property holds each piece's charge to its allocations
  beyond its unit, counted afresh.
- **A row that fits no request is refused.** A row that alone takes more to lower than a request
  may take of the budget, or than a JSON flush's batches leave of theirs, fails the write with
  `row_exceeds_budget`, naming the limit, before it is lowered. A row within the frame limit
  fits half a request, so only a row its table widens, or one beside batches taking more than
  the other half, meets it.
- **A refusal names its cause once.** A refusal of the budget that a later admission of the
  same read passes is forgotten: a read that fails later, for its own reasons, is not reported
  as failing for memory.
- **In-process pushes meet the wire's limits** at the `Emitter`, each at the wire's default.
  - Rows, nesting depth and nested columns are checked for every batch a source pushes, the
    last two on the schema, without recursion, before anything walks the batch; and the
    message that carries a new schema is held to the schema bytes limit.
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
  `cost::push_charge`, as the engine admits it; what the clause renders is held to its rendering
  limit, and `S-RESUME` compares batches only while what they expand to is within it. A
  read-back is admitted by what its batches expand to; a kill clause's load is charged the
  allocations each batch keeps alive.
- **Nothing waits uncharged.**
  - A cursor copies its bytes when it is built, so it keeps no message alive. A JSON push is a
    slice of the message it arrived in, which its charge's `MESSAGE_ROOM` covers beside its
    text: the host refuses a frame whose message holds more, `malformed_frame`.
  - A checkpoint's permit stays with its seal until the commit that takes it has landed, or until
    a later seal of its partition at a cursor takes its place, which frees the cursor and then its
    permit: one cursor a partition, however many a source sends, whatever rows come between. A
    seal ending its partition done keeps the cursor before it, so the source is still told it. A seal with rows starts a new epoch of its partition,
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
  - A JSON push is cut into chunks that keep a span of each push and, of an array, how many
    elements it holds, not a range a record; each chunk's job finds its records again.
  - Row identity reads integers, temporal values and decimals where they lie.
  - A state record's value is the JSON it is wherever the record is JSON.
  - A served write refuses more than four frames' bytes between two flushes, and the host's
    writer flushes before it would send more. The connector decodes a frame only once its
    writer has taken the frame before it, so a write holds its staged bound and one decoded
    frame waiting for its writer.
  - A column a batch holds nothing in is built as the destination stores it. Its nulls are part
    of what each row costs, so a piece is cut by them and reserves them before they are built:
    rows times the table's width is bounded by the budget, through a plan that normalizes too.
- **What a charge covers is freed before the charge is released**, however the work holding it
  ends: done, failed, or let go by a cancelled attempt. Between a release and the free, another
  task may reserve those bytes and allocate them, and the process holds more than its budget.
  - A charge is held in one value with what it covers, declared after it, so dropping the value
    frees what it covers first: a unit's batches with the permits they were admitted with, a JSON
    flush's text with its permits, a flush's batches with what was reserved beyond them, an event
    with its admission while it waits for it, a lowered batch with what holds it on its lane.
  - A unit's pieces share what holds the unit, each holder of a piece's rows declaring its share
    last: the unit's permits go once the last piece is written or let go, wherever it is, on the
    compute pool, in the window being lowered or on its lane.
  - No job on the compute pool outlives the caller waiting for it: a caller that stops waiting
    drops its jobs not yet started, and waits for those running to end, so what a job reads and
    makes goes before what the caller holds.
  - Tests let go of writes at each of their waits, for room, for the pool and for a lane, of an
    Arrow, a JSON and a normalized flush, and of a send waiting for its admission, and hold that
    what each charge covers was freed by the time it was released.
- **The bound is tested as the system runs.** Every run of the engine's integration tests fails
  where it reserved more than its budget. Loads that widen columns, fill wide tables and normalize
  into several tables run with sixteen partitions on a compute pool of four threads, which takes
  turns between pieces: each holds its heap's peak to the bound below. Schedules on a paused clock
  hold the deadline, for every share that waits.

## The bound

A run never reserves more than its memory budget, and its heap stays within the budget, a fifth
of it and 32 MiB, beside what is not reserved: the push or frame each read holds before it is
admitted, and what a remote connector's calls hold in transport.

- The first is one push or one frame a partition, within the wire's limits.
- The second is each open call's stream window on a remote connection, `CREDIT_FLOOR` (4 MiB),
  which HTTP/2 enforces on a connector that ignores its credit; the connection's window holds
  every call's and one more, so no connection holds more than 804 MiB (ADR 0016). A read's
  frames beyond it wait in the connector.
- What a JSON push's records become beyond three times their text is reserved before it is
  built, and a push whose records could not fit is refused unbuilt (ADR 0040), but for the two
  documents "Not bounded here" names.
- The fifth and the 32 MiB are measured, not derived: what the runtime, the channels and the
  builders' spare capacity take. The loads above peak far below them: 5 MB of heap where 300 KB
  pushed fill a table of two hundred 256-bit columns, 9 MB where sixteen million small integers
  widen to 256 bits, and 22 MB with sixteen partitions doing so at once, under 16 MiB.
- What the budget does not bound: what the allocator keeps after it is freed, its arenas and
  its thresholds, and each thread's stack. sonic-rs keeps nothing on a thread: the buffer it
  keeps for each thread fills only as it builds a document, and the shredder builds none, its
  fast parse streaming each record through sonic-rs's deserializer and its exact parse walking
  the record's tokens itself. A nightly check runs the passthrough and the normalized workloads,
  each in a process of its own, with glibc's defaults and with the tunables a spawned
  connector gets, and holds the process's resident peak to the budget's bound, one push, and
  2 MiB of stack a thread. It runs the passthrough workload served too, read from sixteen
  partitions through a host that spawned both connectors, and holds the host's peak to the
  bound, a frame a partition and a stream window an open call, reporting each connector
  process's peak beside it.
- **The simulation holds its attempts to it on the heap, at every moment.** Its test binary counts
  each thread's live bytes (`alloc_tracker`, a counting allocator kept per thread, installed in the
  simulation's test binaries alone); a seed's runs share its thread, compute inline, while seeds run
  side by side on other threads. Built with its `sim` feature, the engine tells its environment each
  change of what a budget holds reserved. On the seed's thread a counterweight, an allocation never
  written whose size falls as the charge rises, makes what the thread counts what it holds beyond
  what is charged at that moment: what the budgets hold reserved and what the source holds of pushes
  made and not yet sent on. What the simulated world keeps of what it stores counts beside the
  charge, exactly, as the world's: a stored row as much as freeing a first making of it gives
  back. The world's own work building a push
  or a row is given room while it runs. Over each attempt, the most the thread holds beyond its
  charge stays within an allowance and, where the connectors listen on the simulated network or are
  played in processes and share the thread, what the reads hold between the source and the engine:
  for each partition each run reads, 4 of the attempt's largest event, a push or a checkpoint's
  cursor (the host's event waiting for admission, the batch being cut and its piece waiting for
  credit, and a frame sent past its credit); what the connector's channel holds, 16 of the largest
  event or, where it is less, the host's frame limit, the largest push and 16 of the largest cursor;
  and the frames credit lets go before the host takes them, two of the largest or the host's credit
  floor of 4 MiB,
  whichever is more. The allowance is what the world holds beside the engine and keeps uncounted,
  the runtime, its maps and builders' spare room, apart by where the connectors run: 13 MiB in
  process and 11 MiB over the network or in played processes. Beyond their charge, seeds 0 to
  99 999 hold at most 12.5 MB in process and 4.3 MB over the network, the network's beyond what
  its reads hold. Over the network the allowance is the next whole mebibyte above five quarters
  of the most a seed holds among the four network seeds, not
  hostile, that hold the most the nightly has measured, replayed under these faults: 8.9 MB on seed
  732369, twice the most of seeds 0 to 99 999. No sweep of the nightly's million has measured the
  network's tail, so the allowance rests on those four seeds and the nightly is what tests it
  further. Played processes share it without a measure of their own: no sweep has taken their tail
  either. In process the allowance is the next whole mebibyte above a mebibyte more than the most
  seeds 0 to 99 999 hold. A failing seed
  names its phase; replayed with `RDLT_SIM_TRACE` set, it names the change of the charge after which
  its thread held too much, and the code that made it. The world is one of fat values: a wide object
  of JSON on every eighth record of a stream pushing JSON, a sixteenth of the least push, and spaces
  beside some pushes up to their limit; on every fourth record the least budget refuses the batches.
  The world keeps its rows apart from the batches they were written in, and the short form of each
  fat value. Not measured: seeds that change rows, seeds under stress, whose compute runs on Rayon's
  threads, hostile seeds over the network, whose hostile host's heap shares the seed's thread, and
  the runs with nothing new that check the reports a source hears.
- **What the check sees.** Every seed of 0 to 99 999 holds to it: what a charge covers is freed
  before the charge goes, so no moment holds it beyond its charge. A JSON flush that gives back
  its text's charge while it holds the text fails it: with that fix undone, 8 of seeds 0 to 1 999
  fail, each once `shredded` shrinks the flush's permits, up to 21.4 MB beyond the charge. It
  cannot see three holds smaller than its allowance, each bounded by the widest record a fat world
  pushes, a sixteenth of the least push: what rendering JSON grows uncharged, twice the widest
  value rendered; the documents the exact parse and a part's read once built, ten to twelve times
  a record, up to 2.9 MB more over seeds 0 to 19 999 with them back; and a body that copies its
  message before taking room, one transport frame. The shredder's heap tests hold the first two,
  and the third is held by `rdlt-wire`'s
  `a_message_behind_one_passed_on_from_its_chunk_takes_room_before_it_is_copied`.
  `alloc_tracker` is kept out of `rdlt-adopt`, which holds the two audited files ADR 0048 keeps,
  and its `unsafe` is a dependency's, which ADR 0048's checks do not see, as `peak_alloc`'s is.

## Consequences

- A push that keeps more alive than its rows take is charged for all of it, each push for
  itself: a source that pushes slices of one large buffer loads slower than one that pushes
  whole batches, and a slice of a buffer beyond 64 MiB is refused in process.
- Nested data is measured by its JSON text whether or not its table stores it natively, so it
  is lowered in smaller pieces.
- A unit whose every column is converted is charged for its source and what it was lowered to
  until its last piece is flushed.
- A push that keeps alive more than pushes may take of the budget, 100 MiB of the default, is
  refused; so is JSON text beyond a third of that less a message's room, 33 MiB, though the wire
  carries a JSON push of 64 MiB: such a push needs a budget of 492 MiB. What a push keeps alive
  is bounded by the frame, batch-bytes and dictionary limits besides.
- What lowering reserves is a sum, the value decoded, converted and rendered: a column stored as
  text is lowered in smaller pieces.
- A piece of a wide table costs its arrays whatever its rows: a row alone of the widest table at
  the defaults reserves 4.8 MB beside its values, each part of a piece of it 2.4 MB and the batch
  several are joined into as much again, so a unit of many small batches of a wide table is
  lowered in more pieces, and a frame at the defaults is at most 28.5 MB.
- No engine runs on less than about thirty-eight mebibytes, the price of a row as long as a
  frame lowering within half a request. The simulator draws budgets between the least for one
  partition and twice that, and gives a run one slot more than the unbounded partitions it
  follows. Its in-process sources press on them, in three worlds of four: pushes keeping alive
  a fifth to nine tenths of a frame, and cursors of half to all a cursor may be, so over two
  thousand seeds pushes wait for lowering's room in 783 and cursors wait for a commit in 474.
  The exactly-once sweep holds the shares that keep the budget pressed, pushes waiting in a fifth
  of its seeds at least and cursors in a tenth, as a one-sided binomial test: a count fails only
  where a share at the gate's would give that count or fewer under once in a thousand. At 200
  seeds that is under 23 seeds with pushes waiting and under 8 with cursors, and at a shard of
  2,500 under 439 and under 205; at any size a count at the share's mean or above passes, and the
  test job's twenty seeds can fail neither.
  The crash tests and certification's kill clauses run with 40 MiB, about the least.
- A simulated network's source sends cursors no larger than its workload's. Cursors as large
  as a cursor may be fill a socket whose peer vanished without the close reaching it: the
  served connector's writes to it, its HTTP/2 pings among them, then wait without end, so its
  keepalive never ends the connection, and a connector stopping gracefully drains that
  connection without end before it listens again. On a real network the kernel's
  retransmission timeout ends such a socket in minutes; the simulator's never does. A deadline
  on a stopping connector's drain would bound it.
- A source may keep 2 MiB of dictionaries a read at the defaults, where a frame may hold
  27 MiB. More memory or fewer partitions raise it.
- A following run needs more partitions than the unbounded partitions it follows, and is
  refused with `partitions_too_few` where it has too few. Its memory does not bound how many it
  follows, only what they keep together: 256 followed partitions read by 257 partitions run
  within the default 256 MiB. Each read then keeps a part of twice the least dictionary, so a
  schema's message may take 52,428 bytes and a table 936 columns, against 419,430 bytes and
  7,489 columns at the defaults. Followed reads that would keep more than the parts no other
  read holds, less one, fail with `limit_exceeded`: within the default memory, 63.5 MiB
  together, about 250 KiB each of 256. A served source carries the engine 199 calls at once (ADR 0042), so
  a run follows at most 197 of its unbounded partitions, however much memory it has: beyond them
  it is refused with `source_calls_exceeded` where its partitions are more than 198, and with
  `partitions_too_few` where they are not.
- Where the reads' share holds fewer parts of twice the least dictionary than the partitions,
  the reads that are not followed read no more at once than the parts, each waiting for one: at
  the default memory, 128.
- A table wider than the derived column limit, 7,489 at the defaults, is refused where its
  source pushes it.
- A partition that ends where it last checkpointed seals that cursor again: the commit records
  it, and the commit's frame holds it, so the seal waits for room in the cursors' share as a
  checkpoint does. The coordinator commits as soon as a cursor waits, whatever it heard last.
- A partition let go while a job of its own runs on the compute pool waits, as it is dropped,
  for that job to end, on the runtime's thread that drops it: the one wait of the runtime on the
  pool, for bounded CPU work that never waits on the runtime. An attempt is let go no sooner
  than the longest of its jobs running then ends, a piece's lowering or a chunk's parse.
- A lane's call into its destination ends with the attempt: a write that never returns keeps
  no attempt that failed from ending. An in-process destination's write behind a lane too full
  to queue on is bounded by nothing: the partition waits for its lane with no deadline. A served
  one's is bounded by the host's write deadline.
- A normalized piece holds a quarter of the budget from before its split until its parts are
  cut, so a little over two normalize at once while pushes fill their share; and a unit of a
  few rows takes three trips to the compute pool before its parts are lowered in one.
- A JSON flush whose batches take more than twice its text waits for its lowering's room with
  what building takes, is planned and cut whole before any of it is lowered, and is built
  again where the budget lacks room for its lowering beside it. A normalized JSON flush holds a
  quarter of the budget from before it is observed until its last piece is split, as a
  normalized piece does from before its split.
- A wait for bytes that reaches the budget's deadline is the last resort: a destination that
  stops writing, or a commit that never lands, fails at its call's own deadline first. A
  destination whose calls take longer than thirty minutes needs `connector_wait` raised, which
  raises the budget's deadline with it.
- A read that hangs while it holds its slot holds the reads waiting for one as long as it
  hangs: no deadline bounds a source's read.
- `ErrorKind` has `Memory`, and `Admission::admit` and `Admission::charge` return a `Result`;
  an `Admission` says the limits it admits within, and a `PartitionSink` holds what is sent to
  them. A run's report counts every wait on the budget and its time on the engine's clock, by
  share, a read's wait for a slot among them, and a served frame's wait to be decoded apart from
  the pushes it waits among; every budget has a clock and a deadline for the bytes it is asked
  for. The wire's limits hold `dictionary_bytes`, with a least of 256 KiB.
- A cut that reads values costs a trip to the compute pool a piece; one that reads none takes
  none.
- The dictionary and staged limits follow the frame limit: one and four frames' bytes.
- An array-form JSON push is scanned for its elements twice: once to cut it, which counts each
  chunk's, and once as each chunk is parsed. A chunk of JSON lines is counted by its line ends
  in its job, beside its parse, blank lines included: its builders are presized for at least
  its records, so blank lines among them can trip its first parse, which then costs an
  observation, a build again and the reservation it plans.
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
  storage in proportion. The nulls of a nested column's fields the shredder builds, and what a
  JSON push of sparse records becomes beyond three times its text, are reserved before they are
  built (ADR 0040). Nor are two things a JSON
  push builds beyond what it was admitted or reserved for: the document the exact parse builds
  of a record, up to twelve times its text, and what reading a part held as JSON renders, up
  to ten times its text beyond its piece's reservation.
