# ADR 0053: Transforms

Status: accepted, 2026-11-06.

## Context

A stream's rows often need reshaping before they land: a column renamed, a value masked, a
constant added, a row kept out. A pipeline's configuration may be written by a tenant
(ADR 0037), and the engine keeps exactly-once, the memory budget (ADR 0039) and its
determinism under simulation whatever the configuration says.

## Decision

- **Placement.** Transforms run once per batch at the start of `partition::write::write`.
  - By then a JSON push is shredded and an Arrow push has had its JSON columns checked. Nothing
    has been split for normalization or resolved against a table.
  - Every push kind passes once: Arrow, JSON and changes. Schema evolution, naming, lowering and
    normalization apply to the output.
  - A stream that includes every column and transforms nothing takes no new step.
- **The stream.** A stream lists an ordered chain of transforms and either an `include_only` or
  an `exclude`.
  - `include_only` is enforced as each batch reaches the stage, since the wire carries only
    exclusions. No Rust identifier is named `include`, which `cargo xtask lint` refuses wherever
    it appears so that no macro can make it `include!` (ADR 0048).
  - Keys (the source's primary key and the columns a merge matches by), change columns and the
    source's change time are always included, and pass every transform as they arrive. The plan
    refuses an op naming one, `transform_invalid`, and the stage refuses a chain whose output
    drops or retypes one, `transform_schema`.
  - A transform may declare, for each output column, the input column it passes on unchanged.
    Ops and SQL do; code need not. The stage refuses a declaring transform whose output under a
    key's, a change column's or the change time's name is not that column passed unchanged,
    `transform_invalid`: a document can name SQL, so SQL is held to the keys code is trusted to
    keep.
  - A declared schema is fitted as the stage makes it.
- **Trust.** Declarative ops are data. The engine validates and bounds them, and they are safe
  in a tenant's document. A Rust transform is code: it runs in process, only as the embedder's
  own trusted code, and a document cannot name one.
- **The contract.**
  - A transform works row by row: what it makes of a row depends on that row alone, so the
    stage may cut a batch and its parts give what the whole does (certification's `T-ROWWISE`
    checks this). `keep` answers one value a row, a null keeping nothing; `apply` keeps the row
    count and order and matches the schema `output` declared for the input.
  - Code is trusted to pass keys, change columns and the change time on with their values: the
    stage checks only that each passes with its name and type.
  - A breach is `transform_contract`: of kind `Internal` for a transform that declares a bound,
    which the engine's own ops and SQL do, and of kind `Transform` for code.
  - A failure is `transform_failed`, of kind `Transform`, never retried: a deterministic
    transform fails the same way again.
- **Filters never leave stale rows.**
  - An append or a replace drops a row its filter does not keep. A change log drops the insert
    or update.
  - A change stream matched by key turns that insert or update into a delete of its key, which
    the stream's delete mode then applies: a hard delete removes the row, a soft one records
    the deletion.
  - Deletes and truncates always pass a filter.
  - A plan is refused, `transform_invalid`, where the rewritten delete could not land: a change
    stream whose deletes are ignored, or a merge or history stream fed by full or incremental
    reads.
  - The filters' work is reported: `StreamReport::filtered` counts the rows they dropped and the
    upserts they turned into deletes.
- **Changes.**
  - Transforms see a change batch's data columns alone. The stage sets the change columns aside
    and puts them back after the output.
  - A transform may declare its lineage: for each output column, the input columns it is
    computed from. Ops and SQL do. After each transform, an output column is unchanged if and
    only if it reads at least one column and every column it reads is unchanged. A constant is
    written.
  - A transform declaring no lineage refuses a batch whose updates flag columns unchanged,
    `transform_partial`, so no derived column is written from a value the source did not send.
    A row where an output column reads both an unchanged and a changed column fails
    `transform_partial` too: its value would be computed from a placeholder.
  - A column flagged unchanged holds a placeholder, so a filter that reads one cannot judge its
    row. A filter may declare the columns `keep` reads; a batch whose updates flag one of them,
    or any column where the filter declares none, is refused, `transform_partial`. A filter on a
    key passes updates that leave other columns unchanged.
  - An op naming a column a declared schema lacks is refused when the attempt starts,
    `transform_schema`. A batch lacking it is passed by, as a JSON stream's drift may.
- **Encoded columns.** A dictionary's or a run-end encoding's rows, decoded, are not linear in
  the bytes it keeps, so no transform could declare a bound on computing from one.
  - The stage decodes a transformed stream's dictionary and run-end encoded columns before its
    first transform, with lowering's decoder, and asks for the room the decoded rows take, as
    lowering measures a decode, before it does. Decoded text keeps 32-bit offsets, as its
    values do, so a part an encoded column of which would decode beyond 2 GiB is cut as a part
    over its room is. Lowering decodes them anyway, so the work moves earlier rather than grows.
  - A decoded column is nullable, as lowering takes it into a table, whatever the field of the
    values a dictionary or a run-end encoding names says: Arrow checks neither's values against
    it, and a key's null is refused as lowering refuses it.
  - Transforms, ops and SQL alike, then see every column flat: a filter on a categorical or a
    Parquet dictionary string is computed like any other. A stage that only includes columns
    decodes nothing and shares every array.
- **Memory.**
  - A flush whose stage transforms, or whose change batches carry unchanged flags the stage
    moves, holds one lowering request. That is reserved while the partition holds only its
    pushes, or it is what a JSON flush of the stream reserved whole, as a normalized flush does.
    Every JSON flush of a stream with a stage is built inside a whole request and reaches the
    stage.
  - Before each transform runs, its declared bound must fit what is left: a bound linear in the
    input's rows and column bytes, with the output's arrays and the stage's own work on the
    rows. That is, for a filter, its answer and what the filter rule makes of the rows it does
    not keep (a copy of those it keeps, or the ops of a change stream matched by key,
    rewritten), and a change batch's flags rebuilt along the transform's lineage. Moving the
    flags to the kept columns' places asks for room before the first transform too.
  - A batch whose bound passes what is left is cut in halves. A row that alone does not fit
    fails `row_exceeds_budget`: of kind `Transform` where a transform asked for the room, and of
    kind `Source`, as lowering's own refusal is, where only moving its flags did.
  - Output beyond its bound is `transform_bound`, a bug. Code declares no bound: its output is
    measured after it runs, as the engine's own work is, and a batch whose output does not fit
    is cut in halves too.
  - What a unit's output keeps alive beyond its pushes, each allocation counted once however many
    of the parts a batch was cut into share it, and the arrays that are none of the pushes' own,
    is held with it, and the rest of the request is lent to its lowering. The partition never
    waits on the budget while it holds what its transforms made.
  - A stage that only includes columns runs on the partition's task on batches that carry no
    unchanged flags. It shares every array it keeps and copies no row, allocating only for each
    column, never for each row, and reserves nothing.
  - A batch of no rows leaves one of no rows, as a stream with no stage keeps it; a batch whose
    every row a filter drops leaves none.
- **Ops.**
  - `rename` renames top-level columns and shares every array.
  - `constant` adds a column holding one text, integer or boolean.
  - `mask` replaces values with their `HMAC-SHA256` in lower-case hex (text and bytes in every
    layout, a dictionary's values hashed once), a redaction, or a column of the null type.
  - A mask reaches nested fields through structs, their siblings shared. A key is a
    `rdlt_connector::Secret` and never appears in errors, events or reports.
  - A column a batch lacks is passed by. There is no expression language: SQL is the
    expression language.
  - The plan bounds what ops hold: at most `TRANSFORMS_PER_STREAM` transforms, `NAMED_COLUMNS`
    columns an op names or a stream includes, and `LITERAL_BYTES` a constant or a redaction.
- **SQL.** `rdlt-sql` makes a transform of one query of the table `input`.
  - The query is a projection over an optional filter, a table alias allowed. A join,
    aggregate, sort, window, limit, distinct, subquery, union or `unnest` is refused when it is
    parsed, `transform_invalid`, naming what it is.
  - It is parsed by sqlparser, as DataFusion carries it, in its generic dialect but for the
    keywords whose own syntax holds expressions, and planned by DataFusion's SQL front end,
    against one table with the batch's schema. sqlparser reads such a keyword, where its own
    syntax fails, again as a function's name, so text failing inside a nest of them is read
    twice a level, time exponential in the nest: 22 nested `CAST`s broken at the innermost took
    five seconds. The 19 keywords that do so are read as their own syntax alone, a name only
    quoted, and a test nests every keyword sqlparser knows to hold every parse to linear time. A
    type that holds other types is read first as a typed string's and again as its own syntax, a
    call or a comparison, and can hold expressions, so where an operand stands one is refused at
    once, as shape refuses it anyway. `INTERVAL` is read as a typed string's type, its operand
    with it, and again as its own syntax, so it is refused where an operand stands too: twenty
    before a 16 KiB list took sixteen seconds. A sweep of every keyword sqlparser knows, each
    repeated before an operand that fails, finds no other read so. Past every path found, the
    parser's work is held to 16 a token of the text, backtracks included, and the text is
    refused beyond it: a read costs one and every token a typed string's type tried there may
    take, up to the first bracket within its parentheses, which tables built once for the text
    give at once. No admitted query in the tests spends 6.5 a token, a list being charged by
    the five reads at most that reach it, and any text read again a level is linear in its
    length. Only the listed functions are registered, so no other can be planned, and no
    volatile or unbounded function is listed. Types are coerced by DataFusion's analyzer, and
    each call is rewritten as its function rewrites it before DataFusion runs it, where the
    rewrite makes another kind of expression: `coalesce` becomes a `CASE`. A rewrite into
    another call is not taken: a listed function's only folds literals, and `concat`'s folds its
    empty literals away into a call it refuses to run.
  - Its projection and predicate become DataFusion physical expressions, which the engine
    evaluates batch by batch on its pool. No session executes them, so DataFusion starts no
    task or thread and keeps no memory pool. A bare column is passed on without a copy.
  - A query is planned once for each input schema it meets, however many batches ask at once,
    and keeps the plans of the eight it was asked for most recently; planning one schema holds
    up no other's. Identifiers
    are matched as written, time is in UTC, and an output expression other than a bare column is
    named.
  - SQL is hostile input: text of at most 64 KiB, expressions nested at most 64 deep, literals
    of at most 4 KiB, regular expressions and `LIKE` patterns only as literals, the former
    compiled under 1 MiB, operators from a list, casts only to the listed scalar types, and
    output columns within the engine's limit. The listed types are booleans, integers signed
    and unsigned, floats, decimals, text, dates, times and timestamps, under their names and the
    other names rdlt-sql lists for them (`CHARACTER VARYING`, `DEC`, `DATETIME`, `INT64` and the
    like), which a query is planned with as DataFusion names it. `INT8` is Postgres's name, 64
    bits, as DataFusion reads it, while `UINT8`, `INT16`, `INT32` and `INT64` count their bits.
    `INTERVAL`, binary, `MEDIUMINT`, `BIGNUMERIC`, a timestamp or a `DATETIME` at a precision
    DataFusion does not plan (`TIMESTAMP(5)`), a time with a precision or a time zone
    (`TIME(2)`), and every nested type are refused when the query is parsed. A column sharing
    its name with a type that holds others, such as `map`, `table` or `tuple`, is quoted where
    `(` or `<` follows it, as in `"map" < 5`; one named `interval`, or as a keyword whose syntax
    holds expressions, such as `cast`, is quoted anywhere.
    The parser builds a chain such as `a + b + c` in a loop, as deep as it is long, which its
    recursion limit does not bound, so the depth is measured on the parsed tree: each expression
    lies within as many as lie above it, and a chain of operators is as deep as it has
    operators. A tree deeper than the limit is refused before anything else reads it.
  - A query's text is parsed, measured and checked, and what is refused dropped, on a stack of
    its own, 4 KiB for each byte of the text, at least 1 MiB and at most 256 MiB, so a short
    query reserves little where a host commits memory strictly. The deepest tree text builds is
    a nested type, which sqlparser parses and drops a frame a level outside its recursion
    limit, so the stack it takes grows with the text. Of every type the dialect nests, `TABLE(a `
    takes the most: at 1 KiB, 8 KiB and 64 KiB, 647 KiB, 4.8 MiB and 38.0 MiB on the dev
    profile and 751 KiB, 5.6 MiB and 44.5 MiB built for coverage, 0.73 KiB a byte at most, and
    36.1 MiB at 64 KiB on the release profile: more than five times under the stack at each
    length. A type is the one part of a query
    its expressions' depth does not bound, so a cast may name only a listed scalar type, and an
    admitted query is flat in its types and at most 64 deep in its expressions before anything
    clones, plans or runs it.
  - An admitted query plans and runs on a thread's stack: planning starts with 4 MiB of it, and
    DataFusion and the bound's own walk grow it as they recurse.
  - A panic in sqlparser or DataFusion while a query is parsed or planned refuses the query
    rather than ending the embedder's process; a batch is evaluated on the engine's pool, which
    contains one already. The panic still reaches the process's panic hook first.
  - No output column's name begins with the engine's prefix. A query declares, for each output
    column, the input column it passes on unchanged, so the stage refuses one computing a column
    under a key's, a change column's or the change time's name.
  - DataFusion's errors can quote the value they failed on, so none is kept. A refusal names the
    stream, the column and the function, never a value: `transform_invalid` when parsed,
    `transform_schema` when planned for a schema, `transform_failed` for a batch.
  - Each listed function declares its bound in terms of its arguments: lower and upper case
    three times their input for Unicode's case mappings, a concatenation its parts, a
    replacement by a literal one growth a byte, and a number, a time or a truth its type's
    width. An expression's bound is twice its nodes' outputs, for builders that double, and a
    view builder's first block, 16 KiB, for each array of text.
  - Some of DataFusion's and Arrow's kernels size what they make by the whole buffer a slice of
    text was cut from, and the stage cuts batches. So a column of text or bytes an output
    column computes from is copied to hold its own rows alone, where a batch holds it in a
    larger buffer, before the projection runs on it, and one the filter reads before the filter
    does; the bound counts each column's copy once, as no two of them are held at once.
  - Text is computed only from flat columns, which a stream's stage decodes encoded ones into
    before a query sees them. Handed an encoded column directly, outside the stage, rdlt-sql
    refuses an expression computing from it when it is planned.
  - Its lineage gives each output column the columns it references, and its filter reads its
    predicate's columns, so a filter passes an update that sent every column it reads.

## Consequences

- A transformed flush holds a quarter of the budget while it lowers, as a normalized JSON flush
  does, so fewer of them lower at once than flushes of streams that do not transform. A JSON
  flush of a stream that only includes columns holds as much while it is built.
- An embedder's transform must declare its lineage to take partial updates, and a filter the
  columns it reads.
- DataFusion's crates build more than runs. `datafusion-functions` builds its execution crate
  and an object store's local files; `datafusion-sql` builds the nested and aggregate function
  crates, and the default function features through them, with `base64`, `hex` and `uuid`; and
  `datafusion-common` takes Arrow's CSV, JSON and IPC readers, IPC with `lz4` and `zstd`, a C
  library built from source. No feature of theirs leaves these out. Arrow and DataFusion are
  upgraded together.
- SQL handed a dictionary or a run-end encoded column directly, outside a stream's stage, passes
  it on and tests it for null but computes nothing else from it.
- The stack SQL text is parsed on is sized for sqlparser optimised, as a release builds it, and
  as this workspace's dev profile builds it too: unoptimised, nested `TABLE(a ` takes 4 KiB of
  stack a byte of text, 252 MiB at 64 KiB, all its stack holds, and a nest of 1 KiB outgrows
  its 4 MiB. An embedder's own dev profile builds sqlparser optimised alike, as README says.
