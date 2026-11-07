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
  - A declared schema is fitted as the stage makes it.
- **Trust.** Declarative ops are data. The engine validates and bounds them, and they are safe
  in a tenant's document. A Rust transform is code: it runs in process, only as the embedder's
  own trusted code, and a document cannot name one.
- **The contract.**
  - A transform is a pure function of a batch. `keep` answers one value a row, a null keeping
    nothing; `apply` keeps the row count and order and matches the schema `output` declared for
    the input.
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
  - A column flagged unchanged holds a placeholder, so a filter that reads one cannot judge its
    row. A filter may declare the columns `keep` reads; a batch whose updates flag one of them,
    or any column where the filter declares none, is refused, `transform_partial`. A filter on a
    key passes updates that leave other columns unchanged.
  - An op naming a column a declared schema lacks is refused when the attempt starts,
    `transform_schema`. A batch lacking it is passed by, as a JSON stream's drift may.
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

## Consequences

- A transformed flush holds a quarter of the budget while it lowers, as a normalized JSON flush
  does, so fewer of them lower at once than flushes of streams that do not transform. A JSON
  flush of a stream that only includes columns holds as much while it is built.
- An embedder's transform must declare its lineage to take partial updates, and a filter the
  columns it reads.
