//! The serve-side gate family under one roof — the third of the three
//! gate seats (the SPI's `gate.rs` owns the shared ceilings and
//! document rules; the client's `gate.rs` owns the dialing side's
//! renders and refusals; THIS module owns what a served connector
//! holds inbound wire values to before its backend sees them).
//!
//! THE FAMILY RULE, stated once: serve gates are LENGTH-ONLY at the
//! wire — an identifier is refused for its size, never rewritten —
//! and content escaping belongs to the DISPLAY renders on the side
//! that displays (the client's bounded renders, a host's own logs).
//! The ceilings themselves are the SPI's one set
//! (`MAX_WIRE_IDENTIFIER_BYTES`, the document and cursor ceilings),
//! referenced from here rather than duplicated; the count caps that
//! mirror the client's stream gates ride inside the functions that
//! enforce them, spelled with the mirror-is-the-contract rule at each.

use rdlt_connector::core::commit::{CommitMeta, WriteMode};
use rdlt_connector::core::schema::{Column, ColumnType, TableSchema};
use rdlt_connector::{gate, source};
use rdlt_connector_protocol::proto::{Classification, session_reply};

use super::wire;

/// The identifier-length half of the wire's identifier rule at the
/// session's inbound seats: ensured names and session ids are retained
/// for the session's lifetime, so a rogue client's multi-megabyte name
/// is memory and log swelling, refused at the door.
pub(super) fn refuse_oversized_identifier(
    kind: &str,
    value: &str,
) -> Result<(), session_reply::Reply> {
    if value.len() > gate::MAX_WIRE_IDENTIFIER_BYTES {
        return Err(session_reply::Reply::Error(wire::error_frame(
            Classification::Fatal,
            format!(
                "a session {kind} of {} bytes exceeds the {}-byte wire identifier ceiling — \
             refused at the session boundary",
                value.len(),
                gate::MAX_WIRE_IDENTIFIER_BYTES
            ),
            None,
        )));
    }
    Ok(())
}

/// One state sub-map's entry count against the flood ceiling.
fn refuse_entry_flood(seat: &str, entries: usize) -> Result<(), session_reply::Reply> {
    if entries > MAX_STATE_SUBMAP_ENTRIES {
        return Err(session_reply::Reply::Error(wire::error_frame(
            Classification::Fatal,
            format!(
                "a committed state carries {entries} {seat} — over the \
                 {MAX_STATE_SUBMAP_ENTRIES} ceiling; a state that grew here honestly \
                 needs its dead entries pruned"
            ),
            None,
        )));
    }
    Ok(())
}

/// The greatest number of entries either identifier sub-map of a
/// committed state document may carry. The document ceiling that
/// admitted the meta bounds its BYTES — about 550k minimal cursor
/// entries, about 105k schema hashes — but every one of those becomes
/// a map node plus an owned key, planted into whatever the backend
/// retains and re-serialized by every later state read. This bounds
/// the count instead.
///
/// WHAT IT CAN REFUSE, said plainly because the count that matters
/// ACCUMULATES: a state's cursors are keyed by every stream name the
/// pipeline has ever committed, not by the streams a source declares
/// today, and nothing prunes the ones that stop appearing. A pipeline
/// minting new stream names forever — per tenant, per day — grows the
/// map monotonically and will, eventually, meet this ceiling and have
/// every publish refuse. The ceiling is set far above any plausible
/// arrival at that point (a pipeline adding a hundred new stream names
/// every day reaches it in seven years) and below what the byte gate
/// admits, so it trims the flood without being the wall an honest
/// pipeline hits first. The remedy when it IS met is to prune the
/// dead cursors from the pipeline's state; making the ceiling
/// configurable is the standing alternative and not taken here.
const MAX_STATE_SUBMAP_ENTRIES: usize = 256 * 1024;

/// Every identifier a decoded `CommitMeta` carries, through the same
/// ceiling as the session's top-level ids: beyond its own load id and
/// its state's pipeline id, the STATE document carries identifier
/// SUB-MAPS — cursor keys are stream names, schema-hash keys are table
/// names, the last commit names a load id, and the engine version is
/// free text quoted like an identifier — all retained by the backend
/// or quoted by its refusals. Each sub-map is held to a COUNT as well
/// as to per-key lengths: length gates alone admit a flood of tiny
/// keys.
///
/// Cursor VALUES are counted too, though never inspected. They stay
/// opaque documents — what a cursor MEANS is the source's business —
/// but the meta's byte ceiling bounds only what arrived, and a dense
/// document becomes far more once parsed. They are planted into
/// whatever the backend retains and re-serialized by every later state
/// read, so the same node count the read seat holds a resume cursor to
/// applies here.
pub(super) fn gate_commit_meta(meta: &CommitMeta) -> Result<(), session_reply::Reply> {
    refuse_oversized_identifier("load id", meta.load_id.as_str())?;
    refuse_oversized_identifier("pipeline id", meta.state.pipeline.as_str())?;
    refuse_entry_flood("cursors", meta.state.cursors.len())?;
    refuse_entry_flood("schema hashes", meta.state.schema_hashes.len())?;
    for (stream, cursor) in &meta.state.cursors {
        refuse_oversized_identifier("stream name", stream.as_str())?;
        if let Err(message) = refuse_dense_cursor("a committed cursor", cursor.as_value()) {
            return Err(session_reply::Reply::Error(wire::error_frame(
                Classification::Fatal,
                message,
                None,
            )));
        }
    }
    for table in meta.state.schema_hashes.keys() {
        refuse_oversized_identifier("table name", table.as_str())?;
    }
    if let Some(last) = &meta.state.last_commit {
        refuse_oversized_identifier("load id", last.load_id.as_str())?;
    }
    refuse_oversized_identifier("engine version", &meta.state.engine_version)?;
    Ok(())
}

/// One column's name through the identifier ceiling, nested struct
/// fields included: `ColumnType::Struct` nests `Column`s recursively,
/// and a nested name is retained by the session and reaches backend
/// error text exactly like a top-level one (`ScalarList` carries a bare
/// element type — no names). Recursion depth is bounded upstream by the
/// JSON parse that produced the schema, which refuses past serde_json's
/// own nesting limit.
pub(super) fn gate_column(column: &Column) -> Result<(), session_reply::Reply> {
    let mut counted = 0usize;
    gate_column_at(column, &mut counted)
}

/// [`gate_column`] carrying the running field count: the top-level
/// column cap bounds how many columns a schema declares, but a single
/// column can nest hundreds of thousands of fields inside the same
/// document, and every one of them is a retained `Column` the backend's
/// DDL walks. The whole tree is held to the same number as the top
/// level — a schema is as wide as its fields, wherever they sit.
fn gate_column_at(column: &Column, counted: &mut usize) -> Result<(), session_reply::Reply> {
    *counted += 1;
    if *counted > MAX_ENSURED_COLUMNS {
        return Err(session_reply::Reply::Error(wire::error_frame(
            Classification::Fatal,
            format!(
                "an ensured table declares more than {MAX_ENSURED_COLUMNS} columns once \
                 nested fields are counted"
            ),
            None,
        )));
    }
    refuse_oversized_identifier("column name", &column.name)?;
    // Exhaustive on purpose: a future ColumnType arm that carries named
    // fields must fail compilation here rather than silently riding the
    // non-recursive arms.
    match &column.column_type {
        ColumnType::Struct { fields } => fields
            .iter()
            .try_for_each(|field| gate_column_at(field, counted)),
        ColumnType::Scalar { .. } | ColumnType::ScalarList { .. } => Ok(()),
    }
}

/// Every identifier a read's declared stream spec carries, through the
/// wire identifier ceiling — the source-side mirror of the session
/// seats' identifier gate, same ceiling, length-only (content escaping
/// belongs to the display renders on the side that displays) — plus
/// the COUNT caps the client's stream gate holds the same collections
/// to, mirrored BY VALUE (the crates cannot share the constants; the
/// mirror IS the contract — primary-key fields ≤ 64, type hints ≤
/// 4096, the same numbers the client seat caps, and both sides say
/// so): a spec of thousands of tiny gate-legal keys passes every
/// per-value gate within the document ceiling otherwise, and the spec
/// is RETAINED for the read's lifetime.
pub(super) fn refuse_oversized_spec_identifiers(spec: &source::StreamSpec) -> Result<(), String> {
    let refuse_count = |seat: &str, n: usize, cap: usize| {
        if n > cap {
            return Err(format!(
                "a read declares {n} {seat} — over the {cap} ceiling"
            ));
        }
        Ok(())
    };
    if let Some(key) = &spec.primary_key {
        refuse_count("primary-key fields", key.len(), 64)?;
    }
    refuse_count("type-hint fields", spec.type_hints.len(), 4096)?;
    let refuse = |kind: &str, value: &str| {
        if value.len() > gate::MAX_WIRE_IDENTIFIER_BYTES {
            return Err(format!(
                "a read {kind} of {} bytes exceeds the {}-byte wire identifier ceiling — \
                 refused at the wire boundary",
                value.len(),
                gate::MAX_WIRE_IDENTIFIER_BYTES
            ));
        }
        Ok(())
    };
    refuse("stream name", spec.name.as_str())?;
    for field in spec.primary_key.iter().flatten() {
        refuse("primary-key field", field)?;
    }
    if let Some(field) = &spec.cursor_field {
        refuse("cursor field", field)?;
    }
    for field in spec.type_hints.keys() {
        refuse("type-hint field", field)?;
    }
    Ok(())
}

/// The greatest number of NODES a resume cursor may carry once parsed.
/// The cursor's 4 MiB byte ceiling bounds what arrives; it does not
/// bound what the arrival becomes, and structure is where an untyped
/// document expands worst: `[0,0,0,…]` spends two wire bytes per
/// element and buys a `Value` node — tens of bytes plus vector slack —
/// so a legal 4 MiB cursor reaches a couple of million nodes, and the
/// read RETAINS it for its whole lifetime, once per admitted read.
/// Counting the nodes bounds that dimension without inspecting a
/// single value: the cursor stays the opaque document its contract
/// says it is, and only its size is judged. At this ceiling a retained
/// cursor costs a few megabytes of structure beside the payload its
/// byte ceiling already bounds; an honest resume cursor — a watermark,
/// a page token, a handful of per-partition offsets — is orders of
/// magnitude below it.
pub(super) const MAX_CURSOR_NODES: usize = 64 * 1024;

/// A parsed cursor's node count against the ceiling. The walk is
/// ITERATIVE with its own stack: a cursor's nesting is bounded by the
/// parser that produced it, but a recursive walk would make this gate
/// the thing that overflows.
///
/// What this bounds is RETENTION, which is what a read holds for its
/// lifetime and what the concurrency ceiling multiplies. It does not
/// bound the parse: the document is already a `Value` by the time the
/// walk runs, so the parse expansion has been paid whatever the count
/// says — and the walk's own stack holds a pointer per child of every
/// node it has popped, so crossing the ceiling is not free either. The
/// gate stops the flood from being KEPT; the byte ceiling above is
/// what stops it from being large.
pub(super) fn refuse_dense_cursor(field: &str, value: &serde_json::Value) -> Result<(), String> {
    let mut pending = vec![value];
    let mut nodes = 0usize;
    while let Some(node) = pending.pop() {
        nodes += 1;
        if nodes > MAX_CURSOR_NODES {
            return Err(format!(
                "{field} carries more than {MAX_CURSOR_NODES} document nodes — a resume cursor \
                 is a position, not a payload"
            ));
        }
        match node {
            serde_json::Value::Array(items) => pending.extend(items.iter()),
            serde_json::Value::Object(fields) => pending.extend(fields.values()),
            _ => {}
        }
    }
    Ok(())
}

/// The greatest number of columns one ensured schema may declare, and
/// of key columns one merge mode may name. The document ceiling that
/// admitted the schema bounds its bytes; a minimal column object costs
/// a few dozen of them, so a legal `table_schema_json` declares tens of
/// thousands of columns and a merge key names millions of strings —
/// each an owned `Column` or `String` the session RETAINS and the
/// backend's DDL walks. The cap is the wire batch's column cap: a
/// schema wider than any batch that could fill it is refused here
/// rather than at the first Write.
const MAX_ENSURED_COLUMNS: usize = 4096;

/// An ensured schema's collection COUNTS, beside the per-identifier
/// lengths every name rides: length gates alone admit a flood of tiny
/// legal names.
pub(super) fn refuse_ensure_counts(
    schema: &TableSchema,
    mode: &WriteMode,
) -> Result<(), session_reply::Reply> {
    refuse_collection_flood("columns", schema.columns.len(), MAX_ENSURED_COLUMNS)?;
    if let WriteMode::Merge { key } = mode {
        refuse_collection_flood("merge key columns", key.len(), MAX_ENSURED_COLUMNS)?;
    }
    Ok(())
}

/// One ensured collection's count against its ceiling.
fn refuse_collection_flood(
    seat: &str,
    entries: usize,
    cap: usize,
) -> Result<(), session_reply::Reply> {
    if entries > cap {
        return Err(session_reply::Reply::Error(wire::error_frame(
            Classification::Fatal,
            format!("an ensured table declares {entries} {seat} — over the {cap} ceiling"),
            None,
        )));
    }
    Ok(())
}
