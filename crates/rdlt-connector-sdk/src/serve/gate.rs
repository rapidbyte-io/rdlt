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

use rdlt_connector::core::commit::CommitMeta;
use rdlt_connector::core::schema::{Column, ColumnType};
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

/// Every identifier a decoded `CommitMeta` carries, through the same
/// ceiling as the session's top-level ids: beyond its own load id and
/// its state's pipeline id, the STATE document carries identifier
/// SUB-MAPS — cursor keys are stream names, schema-hash keys are table
/// names, the last commit names a load id, and the engine version is
/// free text quoted like an identifier — all retained by the backend
/// or quoted by its refusals. Cursor VALUES stay opaque documents
/// (bounded by the document ceiling that admitted the meta), never
/// walked.
pub(super) fn gate_commit_meta(meta: &CommitMeta) -> Result<(), session_reply::Reply> {
    refuse_oversized_identifier("load id", meta.load_id.as_str())?;
    refuse_oversized_identifier("pipeline id", meta.state.pipeline.as_str())?;
    for stream in meta.state.cursors.keys() {
        refuse_oversized_identifier("stream name", stream.as_str())?;
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
    refuse_oversized_identifier("column name", &column.name)?;
    // Exhaustive on purpose: a future ColumnType arm that carries named
    // fields must fail compilation here rather than silently riding the
    // non-recursive arms.
    match &column.column_type {
        ColumnType::Struct { fields } => fields.iter().try_for_each(gate_column),
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
