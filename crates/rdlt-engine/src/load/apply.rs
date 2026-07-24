//! The two destination-facing apply primitives, shared by the live loader and
//! WAL replay so both drive the session through the SAME lowering seam.
//!
//! A schema delta and a batch each lower for the destination's capabilities
//! exactly once, in one place: the live `Loader::process` and both legs of
//! `wal::resume::replay` (the pre-loop table ensure and the per-record delta arm)
//! route through here, so recovery reproduces the live path byte-for-byte.

use rdlt_connector::{DestCapabilities, LoadSession, RecordBatch};
use rdlt_core::{RdltError, StateDoc, TableName, TableSchema, WriteMode};

use super::lowering;

/// Apply one schema delta to the session: lower the rich engine schema for the
/// destination, ensure the (lowered) table exists, and record the ORIGINAL
/// schema's content hash in the pipeline state. Re-ensuring a table a
/// destination already holds is tolerated — callers deduplicate the code, not
/// the ensure calls.
pub(crate) async fn apply_delta(
    session: &mut dyn LoadSession,
    state: &mut StateDoc,
    caps: &DestCapabilities,
    schema: &TableSchema,
    mode: &WriteMode,
) -> Result<(), RdltError> {
    let lowered = lowering::lower_schema(schema, caps);
    session
        .ensure_table(&lowered, mode)
        .await
        .map_err(|e| crate::runtime::run::classify_dest_error(&e))?;
    state
        .schema_hashes
        .insert(schema.table.clone(), schema.content_hash());
    Ok(())
}

/// Apply one batch to the session: lower it for the destination's capabilities,
/// then write it under `table`. Row/byte accounting stays with the caller — it
/// is measured from the un-lowered batch and differs between the live loader and
/// replay.
pub(crate) async fn apply_batch(
    session: &mut dyn LoadSession,
    caps: &DestCapabilities,
    table: &TableName,
    batch: &RecordBatch,
) -> Result<(), RdltError> {
    let lowered = lowering::lower_batch(batch, caps)?;
    session
        .write(table, lowered)
        .await
        .map_err(|e| crate::runtime::run::classify_dest_error(&e))
}
