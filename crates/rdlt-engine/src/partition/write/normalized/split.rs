//! A normalized piece split into its tables' parts on the compute pool, and what the split
//! holds beyond the unit the piece is cut from.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use rdlt_connector::StreamName;
use rdlt_connector::cost::{Allocations, ViewBuffers, array_bytes, arrays, listed_bytes};
use rdlt_wire::limits::count;

use super::super::{PartitionContext, PartitionJob};
use super::{keys, on_pool};
use crate::error::{Error, codes};
use crate::normalize::{self, Lineage, Part, PartSchemas, Shape};
use crate::table::{key_values, with_columns};

/// `parts`, a piece of a unit, normalized as `shape` on the compute pool, their parts' schemas
/// those `schemas` kept of earlier batches of their schema where they hold, and kept there.
pub(super) async fn split_kept(
    job: &PartitionJob,
    context: &PartitionContext,
    schemas: &mut PartSchemas,
    shape: &Arc<Shape>,
    parts: &[RecordBatch],
) -> Result<Vec<Part>, Error> {
    let (shape, stream, parts) = (Arc::clone(shape), job.stream.clone(), parts.to_vec());
    let mut kept = std::mem::take(schemas);
    let splitting = move || {
        let parts = split(&stream, &parts, &shape, &mut kept);
        (parts, kept)
    };
    let (parts, kept) = on_pool(context, splitting).await;
    *schemas = kept;
    parts
}

/// `parts`, one batch once concatenated, normalized as `shape`, the schemas of what they split
/// into kept in `schemas`.
pub(in crate::partition::write) fn split(
    stream: &StreamName,
    parts: &[RecordBatch],
    shape: &Shape,
    schemas: &mut PartSchemas,
) -> Result<Vec<Part>, Error> {
    if let Some(key) = keys::taken_apart(&parts[0].schema(), shape) {
        let detail = format!(
            "stream {stream}: the merge key {key} holds objects or arrays, which normalizing \
             takes apart into columns or tables of their own: a key must be a value the stream \
             keeps whole"
        );
        return Err(Error::schema(detail)
            .with_code(codes::MERGE_KEY_NESTED)
            .with_stream(stream));
    }
    let failed =
        |error: ArrowError| normalize::identity::unread(stream, "normalizing a batch", &error);
    let batch = super::super::joined(parts).map_err(failed)?;
    // A merge stream's root row is identified by its key, so every row must hold one, and one
    // that equals itself.
    let mut columns = batch.columns().to_vec();
    for column in &shape.key {
        let Some(index) = batch.schema().index_of(column).ok() else {
            let detail = format!("stream {stream}: a batch has no key column {column}");
            return Err(Error::schema(detail)
                .with_code(codes::MERGE_KEY_MISSING)
                .with_stream(stream));
        };
        columns[index] = key_values(stream, column, batch.column(index), &|_| true)?;
    }
    let batch = with_columns(&batch, columns).map_err(failed)?;
    normalize::normalize(&batch, shape, schemas).map_err(failed)
}

/// Bytes: what splitting `piece`, slices of a unit, into `parts` holds beyond the unit until the
/// last of their pieces is written: the allocations of the parts' batches and lineage that the
/// piece's batches do not keep alive, each counted once, the arrays of `piece`'s batches, of the
/// parts' and of their lineage, a place in a list for each data buffer of the parts' views
/// beyond the first of each view array: the split joins the piece's lists, and a part's arrays
/// hold the first; and for a merge stream, the bit for each root row its parts share once its
/// table has found which rows its merge keeps.
pub(in crate::partition::write) fn made(piece: &[RecordBatch], parts: &[Part]) -> u64 {
    // A split reads only its piece, so what its parts share with the unit is the piece's.
    let (unit, mut made): (Allocations, _) = (piece.iter().collect(), Allocations::default());
    let sliced = piece
        .iter()
        .map(|batch| arrays(batch.schema_ref().fields()));
    let mut held = sliced.fold(0, u64::saturating_add);
    for part in parts {
        part_growth(part, &unit, &mut made);
        let lineage = part.lineage.arrays().len() + usize::from(part.lineage.keys.is_some());
        held = held
            .saturating_add(arrays(part.batch.schema_ref().fields()))
            .saturating_add(array_bytes(count(lineage)))
            .saturating_add(listed_bytes(ViewBuffers::of(&part.batch).beyond_first));
    }
    if let Some(keys) = parts.first().and_then(|root| root.lineage.keys.as_ref()) {
        held = held
            .saturating_add(array_bytes(1))
            .saturating_add(bitmap_bytes(keys.len()));
    }
    made.bytes().saturating_add(held)
}

/// Bytes: a buffer of a bit for each of `rows` rows, as Arrow allocates it, in 64-byte blocks.
fn bitmap_bytes(rows: usize) -> u64 {
    count(rows.div_ceil(8).next_multiple_of(64))
}

/// The bytes of the allocations `part`'s batch and lineage keep alive beyond those `unit` holds
/// that `made` did not hold yet, which then holds them too.
pub(in crate::partition::write) fn part_growth(
    part: &Part,
    unit: &Allocations,
    made: &mut Allocations,
) -> u64 {
    let lineage = &part.lineage;
    let mut arrays = lineage.arrays();
    arrays.extend(&lineage.keys);
    let batch = made.add_beyond(&part.batch, unit);
    arrays
        .into_iter()
        .map(|array| made.add_array_beyond(array.as_ref(), unit))
        .fold(batch, u64::saturating_add)
}

/// What a split's `parts` hold, their batches and lineage, kept until the last of their pieces
/// is written, as what the split made is charged until then.
pub(in crate::partition::write) fn kept<'a>(
    parts: impl IntoIterator<Item = &'a Part>,
) -> Vec<(RecordBatch, Lineage)> {
    parts
        .into_iter()
        .map(|part| (part.batch.clone(), part.lineage.clone()))
        .collect()
}
