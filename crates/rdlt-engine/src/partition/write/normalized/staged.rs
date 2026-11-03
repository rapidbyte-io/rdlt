//! Which root rows of a merge stream's flush the flush stages: those no later row of the flush
//! shares a key with, found before any of its pieces is split.
//!
//! Every piece of a flush is written to one segment, which a commit publishes whole, and a later
//! row of a key has the greater sequence: a row a later one of its flush shares a key with is
//! never the row a commit publishes, nor are its children.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::BooleanBuffer;
use arrow_schema::ArrowError;
use rdlt_connector::StreamName;
use rdlt_connector::cost::array_bytes;
use rdlt_wire::limits::count;

use super::super::{Held, PartitionContext, PartitionJob};
use super::on_pool;
use crate::budget::Reservation;
use crate::error::Error;
use crate::normalize::Shape;
use crate::table::{comparing, key_values, last_across};

/// Which root rows of each piece of a flush the flush stages, and the charge of the bits no piece
/// has taken yet.
///
/// The bits are declared before their charge: dropped, they are freed before it goes.
pub(super) struct Staged {
    /// Each piece's bits, by its root rows' positions, until the piece takes them; none where each
    /// table of the flush keeps what its own merge keeps.
    pieces: Vec<Option<BooleanBuffer>>,
    held: Option<Reservation>,
}

impl Staged {
    /// The bits of the piece at `piece`, which it takes with their charge: its split charges them
    /// from then on.
    pub(super) fn take(&mut self, piece: usize) -> Option<BooleanBuffer> {
        let bits = self.pieces.get_mut(piece).and_then(Option::take)?;
        if let Some(held) = &mut self.held {
            held.shrink(held.bytes().saturating_sub(bits_bytes(bits.len())));
        }
        Some(bits)
    }

    /// Gives up the bits no piece has taken, and then their charge: those pieces each keep what
    /// their own merge keeps, which stages each key's last row of the flush as surely.
    pub(super) fn give_up(&mut self) {
        self.pieces.fill(None);
        self.held = None;
    }
}

/// A flush's pieces as finding their staged root rows reads them: the key columns of each of
/// their batches, in order, where every batch holds them, and each piece's root rows.
pub(super) struct Keys {
    columns: Option<Vec<Vec<ArrayRef>>>,
    rows: Vec<usize>,
}

impl Keys {
    /// The keys of `pieces`, of a stream that normalizes as `shape`: none where the stream does not
    /// merge, or a batch lacks a key column, which its split refuses.
    pub(super) fn of(pieces: &[Held], shape: &Shape) -> Self {
        let key = |batch: &RecordBatch| -> Option<Vec<ArrayRef>> {
            let columns = shape
                .key
                .iter()
                .map(|key| batch.column_by_name(key).cloned());
            columns.collect()
        };
        let mut batches = pieces.iter().flat_map(|piece| &piece.batches);
        let columns = if shape.key.is_empty() {
            None
        } else {
            batches.try_fold(Vec::new(), |mut columns, batch| {
                columns.push(key(batch)?);
                Some(columns)
            })
        };
        let rows = pieces
            .iter()
            .map(|piece| piece.batches.iter().map(RecordBatch::num_rows).sum())
            .collect();
        Self { columns, rows }
    }
}

/// Which root rows of a flush, whose pieces' keys are `keys`, of a stream that normalizes as
/// `shape`, the flush stages.
///
/// Where the stream merges and its schema policy drops no row, so that a later row of a key is
/// always staged, they are those no later row of the flush shares a key with: compared on the
/// compute pool, as the split's key columns hold them, where the budget has room at once for what
/// comparing holds. Each table keeps what its own merge keeps otherwise, as it does where a key
/// holds a null or a NaN, which the split refuses.
pub(super) async fn staged(
    job: &PartitionJob,
    context: &PartitionContext,
    shape: &Shape,
    keys: Keys,
) -> Result<Staged, Error> {
    let Keys { columns, rows } = keys;
    let pieces = rows.len();
    let none = || Staged {
        pieces: vec![None; pieces],
        held: None,
    };
    let merges = !shape.key.is_empty() && !context.tables.drops_rows(job.table);
    let Some(columns) = columns.filter(|_| merges) else {
        return Ok(none());
    };
    let Some(bytes) = reserving(&columns, &rows) else {
        return Ok(none());
    };
    let Some(mut held) = context.budget.try_acquire_working(bytes) else {
        return Ok(none());
    };
    let (stream, names) = (job.stream.clone(), shape.key.clone());
    let kept = rows_bits(&rows);
    let comparing = move || bits(&stream, &names, &columns, rows);
    let bits = on_pool(context, comparing).await.map_err(|error| {
        Error::internal(format!("stream {}: comparing merge keys", job.stream)).with_source(error)
    })?;
    let Some(bits) = bits else {
        return Ok(none());
    };
    held.shrink(kept);
    Ok(Staged {
        pieces: bits,
        held: Some(held),
    })
}

/// The bits of each piece's root rows, `rows` a piece, the key columns `names` of `stream`'s
/// batches being `columns`: set where no later row of any piece shares the row's key, as the
/// split holds it; none where a key holds a null or a NaN, which the split refuses.
fn bits(
    stream: &StreamName,
    names: &[Arc<str>],
    columns: &[Vec<ArrayRef>],
    rows: Vec<usize>,
) -> Result<Option<Vec<Option<BooleanBuffer>>>, ArrowError> {
    let mut zeroed = Vec::with_capacity(columns.len());
    for keys in columns {
        let columns = keys.iter().zip(names);
        let keys = columns.map(|(column, name)| key_values(stream, name, column, &|_| true));
        let Ok(keys) = keys.collect::<Result<Vec<_>, _>>() else {
            return Ok(None);
        };
        zeroed.push(keys);
    }
    let last = last_across(&zeroed)?;
    let mut first = 0;
    let mut bits = Vec::with_capacity(rows.len());
    for rows in rows {
        let piece = last.slice(first, rows);
        bits.push(Some(BooleanBuffer::new(piece.sliced(), 0, rows)));
        first += rows;
    }
    Ok(Some(bits))
}

/// Bytes: what finding the bits of pieces of `rows` root rows each, whose batches' key columns are
/// `columns`, holds at most, the bits included; none where comparing their keys is not bounded.
fn reserving(columns: &[Vec<ArrayRef>], rows: &[usize]) -> Option<u64> {
    let held = comparing(columns)?.saturating_add(zeroing(columns));
    Some(held.saturating_add(rows_bits(rows)))
}

/// Bytes: what the bits of pieces of `rows` root rows each hold.
fn rows_bits(rows: &[usize]) -> u64 {
    rows.iter().map(|rows| bits_bytes(*rows)).sum()
}

/// Bytes: the most holding `columns`, batches' key columns, as the split holds them takes beside
/// them: each column again, as an array of its own, where it holds a negative zero, and a byte a
/// row its NaNs are found in.
fn zeroing(columns: &[Vec<ArrayRef>]) -> u64 {
    let keys = columns.iter().flatten();
    let held = keys.map(|key| {
        let values = key.to_data().get_slice_memory_size().unwrap_or(usize::MAX);
        array_bytes(1)
            .saturating_add(count(values))
            .saturating_add(count(key.len()))
    });
    held.fold(0, u64::saturating_add)
}

/// Bytes: what the bits of `rows` root rows of a piece hold, an array's place and a buffer of a
/// bit a row in blocks of 64 bytes, as the piece's split charges them too.
pub(super) fn bits_bytes(rows: usize) -> u64 {
    array_bytes(1).saturating_add(count(rows.div_ceil(8).next_multiple_of(64)))
}

#[cfg(test)]
mod tests;
