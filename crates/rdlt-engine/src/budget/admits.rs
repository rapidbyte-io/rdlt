//! The limits a budget admits within: what a connector told them never sends beyond what the
//! budget's shares hold.

#[cfg(test)]
mod tests;

use rdlt_wire::Limits;

use super::Shares;
use crate::cost::JSON_CHARGE;
use crate::limits::{COLUMN_RECORD, RECORDED, SCHEMA_KEPT};

/// The limits on what a read sends that a budget of `shares`, read by `readers` reads at once at
/// most, admits: a connector that keeps to them is refused nothing for the budget's sake.
///
/// Each is what lets the worst case it admits, all at once, fit the share it draws on:
///
/// - What a read keeps is half its dictionaries and half its schema: the schema's message is a
///   fifth of that half, since the schema decoded from it takes up to four times the message.
/// - A frame's batch keeps alive its own bytes, the dictionaries its keys name and its schema,
///   so a frame is at most what pushes may take less what a read may keep. And one row of a
///   frame, lowered as it arrives, with a null in each column of the widest table and the
///   metadata lowering adds, fits what one request for lowering a row may take where the load
///   keeps a log, half a request.
/// - What a commit records of a table, its schema and names twice over, fits the tables' share
///   where its columns, nested fields too, are no more than the share over what a column's
///   record takes with names of up to a hundred and fifty bytes; and no more than a schema's
///   message may carry of columns with short names.
/// - JSON text is admitted for itself and for the batches it becomes.
/// - Every read may hold a cursor waiting for a commit and a barrier's answer beside it, and the
///   commit a barrier calls is due once waiting cursors take half their share: half the share,
///   one cursor more and every read's answer fit it when a cursor is the share over twice one
///   more than the reads. A seal's frame records two cursors, each twice over, within the log's
///   share.
///
/// Rows, values and nesting are not the budget's to bound: a batch is lowered a piece at a time,
/// whatever it holds. A table whose records take more than its share all the same, as one of
/// longer names does, is refused at its schema change, before any commit.
pub(crate) fn admitted(shares: Shares, readers: usize) -> Limits {
    let kept = kept(shares, readers);
    let readers = u64::try_from(readers).unwrap_or(u64::MAX).max(1);
    let defaults = Limits::default();
    let schema_bytes = kept / 2 / SCHEMA_KEPT;
    let columns = (shares.tables / RECORDED.saturating_mul(COLUMN_RECORD))
        .min(schema_bytes / FIELD_MESSAGE)
        .min(defaults.schema_columns);
    let row = (shares.request / LOGGED).saturating_sub(row_overhead(columns));
    Limits {
        schema_columns: columns,
        frame_bytes: shares.intake.saturating_sub(kept).min(row),
        json_push_bytes: shares.intake / JSON_CHARGE,
        cursor_bytes: (shares.cursors / readers.saturating_add(1).saturating_mul(2))
            .min(shares.log / (2 * RECORDED)),
        schema_bytes,
        dictionary_bytes: kept / 2,
        ..defaults
    }
    .lesser(&defaults)
}

/// Bytes: what a column of a short name takes in the schema message that carries it: a schema
/// may hold no more columns than its message may carry so.
const FIELD_MESSAGE: u64 = 56;

/// Times what lowering a piece takes is reserved for it where the load keeps a log: for the
/// piece, and for its frame there.
const LOGGED: u64 = 2;

/// Bytes: the most a row's null in a column of its table takes, a 256-bit decimal's, with its
/// validity.
const NULL_SLOT: u64 = 33;

/// Bytes: the most the metadata columns lowering adds take of a row.
const META_ROW: u64 = 1 << 10;

/// Bytes: what lowering a row takes beside its own values, in a table of `columns` columns at
/// most: a null in each, and the metadata.
pub(crate) fn row_overhead(columns: u64) -> u64 {
    columns.saturating_mul(NULL_SLOT).saturating_add(META_ROW)
}

/// Bytes: what one of `readers` reads may keep beside its events of a budget of `shares`.
pub(crate) fn kept(shares: Shares, readers: usize) -> u64 {
    shares.reads / u64::try_from(readers).unwrap_or(u64::MAX).max(1)
}

/// Bytes: the least budget whose shares admit what the protocol lets no peer go below, a frame
/// of [`rdlt_wire::limits::MIN_FRAME_BYTES`] among them, read by `readers` reads at once.
pub(crate) fn least(readers: usize) -> u64 {
    let admits = |capacity: u64| admitted(Shares::of(capacity), readers).admit_peer().is_ok();
    // What a budget admits grows with it, but for a byte of rounding: found by halving, then
    // walked down past any budget a byte smaller that admits as much.
    let (mut low, mut high) = (0_u64, 1_u64 << 40);
    while low < high {
        let middle = low + (high - low) / 2;
        if admits(middle) {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    while low > 0 && admits(low - 1) {
        low -= 1;
    }
    low
}
