//! The limits a budget admits within: what a connector told them never sends beyond what the
//! budget's shares hold.

#[cfg(kani)]
mod proofs;
#[cfg(test)]
mod tests;

use rdlt_connector::cost::JSON_CHARGE;
use rdlt_wire::Limits;
use rdlt_wire::limits::{Class, MESSAGE_ROOM, MIN_DICTIONARY_BYTES};

use super::Shares;
use crate::limits::{COLUMN_RECORD, READ_SHARE, RECORDED, SCHEMA_KEPT};

/// The limits on what a read sends that a budget of `shares`, read by `readers` reads at once at
/// most, admits: a connector that keeps to them is refused nothing for the budget's sake but
/// what its pushes become beyond one request.
///
/// Each is what lets the worst case it admits, all at once, fit the share it draws on:
///
/// - What a read keeps is half its dictionaries and half its schema: the schema's message is a
///   fifth of that half, since the schema decoded from it takes up to four times the message.
/// - A frame's batch keeps alive the message it arrived in, its own bytes and at most
///   `MESSAGE_ROOM` beside them, the dictionaries its keys name and its schema, so a frame is at
///   most what pushes may take less what a read may keep and that room. And one row of a
///   frame, lowered as it arrives, with a null in each column of the widest table and the
///   metadata lowering adds, fits what one request for lowering a row may take where the load
///   keeps a log, half a request.
/// - What a commit records of a table, its schema and names at most twice over, fits the
///   tables' share where its columns, nested fields too, are no more than the share over what a
///   column's record takes with names of up to a hundred and fifty bytes; and no more than a
///   schema's message may carry of columns with short names.
/// - A frame is charged, before it is decoded, what its scan counts decoding it holds, at most
///   twice its bytes and the fields around them, to what pushes may take: a frame within half a
///   request beside a row's nulls is within half of that too.
/// - Every other answer is charged what decoding it holds to the share of answers being
///   decoded: a catalog, state and any other control message are at most what the share holds
///   of one decoded, as [`Class::decoded_per_byte`] says for each. State is bounded on what it
///   holds decoded, so it may take the whole share, and the engine commits no more of it.
/// - JSON text is admitted for itself, for the room of the message it arrives in and for twice
///   itself of the batches it becomes; what they take beyond that is reserved from a request,
///   and refused beyond one.
/// - Every read may hold a cursor waiting for a commit and a barrier's answer beside it, and the
///   commit a barrier calls is due once waiting cursors take half their share: half the share,
///   one cursor more and every read's answer fit it when a cursor is the share over twice one
///   more than the reads. A seal's frame records two cursors, each at most twice over, within the
///   log's share.
///
/// Rows, values and nesting are not the budget's to bound: a batch is lowered a piece at a time,
/// whatever it holds. A table whose records take more than its share all the same, as one of
/// longer names does, is refused at its schema change, before any commit; a row its table
/// widens, or JSON whose batches take more than a request beyond twice its text, when it is
/// lowered or built.
pub(crate) fn admitted(shares: Shares, readers: usize) -> Limits {
    let kept = kept(shares, readers);
    let readers = u64::try_from(readers).unwrap_or(u64::MAX).max(1);
    let defaults = Limits::default();
    let schema_bytes = kept / 2 / SCHEMA_KEPT;
    let columns = (shares.tables / RECORDED.saturating_mul(COLUMN_RECORD))
        .min(schema_bytes / FIELD_MESSAGE)
        .min(defaults.schema_columns);
    let row = (shares.request / LOGGED).saturating_sub(row_overhead(columns));
    let decoded = |class: Class| shares.control / per_byte(class);
    Limits {
        schema_columns: columns,
        frame_bytes: shares
            .intake
            .saturating_sub(kept)
            .saturating_sub(MESSAGE_ROOM)
            .min(row),
        catalog_bytes: decoded(Class::Catalog),
        state_bytes: decoded(Class::State),
        control_message_bytes: decoded(Class::Control),
        json_push_bytes: shares.intake.saturating_sub(MESSAGE_ROOM) / JSON_CHARGE,
        // A quarter of the log's share, which a commit taking at most twice each cursor holds,
        // is the cursors' share itself: the partitions' bound is the lesser.
        cursor_bytes: shares.cursors / readers.saturating_add(1).saturating_mul(2),
        schema_bytes,
        dictionary_bytes: kept / 2,
        ..defaults
    }
    .lesser(&defaults)
}

/// Bytes a message of `class` holds decoded for each byte it takes on the wire.
fn per_byte(class: Class) -> u64 {
    u64::try_from(class.decoded_per_byte()).unwrap_or(u64::MAX)
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

/// Reads at once up to which the least budget is found by halving: a frame sets it for up to 16
/// reads, and the dictionaries do from 17.
pub(crate) const HALVED: usize = 24;

/// Bytes: the least budget from which every budget upward admits what the protocol lets no peer
/// go below, a frame of [`rdlt_wire::limits::MIN_FRAME_BYTES`] among them, read by `readers`
/// reads at once, while the budget a byte below does not; none where no budget does: from 2^43
/// reads, the reads' quarter of the largest budget holds less than twice the least dictionary for
/// each.
///
/// It is the larger of two. A read's dictionaries may take half what it keeps, which grows with
/// the budget, so the least budget whose reads' share holds twice the least dictionary for each
/// read is exact.
/// What a frame may take does not grow with the budget byte by byte: it is what pushes may take
/// less what a read keeps, or half a request less a row's nulls, and each falls by a few bytes
/// where the share it subtracts, or the columns a schema may have, grows by its rounding. So a
/// budget may admit a peer where one a few bytes larger does not, and halving ends beside
/// whichever of those changes its steps meet: up to [`HALVED`] reads at once it meets the last.
/// More reads each keep less, which leaves a frame no less, and their dictionaries alone need
/// more.
pub(crate) fn least(readers: usize) -> Option<u64> {
    let readers = readers.max(1);
    let halved = readers.min(HALVED);
    let admits = |capacity: u64| admitted(Shares::of(capacity), halved).admit_peer().is_ok();
    let (mut low, mut high) = (0_u64, 1_u64 << 40);
    while low < high {
        let middle = low + (high - low) / 2;
        if admits(middle) {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    let dictionaries = u64::try_from(readers)
        .ok()?
        .checked_mul(READ_SHARE * 2 * MIN_DICTIONARY_BYTES)?;
    Some(low.max(dictionaries))
}
