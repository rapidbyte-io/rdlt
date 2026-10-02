//! The limits a budget admits within: what a connector told them never sends beyond what the
//! budget's shares hold.

#[cfg(test)]
mod tests;

use rdlt_wire::Limits;

use super::Shares;
use crate::cost::JSON_CHARGE;
use crate::limits::{RECORDED, SCHEMA_KEPT};

/// The limits on what a read sends that a budget of `shares`, read by `readers` reads at once at
/// most, admits: a connector that keeps to them is refused nothing for the budget's sake.
///
/// - What a read keeps is half its dictionaries and half its schema: the schema's message is a
///   fifth of that half, since the schema decoded from it takes up to four times the message.
/// - A frame's batch keeps alive its own bytes, the dictionaries its keys name and its schema,
///   so a frame is what pushes may take less what a read may keep.
/// - JSON text is admitted for itself and for the batches it becomes.
/// - A cursor fits the cursors' share, and the seal frame recording two of them, each twice
///   over, the log's.
///
/// Rows and values in a batch are not the budget's to bound: a batch is lowered a piece at a
/// time, whatever it holds.
pub(crate) fn admitted(shares: Shares, readers: usize) -> Limits {
    let kept = kept(shares, readers);
    let defaults = Limits::default();
    Limits {
        frame_bytes: shares.intake.saturating_sub(kept),
        json_push_bytes: shares.intake / JSON_CHARGE,
        cursor_bytes: shares.cursors.min(shares.log / (2 * RECORDED)),
        schema_bytes: kept / 2 / SCHEMA_KEPT,
        dictionary_bytes: kept / 2,
        ..defaults
    }
    .lesser(&defaults)
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
