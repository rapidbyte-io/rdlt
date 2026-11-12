//! What the first phase's reads of each stream ended on.

#[cfg(test)]
mod tests;

use std::ops::Range;

use crate::workload::End;

/// What a read that sent the rows at the offsets `sent` ended on, where the engine took its last
/// checkpoint before the row at `checkpointed`, if anywhere: a checkpoint after its last row, rows
/// after a checkpoint after one of its rows, or rows with no checkpoint after any, as a
/// checkpoint at its start comes after none of them; nothing where it sent no rows.
pub(crate) fn end(checkpointed: Option<u64>, sent: Range<usize>) -> Option<End> {
    let (start, last) = (sent.start as u64, sent.end as u64);
    match checkpointed {
        _ if sent.is_empty() => None,
        Some(at) if at == last => Some(End::Checkpoint),
        Some(at) if at > start => Some(End::Tail),
        _ => Some(End::Unmarked),
    }
}
