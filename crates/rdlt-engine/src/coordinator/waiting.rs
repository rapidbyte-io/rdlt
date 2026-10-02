//! The seals waiting for the next commit, and the cursor bytes they hold.

use std::collections::BTreeMap;

use crate::partition::Seal;

/// Seals in the order they arrived, until a commit takes them.
///
/// A seal of no rows only moves its partition's position, so the next seal of no rows at a
/// cursor of the same partition replaces it where no other seal came between: however many
/// checkpoints a source sends without rows, one cursor a partition waits. A seal that ends a
/// partition done replaces nothing, so its source is still told the cursor before it.
#[derive(Debug, Default)]
pub(super) struct WaitingSeals {
    seals: Vec<Seal>,
    /// Where each partition's last seal lies, where that seal has no rows.
    moves: BTreeMap<usize, usize>,
    /// Bytes: the cursors of the seals.
    cursor_bytes: u64,
}

impl WaitingSeals {
    /// Adds `seal`: in place of its partition's last where both seal no rows and `seal` is at a
    /// cursor, keeping the barrier either answers.
    pub(super) fn push(&mut self, mut seal: Seal) {
        let moves_only =
            seal.moves_only() && matches!(seal.state, rdlt_connector::PartitionState::Cursor(_));
        self.cursor_bytes = self.cursor_bytes.saturating_add(seal.cursor_bytes());
        match self.moves.get(&seal.partition) {
            Some(&at) if moves_only => {
                let replaced = &mut self.seals[at];
                self.cursor_bytes = self.cursor_bytes.saturating_sub(replaced.cursor_bytes());
                seal.answers = seal.answers.max(replaced.answers);
                *replaced = seal;
            }
            _ => {
                if moves_only {
                    self.moves.insert(seal.partition, self.seals.len());
                } else {
                    self.moves.remove(&seal.partition);
                }
                self.seals.push(seal);
            }
        }
    }

    /// Bytes: the cursors of the seals waiting.
    pub(super) fn cursor_bytes(&self) -> u64 {
        self.cursor_bytes
    }

    /// The seals waiting, in order.
    #[cfg(test)]
    pub(super) fn iter(&self) -> impl Iterator<Item = &Seal> {
        self.seals.iter()
    }

    /// Takes every seal waiting, in order.
    pub(super) fn take(&mut self) -> Vec<Seal> {
        self.moves.clear();
        self.cursor_bytes = 0;
        std::mem::take(&mut self.seals)
    }
}
