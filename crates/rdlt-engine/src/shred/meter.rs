//! What a chunk's build may take, charged before its builders take it, and the columns its
//! records may hold, counted as they appear.

#[cfg(test)]
mod tests;

use std::cell::Cell;

use super::ShredError;

/// Bytes a speculative build presizes a column of text for each row it is sized for.
const TEXT_PER_ROW: usize = 8;

/// The bytes a chunk's builders may still take.
///
/// A build parsed speculatively, before the push's shape is known, may take what its push was
/// admitted for: once a builder would take more, the build stops and the chunk is observed
/// instead. A build against the push's shape was reserved for before it began, and presized from
/// what the chunk was observed to hold.
#[derive(Debug)]
pub(crate) struct Meter {
    room: Cell<u64>,
    allowance: u64,
    tripped: Cell<bool>,
    /// Bytes of text a column presizes for each row.
    text_per_row: usize,
}

/// A charge the meter has no room for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Over;

impl Meter {
    /// A speculative build's meter, with room for `allowance` bytes.
    pub(crate) fn new(allowance: u64) -> Self {
        Self {
            room: Cell::new(allowance),
            allowance,
            tripped: Cell::new(false),
            text_per_row: TEXT_PER_ROW,
        }
    }

    /// The meter of a build against the push's shape, which was reserved for before it began:
    /// text presizes nothing and grows as it is written.
    pub(crate) fn reserved() -> Self {
        Self {
            room: Cell::new(u64::MAX),
            allowance: u64::MAX,
            tripped: Cell::new(false),
            text_per_row: 0,
        }
    }

    /// Takes `bytes` of the room, or trips where there is not that much left.
    ///
    /// # Errors
    ///
    /// [`Over`] where the room left is less than `bytes`; the meter has tripped.
    pub(crate) fn charge(&self, bytes: u64) -> Result<(), Over> {
        let room = self.room.get();
        if bytes > room {
            self.tripped.set(true);
            return Err(Over);
        }
        self.room.set(room - bytes);
        Ok(())
    }

    /// Whether a charge found no room.
    pub(crate) fn tripped(&self) -> bool {
        self.tripped.get()
    }

    /// Bytes charged so far.
    pub(crate) fn spent(&self) -> u64 {
        self.allowance - self.room.get()
    }

    /// Bytes of text a column sized for `rows` rows presizes.
    pub(crate) fn text_bytes(&self, rows: usize) -> usize {
        rows.saturating_mul(self.text_per_row)
    }
}

/// The columns a chunk's records hold, counted as each first appears, at any depth, a list's
/// items being one: the limit a schema's columns meet holds for all of them together.
#[derive(Debug)]
pub(crate) struct Columns {
    count: Cell<u64>,
    limit: u64,
}

impl Columns {
    /// No columns yet, of at most `limit`.
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            count: Cell::new(0),
            limit,
        }
    }

    /// Counts a column that first appears.
    ///
    /// # Errors
    ///
    /// [`ShredError::TooManyColumns`] for a column past the limit.
    pub(crate) fn add(&self) -> Result<(), ShredError> {
        let count = self.count.get().saturating_add(1);
        if count > self.limit {
            return Err(ShredError::TooManyColumns(count, self.limit));
        }
        self.count.set(count);
        Ok(())
    }
}
