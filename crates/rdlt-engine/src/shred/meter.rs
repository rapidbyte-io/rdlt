//! What a chunk's build may take, charged before its builders take it, and the columns its
//! records may hold, counted as they appear.

#[cfg(test)]
mod tests;

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::ShredError;

/// Bytes a speculative build presizes a column of text for each row it is sized for.
const TEXT_PER_ROW: usize = 8;

/// Bytes a column's entry in a chunk's record or shape takes beside its name: its name's
/// allocation, its place in the record's or shape's vectors and its index's node, as they stand
/// just after the vectors doubled to hold it, the buffer they grew from not yet freed.
pub(crate) const KEY: u64 = 360;

/// Bytes a leaf's or a list's builder takes beside what grows with its rows: the builder and
/// its buffers' rounding to 64 bytes.
pub(crate) const BUILDER: u64 = 256;

/// Bytes a struct's builder, a record of its own, or its built array, takes beside its fields.
pub(crate) const RECORD: u64 = 1_536;

/// Bytes a leaf's or a list's built array takes beside what grows with its rows: the array, its
/// data and its buffers' rounding, and the builder it is made from while it is made.
pub(crate) const ARRAY: u64 = 640;

/// Bytes an observed object's own shape takes beside its fields.
pub(crate) const OBJECT_SHAPE: u64 = 384;

/// What observing a flush's chunks may hold beyond their chunks' allowances, shared by them: a
/// chunk smaller than its records' columns, the last of the flush, observes them beyond it.
#[derive(Debug)]
pub(crate) struct Beyond {
    room: AtomicU64,
    limit: u64,
}

impl Beyond {
    /// Room for `limit` bytes beyond the chunks' allowances.
    pub(crate) fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            room: AtomicU64::new(limit),
            limit,
        })
    }

    /// Takes `bytes` of the room, where there is that much.
    fn take(&self, bytes: u64) -> bool {
        self.room
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |room| {
                room.checked_sub(bytes)
            })
            .is_ok()
    }

    /// The bytes the room had.
    pub(crate) fn limit(&self) -> u64 {
        self.limit
    }
}

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
    /// Where a charge past the room is taken from instead of tripping, for an observation.
    beyond: Option<Arc<Beyond>>,
    /// Bytes taken from beyond the room.
    drawn: Cell<u64>,
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
            beyond: None,
            drawn: Cell::new(0),
        }
    }

    /// An observation's meter, with room for `allowance` bytes and, past it, what `beyond`
    /// has room for.
    pub(crate) fn observing(allowance: u64, beyond: &Arc<Beyond>) -> Self {
        Self {
            beyond: Some(Arc::clone(beyond)),
            ..Self::new(allowance)
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
            beyond: None,
            drawn: Cell::new(0),
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
            let short = bytes - room;
            if self
                .beyond
                .as_ref()
                .is_some_and(|beyond| beyond.take(short))
            {
                self.room.set(0);
                self.drawn.set(self.drawn.get().saturating_add(short));
                return Ok(());
            }
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

    /// Bytes charged so far, within the room and beyond it.
    pub(crate) fn spent(&self) -> u64 {
        (self.allowance - self.room.get()).saturating_add(self.drawn.get())
    }

    /// The bytes an observation may hold beyond its chunk's allowance, for all the flush's chunks.
    pub(crate) fn beyond_limit(&self) -> u64 {
        self.beyond.as_ref().map_or(0, |beyond| beyond.limit())
    }

    /// Bytes `name`'s entry as a column of a record or shape takes.
    pub(crate) fn key(name: &str) -> u64 {
        KEY.saturating_add(name.len() as u64)
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
