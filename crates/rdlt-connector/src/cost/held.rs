//! The allocations a batch keeps alive, each counted once however many arrays share it.

use std::collections::BTreeSet;

use arrow_array::{Array, RecordBatch};
use arrow_buffer::Buffer;
use arrow_data::ArrayData;

use super::widths::count;

/// A set of allocations and the bytes they take.
///
/// An allocation is told from another by where it starts, so a slice counts the whole buffer it
/// was cut from, and a dictionary or a body shared by many columns counts once.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Allocations {
    /// Where each allocation starts.
    seen: BTreeSet<usize>,
    bytes: u64,
}

impl Allocations {
    /// The allocations `batch` keeps alive.
    pub fn of(batch: &RecordBatch) -> Self {
        let mut allocations = Self::default();
        allocations.add(batch);
        allocations
    }

    /// The allocations `array` keeps alive.
    pub fn of_array(array: &dyn Array) -> Self {
        let mut allocations = Self::default();
        allocations.add_array(array);
        allocations
    }

    /// Bytes: every allocation in the set.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Adds the allocations `batch` keeps alive, and returns the bytes of those the set did not
    /// hold yet.
    pub fn add(&mut self, batch: &RecordBatch) -> u64 {
        let before = self.bytes;
        for column in batch.columns() {
            self.data(&column.to_data());
        }
        self.bytes - before
    }

    /// Adds the allocations `array` keeps alive, and returns the bytes of those the set did not
    /// hold yet.
    pub fn add_array(&mut self, array: &dyn Array) -> u64 {
        let before = self.bytes;
        self.data(&array.to_data());
        self.bytes - before
    }

    fn data(&mut self, data: &ArrayData) {
        // A stack of the nodes still to visit, so a deep type costs no call stack.
        let mut pending = vec![data];
        while let Some(data) = pending.pop() {
            for buffer in data.buffers() {
                self.buffer(buffer);
            }
            if let Some(nulls) = data.nulls() {
                self.buffer(nulls.buffer());
            }
            pending.extend(data.child_data());
        }
    }

    fn buffer(&mut self, buffer: &Buffer) {
        let bytes = count(buffer.capacity());
        if bytes > 0 && self.seen.insert(buffer.data_ptr().as_ptr() as usize) {
            self.bytes = self.bytes.saturating_add(bytes);
        }
    }
}
