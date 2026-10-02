//! The allocations a batch keeps alive, each counted once however many arrays share it.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, Schema};

use super::widths::count;

/// A set of allocations and the bytes they take.
///
/// An allocation is told from another by where it starts, so a slice counts the whole buffer it
/// was cut from, and a dictionary or a body shared by many columns counts once. A batch's schema
/// counts too, once for the batches that share it.
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
        // A batch keeps its schema alive too, which batches of one stream share.
        let schema = batch.schema_ref();
        if self.seen.insert(Arc::as_ptr(schema).addr()) {
            self.bytes = self.bytes.saturating_add(schema_bytes(schema));
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
        if bytes > 0 && self.seen.insert(buffer.data_ptr().as_ptr().addr()) {
            self.bytes = self.bytes.saturating_add(bytes);
        }
    }
}

/// Bytes: what one field of a schema takes beside its name and its metadata.
const FIELD: u64 = 128;

/// Bytes: what one entry of a schema's or a field's metadata takes beside its text.
const ENTRY: u64 = 48;

/// Bytes: about what `schema` takes: each of its fields, nested ones too, with its name, its
/// metadata and its time zone, and the schema's own metadata.
pub fn schema_bytes(schema: &Schema) -> u64 {
    let text = |metadata: &HashMap<String, String>| {
        let entries = metadata.iter();
        let bytes = entries.map(|(key, value)| count(key.len() + value.len()) + ENTRY);
        bytes.fold(0, u64::saturating_add)
    };
    let mut bytes = text(schema.metadata());
    // A stack of the fields still to count, so a deep type costs no call stack.
    let mut pending: Vec<&Field> = schema.fields().iter().map(AsRef::as_ref).collect();
    while let Some(field) = pending.pop() {
        let own = FIELD + count(field.name().len()) + text(field.metadata());
        bytes = bytes.saturating_add(own);
        let mut data_type = field.data_type();
        while let DataType::Dictionary(_, values) = data_type {
            data_type = values;
        }
        match data_type {
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::ListView(item)
            | DataType::LargeListView(item)
            | DataType::FixedSizeList(item, _)
            | DataType::Map(item, _) => pending.push(item),
            DataType::Struct(fields) => pending.extend(fields.iter().map(AsRef::as_ref)),
            DataType::Union(fields, _) => {
                pending.extend(fields.iter().map(|(_, field)| field.as_ref()));
            }
            DataType::RunEndEncoded(ends, values) => {
                pending.extend([ends.as_ref(), values.as_ref()]);
            }
            DataType::Timestamp(_, Some(zone)) => bytes = bytes.saturating_add(count(zone.len())),
            _ => {}
        }
    }
    bytes
}
