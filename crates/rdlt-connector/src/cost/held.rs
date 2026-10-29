//! The allocations a batch keeps alive, each counted once however many arrays share it, and the
//! data buffers its views hold.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::{Int16Type, Int32Type, Int64Type};
use arrow_array::{Array, RecordBatch};
use arrow_buffer::Buffer;
use arrow_schema::{DataType, Field, Schema};
use rdlt_wire::limits::count;

/// A set of allocations and the bytes they take.
///
/// An allocation is told from another by where it starts, so a slice counts the whole buffer it
/// was cut from, and a dictionary or a body shared by many columns counts once. A batch's schema
/// counts too, once for the batches that share it.
///
/// A set is kept only while what it names is alive: the allocator hands a freed address out
/// again, and an allocation made there would count as one the set holds.
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

    /// Bytes: every allocation in the set.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Adds the allocations `batch` keeps alive, and returns the bytes of those the set did not
    /// hold yet.
    pub fn add(&mut self, batch: &RecordBatch) -> u64 {
        self.batch(batch, None)
    }

    /// Adds the allocations `array` keeps alive, and returns the bytes of those the set did not
    /// hold yet.
    pub fn add_array(&mut self, array: &dyn Array) -> u64 {
        self.array(array, None)
    }

    /// Adds the allocations `batch` keeps alive beyond those `held` holds, and returns the bytes
    /// of those the set did not hold yet; `held` is left as it is.
    pub fn add_beyond(&mut self, batch: &RecordBatch, held: &Self) -> u64 {
        self.batch(batch, Some(held))
    }

    /// Adds the allocations `array` keeps alive beyond those `held` holds, and returns the bytes
    /// of those the set did not hold yet; `held` is left as it is.
    pub fn add_array_beyond(&mut self, array: &dyn Array, held: &Self) -> u64 {
        self.array(array, Some(held))
    }

    fn batch(&mut self, batch: &RecordBatch, beyond: Option<&Self>) -> u64 {
        let before = self.bytes;
        for column in batch.columns() {
            self.data(column.as_ref(), beyond);
        }
        // A batch keeps its schema alive too, which batches of one stream share.
        let schema = batch.schema_ref();
        let start = Arc::as_ptr(schema).addr();
        if !held_by(beyond, start) && self.seen.insert(start) {
            self.bytes = self.bytes.saturating_add(schema_bytes(schema));
        }
        self.bytes - before
    }

    fn array(&mut self, array: &dyn Array, beyond: Option<&Self>) -> u64 {
        let before = self.bytes;
        self.data(array, beyond);
        self.bytes - before
    }

    fn data(&mut self, array: &dyn Array, beyond: Option<&Self>) {
        let data = array.to_data();
        // A stack of the nodes still to visit, so a deep type costs no call stack.
        let mut pending = vec![&data];
        while let Some(data) = pending.pop() {
            for buffer in data.buffers() {
                self.buffer(buffer, beyond);
            }
            pending.extend(data.child_data());
        }
        // An array's data leaves out a validity holding no null, which the array keeps alive.
        each_array(array, |array| {
            if let Some(nulls) = array.nulls() {
                self.buffer(nulls.buffer(), beyond);
            }
        });
    }

    fn buffer(&mut self, buffer: &Buffer, beyond: Option<&Self>) {
        let bytes = count(buffer.capacity());
        let start = buffer.data_ptr().as_ptr().addr();
        if bytes > 0 && !held_by(beyond, start) && self.seen.insert(start) {
            self.bytes = self.bytes.saturating_add(bytes);
        }
    }
}

impl<'a> FromIterator<&'a RecordBatch> for Allocations {
    /// The allocations the batches keep alive together.
    fn from_iter<I: IntoIterator<Item = &'a RecordBatch>>(batches: I) -> Self {
        let mut allocations = Self::default();
        for batch in batches {
            allocations.add(batch);
        }
        allocations
    }
}

/// How many data buffers the view arrays of a batch hold, at any depth.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ViewBuffers {
    /// Every one.
    pub all: u64,
    /// Those beyond the first of each array.
    pub beyond_first: u64,
}

impl ViewBuffers {
    /// The data buffers the view arrays of `batch` hold.
    pub fn of(batch: &RecordBatch) -> Self {
        let mut counted = Self::default();
        for column in batch.columns() {
            each_array(column.as_ref(), |array| {
                let buffers = match array.data_type() {
                    DataType::Utf8View => array.as_string_view().data_buffers().len(),
                    DataType::BinaryView => array.as_binary_view().data_buffers().len(),
                    _ => 0,
                };
                let buffers = count(buffers);
                counted.all = counted.all.saturating_add(buffers);
                counted.beyond_first = counted
                    .beyond_first
                    .saturating_add(buffers.saturating_sub(1));
            });
        }
        counted
    }
}

/// Visits `array` and every array below it, at any depth.
fn each_array<'a>(array: &'a dyn Array, mut visit: impl FnMut(&'a dyn Array)) {
    // A stack of the arrays still to visit, which a flat array never fills.
    let (mut next, mut below) = (Some(array), Vec::new());
    while let Some(array) = next.take().or_else(|| below.pop()) {
        visit(array);
        children(array, &mut below);
    }
}

/// Pushes onto `below` the arrays one level below `array`: a struct's fields, a list's or a map's
/// items, a union's members, a dictionary's keys and values, and a run-end encoding's values.
fn children<'a>(array: &'a dyn Array, below: &mut Vec<&'a dyn Array>) {
    use DataType as T;
    match array.data_type() {
        T::Struct(_) => below.extend(array.as_struct().columns().iter().map(AsRef::as_ref)),
        T::List(_) => below.push(array.as_list::<i32>().values().as_ref()),
        T::LargeList(_) => below.push(array.as_list::<i64>().values().as_ref()),
        T::ListView(_) => below.push(array.as_list_view::<i32>().values().as_ref()),
        T::LargeListView(_) => below.push(array.as_list_view::<i64>().values().as_ref()),
        T::FixedSizeList(..) => below.push(array.as_fixed_size_list().values().as_ref()),
        T::Map(..) => below.push(array.as_map().entries()),
        T::Union(fields, _) => {
            let union = array.as_union();
            below.extend(fields.iter().map(|(id, _)| union.child(id).as_ref()));
        }
        T::Dictionary(..) => {
            let dictionary = array.as_any_dictionary();
            below.extend([dictionary.keys(), dictionary.values().as_ref()]);
        }
        T::RunEndEncoded(ends, _) => below.push(match ends.data_type() {
            T::Int16 => array.as_run::<Int16Type>().values().as_ref(),
            T::Int32 => array.as_run::<Int32Type>().values().as_ref(),
            _ => array.as_run::<Int64Type>().values().as_ref(),
        }),
        _ => {}
    }
}

/// Whether `held`, where there is one, holds the allocation that starts at `start`.
fn held_by(held: Option<&Allocations>, start: usize) -> bool {
    held.is_some_and(|held| held.seen.contains(&start))
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
