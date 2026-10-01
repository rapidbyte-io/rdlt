//! One column of a batch as its rows are weighed: how to find what a row names, and the columns
//! nested in it.

use std::fmt;

use arrow_array::cast::AsArray as _;
use arrow_array::types::{
    ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, RunEndIndexType, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, OffsetSizeTrait};
use arrow_buffer::{ArrowNativeType as _, ScalarBuffer};
use arrow_schema::{DataType, UnionMode};

use super::Weight;

/// What a column's schema counts, nested columns included.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Counts {
    /// Field nodes.
    pub(super) nodes: usize,
    /// Buffers.
    pub(super) buffers: usize,
    /// Run-end columns, each with a place of its own for its last run.
    pub(super) runs: usize,
}

/// Finds what row `index` of a column names: the range of items of a list, the item of a union,
/// the run of a run-end column, the value of a dictionary key.
type Named<T> = Box<dyn Fn(usize) -> T + Send + Sync>;

/// A column as its rows are weighed.
pub(super) enum Column {
    /// Values of `bits` bits each, and a validity bit.
    Fixed { bits: u64 },
    /// Offsets of `bits` bits each into bytes.
    Bytes {
        bits: u64,
        range: Named<(usize, usize)>,
    },
    /// Views of bytes.
    Views { views: ScalarBuffer<u128> },
    /// Lists, maps and fixed-size lists: `bits` bits a row, and the items a row spans.
    List {
        bits: u64,
        range: Named<(usize, usize)>,
        item: Box<Column>,
    },
    /// List views: `bits` bits a row, and the items a row that is not null names.
    ListView {
        bits: u64,
        range: Named<(usize, usize)>,
        item: Box<Column>,
    },
    /// Structs and sparse unions: `bits` bits a row, and the same row of each child.
    Each { bits: u64, children: Vec<Column> },
    /// Dense unions: the child a row is of, and its item there.
    Dense {
        named: Named<(usize, usize)>,
        children: Vec<Column>,
    },
    /// Run-end columns: the run a row is in, its ends of `bits` bits each, and its values.
    Runs {
        bits: u64,
        run: Named<usize>,
        place: usize,
        values: Box<Column>,
    },
    /// Dictionary columns: keys of `bits` bits each, and the value a key that is not null names.
    Keyed {
        bits: u64,
        key: Named<Option<usize>>,
        values: Box<Column>,
    },
}

impl fmt::Debug for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Fixed { .. } => "fixed-width values",
            Self::Bytes { .. } => "bytes",
            Self::Views { .. } => "views",
            Self::List { .. } => "lists",
            Self::ListView { .. } => "list views",
            Self::Each { .. } => "a struct or sparse union",
            Self::Dense { .. } => "a dense union",
            Self::Runs { .. } => "runs",
            Self::Keyed { .. } => "dictionary keys",
        })
    }
}

/// Bits: a validity bit, which Arrow's writer sends for every value of a column that has one.
const VALID: u64 = 1;

impl Column {
    /// `array` as its rows are weighed, its nodes, buffers and run-end columns counted.
    pub(super) fn of(array: &dyn Array, counts: &mut Counts) -> Self {
        use DataType as T;
        counts.nodes += 1;
        counts.buffers += buffers(array.data_type());
        match array.data_type() {
            T::Utf8 => bytes(array.as_string::<i32>().offsets()),
            T::Binary => bytes(array.as_binary::<i32>().offsets()),
            T::LargeUtf8 => bytes(array.as_string::<i64>().offsets()),
            T::LargeBinary => bytes(array.as_binary::<i64>().offsets()),
            T::Utf8View => Self::Views {
                views: array.as_string_view().views().clone(),
            },
            T::BinaryView => Self::Views {
                views: array.as_binary_view().views().clone(),
            },
            T::List(_) => lists(array.as_list::<i32>(), counts),
            T::LargeList(_) => lists(array.as_list::<i64>(), counts),
            T::Map(..) => {
                let map = array.as_map();
                listed(map.offsets(), map.entries(), counts)
            }
            T::ListView(_) => list_views(array.as_list_view::<i32>(), counts),
            T::LargeListView(_) => list_views(array.as_list_view::<i64>(), counts),
            T::FixedSizeList(_, size) => {
                let size = usize::try_from(*size).unwrap_or(0);
                let range = move |row: usize| (row * size, row * size + size);
                let item = Self::of(array.as_fixed_size_list().values().as_ref(), counts);
                Self::List {
                    bits: VALID,
                    range: Box::new(range),
                    item: Box::new(item),
                }
            }
            T::Struct(_) => {
                let columns = array.as_struct().columns().iter();
                let children = columns
                    .map(|column| Self::of(column.as_ref(), counts))
                    .collect();
                Self::Each {
                    bits: VALID,
                    children,
                }
            }
            T::Union(fields, mode) => union(array, fields, *mode, counts),
            T::RunEndEncoded(ends, _) => match ends.data_type() {
                T::Int16 => runs(array.as_run::<Int16Type>(), counts),
                T::Int32 => runs(array.as_run::<Int32Type>(), counts),
                _ => runs(array.as_run::<Int64Type>(), counts),
            },
            T::Dictionary(keys, _) => match keys.as_ref() {
                T::Int8 => keyed::<Int8Type>(array, counts),
                T::Int16 => keyed::<Int16Type>(array, counts),
                T::Int32 => keyed::<Int32Type>(array, counts),
                T::Int64 => keyed::<Int64Type>(array, counts),
                T::UInt8 => keyed::<UInt8Type>(array, counts),
                T::UInt16 => keyed::<UInt16Type>(array, counts),
                T::UInt32 => keyed::<UInt32Type>(array, counts),
                _ => keyed::<UInt64Type>(array, counts),
            },
            leaf => fixed(leaf),
        }
    }

    /// Adds what item `index` of the column weighs to `weight`: to all of it where `framed`,
    /// else, for a value a dictionary key or a run already weighed names, to its expanded bits.
    pub(super) fn weigh(
        &self,
        index: usize,
        framed: bool,
        runs: &mut [Option<usize>],
        weight: &mut Weight,
    ) {
        match self {
            Self::Fixed { bits } => own(weight, framed, 1, *bits, *bits),
            Self::Bytes { bits, range } => {
                let (start, end) = range(index);
                let bits = bits + 8 * wide(end.saturating_sub(start));
                own(weight, framed, 1, bits, bits);
            }
            Self::Views { views } => view(views, index, framed, weight),
            Self::List { bits, range, item } | Self::ListView { bits, range, item } => {
                let (start, end) = range(index);
                let listed = matches!(self, Self::ListView { .. });
                let named = if listed {
                    wide(end.saturating_sub(start))
                } else {
                    0
                };
                own(weight, framed, 1 + named, *bits, *bits);
                for index in start..end {
                    item.weigh(index, framed, runs, weight);
                }
            }
            Self::Each { bits, children } => {
                own(weight, framed, 1, *bits, *bits);
                for child in children {
                    child.weigh(index, framed, runs, weight);
                }
            }
            Self::Dense { named, children } => {
                own(weight, framed, 1, 8 + 32, 8 + 32);
                let (child, index) = named(index);
                if let Some(child) = children.get(child) {
                    child.weigh(index, framed, runs, weight);
                }
            }
            Self::Runs {
                bits,
                run,
                place,
                values,
            } => {
                // A run's end and value are in the frame once, with the first row of the piece
                // in the run; every row takes the value once the runs are replaced by values.
                let run = run(index);
                let last = runs.get_mut(*place).filter(|_| framed);
                let begins = last.is_some_and(|last| last.replace(run) != Some(run));
                own(
                    weight,
                    framed,
                    1 + u64::from(begins),
                    if begins { *bits } else { 0 },
                    0,
                );
                values.weigh(run, begins, runs, weight);
            }
            Self::Keyed { bits, key, values } => {
                own(weight, framed, 1, *bits, 0);
                if let Some(key) = key(index) {
                    values.weigh(key, false, runs, weight);
                }
            }
        }
    }
}

/// Adds what view `index` of `views` weighs: itself, and the bytes it names beyond those it
/// holds.
fn view(views: &[u128], index: usize, framed: bool, weight: &mut Weight) {
    let length = views
        .get(index)
        .map_or(0, |view| *view & u128::from(u32::MAX));
    let named = u64::try_from(length).ok().filter(|length| *length > 12);
    let (named, bits) = (named.unwrap_or(0), 128 + VALID + 8 * named.unwrap_or(0));
    own(weight, framed, 1, bits, bits);
    if framed {
        weight.view_bytes = weight.view_bytes.saturating_add(named);
    }
}

/// Adds a column's own `values` and `bits` to `weight`'s frame where `framed`, and `expanded`
/// bits to what it takes expanded.
fn own(weight: &mut Weight, framed: bool, values: u64, bits: u64, expanded: u64) {
    if framed {
        weight.values = weight.values.saturating_add(values);
        weight.frame_bits = weight.frame_bits.saturating_add(bits);
    }
    weight.expanded_bits = weight.expanded_bits.saturating_add(expanded);
}

/// The items row `row` spans by `offsets`; none where the offsets do not reach.
fn span<O: OffsetSizeTrait>(offsets: &[O], row: usize) -> (usize, usize) {
    match (offsets.get(row), offsets.get(row + 1)) {
        (Some(start), Some(end)) => (start.as_usize(), end.as_usize()),
        _ => (0, 0),
    }
}

fn wide(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

/// A column of the fixed-width type `data_type`: nulls, booleans and values of whole bytes.
fn fixed(data_type: &DataType) -> Column {
    match data_type {
        DataType::Null => Column::Fixed { bits: 0 },
        DataType::Boolean => Column::Fixed { bits: 1 + VALID },
        fixed => Column::Fixed {
            bits: width(fixed) + VALID,
        },
    }
}

/// Bits in one value of the fixed-width type `data_type`.
fn width(data_type: &DataType) -> u64 {
    let bytes = match data_type {
        DataType::FixedSizeBinary(bytes) => usize::try_from(*bytes).unwrap_or(0),
        fixed => fixed.primitive_width().unwrap_or(0),
    };
    8 * wide(bytes)
}

/// How many buffers a column of `data_type` has of its own.
fn buffers(data_type: &DataType) -> usize {
    use DataType as T;
    match data_type {
        T::Null | T::RunEndEncoded(..) => 0,
        T::FixedSizeList(..) | T::Struct(_) | T::Union(_, UnionMode::Sparse) => 1,
        // Views have, once the column holds only what its rows name, one buffer of data.
        T::Utf8
        | T::Binary
        | T::LargeUtf8
        | T::LargeBinary
        | T::Utf8View
        | T::BinaryView
        | T::ListView(_)
        | T::LargeListView(_) => 3,
        _ => 2,
    }
}

fn bytes<O: OffsetSizeTrait>(offsets: &arrow_buffer::OffsetBuffer<O>) -> Column {
    let (offsets, bits) = (offsets.clone(), 8 * wide(size_of::<O>()) + VALID);
    let range = move |row: usize| span(&offsets, row);
    Column::Bytes {
        bits,
        range: Box::new(range),
    }
}

fn lists<O: OffsetSizeTrait>(
    lists: &arrow_array::GenericListArray<O>,
    counts: &mut Counts,
) -> Column {
    listed(lists.offsets(), lists.values().as_ref(), counts)
}

/// Lists of `items` by `offsets`.
fn listed<O: OffsetSizeTrait>(
    offsets: &arrow_buffer::OffsetBuffer<O>,
    items: &dyn Array,
    counts: &mut Counts,
) -> Column {
    let (offsets, bits) = (offsets.clone(), 8 * wide(size_of::<O>()) + VALID);
    let range = move |row: usize| span(&offsets, row);
    Column::List {
        bits,
        range: Box::new(range),
        item: Box::new(Column::of(items, counts)),
    }
}

fn list_views<O: OffsetSizeTrait>(
    lists: &arrow_array::GenericListViewArray<O>,
    counts: &mut Counts,
) -> Column {
    let (offsets, sizes, nulls) = (
        lists.offsets().clone(),
        lists.sizes().clone(),
        lists.nulls().cloned(),
    );
    // A null row names nothing, whatever its offset and size say.
    let range = move |row: usize| {
        let null = nulls.as_ref().is_some_and(|nulls| nulls.is_null(row));
        match (offsets.get(row), sizes.get(row)) {
            (Some(offset), Some(size)) if !null => (
                offset.as_usize(),
                offset.as_usize().saturating_add(size.as_usize()),
            ),
            _ => (0, 0),
        }
    };
    let item = Column::of(lists.values().as_ref(), counts);
    let bits = 16 * wide(size_of::<O>()) + VALID;
    Column::ListView {
        bits,
        range: Box::new(range),
        item: Box::new(item),
    }
}

fn union(
    array: &dyn Array,
    fields: &arrow_schema::UnionFields,
    mode: UnionMode,
    counts: &mut Counts,
) -> Column {
    let union = array.as_union();
    let children = fields
        .iter()
        .map(|(id, _)| Column::of(union.child(id).as_ref(), counts));
    let children: Vec<_> = children.collect();
    let Some(offsets) = union
        .offsets()
        .filter(|_| mode == UnionMode::Dense)
        .cloned()
    else {
        return Column::Each { bits: 8, children };
    };
    let ids: Vec<i8> = fields.iter().map(|(id, _)| id).collect();
    let of = union.type_ids().clone();
    let named = move |row: usize| {
        let child = ids
            .iter()
            .position(|id| *id == of[row])
            .unwrap_or(usize::MAX);
        (child, usize::try_from(offsets[row]).unwrap_or(usize::MAX))
    };
    Column::Dense {
        named: Box::new(named),
        children,
    }
}

fn runs<R: RunEndIndexType>(runs: &arrow_array::RunArray<R>, counts: &mut Counts) -> Column {
    // The run ends are a column of their own in a frame.
    counts.nodes += 1;
    counts.buffers += 2;
    let place = counts.runs;
    counts.runs += 1;
    let ends = runs.run_ends().clone();
    let run = move |row: usize| ends.get_physical_index(row);
    let values = Column::of(runs.values().as_ref(), counts);
    let bits = 8 * wide(size_of::<R::Native>()) + VALID;
    Column::Runs {
        bits,
        run: Box::new(run),
        place,
        values: Box::new(values),
    }
}

fn keyed<K: ArrowDictionaryKeyType>(array: &dyn Array, counts: &mut Counts) -> Column {
    let keyed = array.as_dictionary::<K>();
    let keys = keyed.keys().clone();
    let key = move |row: usize| keys.is_valid(row).then(|| keys.value(row).as_usize());
    // A dictionary's values are in a frame of their own: none of their nodes or buffers is in
    // the batch's.
    let values = Column::of(
        keyed.values().as_ref(),
        &mut Counts {
            runs: counts.runs,
            ..Counts::default()
        },
    );
    Column::Keyed {
        bits: 8 * wide(size_of::<K::Native>()) + VALID,
        key: Box::new(key),
        values: Box::new(values),
    }
}
