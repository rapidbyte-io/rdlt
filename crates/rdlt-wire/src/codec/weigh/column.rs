//! One column of a batch as its rows are weighed: how to find what a row names, and the columns
//! nested in it.

use std::fmt;

use arrow_array::Array;
use arrow_array::cast::AsArray as _;
use arrow_array::types::{
    Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, UnionMode};

use super::build;

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

/// Finds what item `index` of a column names: where its bytes or items begin, the items of a
/// list view, the item of a union, the run of a run-end column.
pub(super) type Named<T> = Box<dyn Fn(usize) -> T + Send + Sync>;

/// A column as its rows are weighed.
pub(super) enum Column {
    /// Values of `bits` bits each, and a validity bit.
    Fixed { bits: u64 },
    /// Offsets of `bits` bits each into bytes; `offset` is where an item's bytes begin.
    Bytes { bits: u64, offset: Named<usize> },
    /// Views of bytes.
    Views { views: ScalarBuffer<u128> },
    /// Lists and maps: `bits` bits a row, and the items a row spans from `offset`; a row null
    /// by `nulls` spans none.
    List {
        bits: u64,
        offset: Named<usize>,
        nulls: Option<NullBuffer>,
        item: Box<Column>,
    },
    /// Fixed-size lists of `size` items a row.
    Sized { size: usize, item: Box<Column> },
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
    /// Run-end columns: the run a row is in and the row that run ends before, its ends of
    /// `bits` bits each, and its values.
    Runs {
        bits: u64,
        reach: Named<(usize, usize)>,
        place: usize,
        values: Box<Column>,
    },
    /// Dictionary columns: keys of `bits` bits each, and their `length` values, which are in a
    /// frame of their own.
    Keyed {
        bits: u64,
        length: usize,
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
            Self::Sized { .. } => "fixed-size lists",
            Self::ListView { .. } => "list views",
            Self::Each { .. } => "a struct or sparse union",
            Self::Dense { .. } => "a dense union",
            Self::Runs { .. } => "runs",
            Self::Keyed { .. } => "dictionary keys",
        })
    }
}

impl Column {
    /// `array` as its rows are weighed, its nodes, buffers and run-end columns counted;
    /// `rebuilt` where its piece is rebuilt from what its rows name, which drops what a null
    /// list spans.
    pub(super) fn of(array: &dyn Array, rebuilt: bool, counts: &mut Counts) -> Self {
        use DataType as T;
        counts.nodes += 1;
        counts.buffers += buffers(array.data_type());
        match array.data_type() {
            T::Utf8 => build::bytes(array.as_string::<i32>().offsets()),
            T::Binary => build::bytes(array.as_binary::<i32>().offsets()),
            T::LargeUtf8 => build::bytes(array.as_string::<i64>().offsets()),
            T::LargeBinary => build::bytes(array.as_binary::<i64>().offsets()),
            T::Utf8View => Self::Views {
                views: array.as_string_view().views().clone(),
            },
            T::BinaryView => Self::Views {
                views: array.as_binary_view().views().clone(),
            },
            T::List(_) => build::lists(array.as_list::<i32>(), rebuilt, counts),
            T::LargeList(_) => build::lists(array.as_list::<i64>(), rebuilt, counts),
            T::Map(..) => {
                let map = array.as_map();
                build::listed(map.offsets(), map.nulls(), map.entries(), rebuilt, counts)
            }
            T::ListView(_) => build::list_views(array.as_list_view::<i32>(), rebuilt, counts),
            T::LargeListView(_) => build::list_views(array.as_list_view::<i64>(), rebuilt, counts),
            T::FixedSizeList(_, size) => build::fixed_lists(array, *size, rebuilt, counts),
            T::Struct(_) => {
                let columns = array.as_struct().columns().iter();
                let children = columns.map(|column| Self::of(column.as_ref(), rebuilt, counts));
                Self::Each {
                    bits: build::VALID,
                    children: children.collect(),
                }
            }
            T::Union(fields, mode) => build::union(array, fields, *mode, rebuilt, counts),
            T::RunEndEncoded(ends, _) => match ends.data_type() {
                T::Int16 => build::runs(array.as_run::<Int16Type>(), rebuilt, counts),
                T::Int32 => build::runs(array.as_run::<Int32Type>(), rebuilt, counts),
                _ => build::runs(array.as_run::<Int64Type>(), rebuilt, counts),
            },
            T::Dictionary(keys, _) => match keys.as_ref() {
                T::Int8 => build::keyed::<Int8Type>(array, counts),
                T::Int16 => build::keyed::<Int16Type>(array, counts),
                T::Int32 => build::keyed::<Int32Type>(array, counts),
                T::Int64 => build::keyed::<Int64Type>(array, counts),
                T::UInt8 => build::keyed::<UInt8Type>(array, counts),
                T::UInt16 => build::keyed::<UInt16Type>(array, counts),
                T::UInt32 => build::keyed::<UInt32Type>(array, counts),
                _ => build::keyed::<UInt64Type>(array, counts),
            },
            leaf => build::fixed(leaf),
        }
    }
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
