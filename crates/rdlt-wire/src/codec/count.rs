//! Counts what a batch's frame will hold from its columns, without encoding it: the values and
//! view bytes the receiver's walk counts in the frame Arrow's writer sends for a batch that
//! holds only what its rows name.

use arrow_array::cast::AsArray as _;
use arrow_array::{Array, RecordBatch, make_array};
use arrow_buffer::ArrowNativeType as _;
use arrow_schema::DataType;

/// What a batch's frame holds, as its receiver counts it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Counted {
    /// Every column's length, nested columns included, and every list view's size.
    pub(super) values: u64,
    /// The bytes views name in their data buffers, counted once a view.
    pub(super) view_bytes: u64,
}

/// Counts what the frame of `batch` holds.
pub(super) fn counted(batch: &RecordBatch) -> Counted {
    let mut counted = Counted::default();
    for column in batch.columns() {
        count(column.as_ref(), &mut counted);
    }
    counted
}

fn count(array: &dyn Array, counted: &mut Counted) {
    counted.values = counted.values.saturating_add(wide(array.len()));
    let named = |views: &[u128]| -> u64 {
        let lengths = views.iter().map(|view| *view & u128::from(u32::MAX));
        let lengths = lengths.map(|length| u64::try_from(length).unwrap_or(u64::MAX));
        lengths.filter(|length| *length > 12).sum()
    };
    match array.data_type() {
        DataType::Utf8View => counted.view_bytes += named(array.as_string_view().views()),
        DataType::BinaryView => counted.view_bytes += named(array.as_binary_view().views()),
        DataType::ListView(_) => {
            let sizes = array.as_list_view::<i32>().sizes().iter();
            counted.values += sizes.map(|size| wide(size.as_usize())).sum::<u64>();
        }
        DataType::LargeListView(_) => {
            let sizes = array.as_list_view::<i64>().sizes().iter();
            counted.values += sizes.map(|size| wide(size.as_usize())).sum::<u64>();
        }
        // A dictionary's values travel in a frame of their own, and nothing under a run-end
        // column of no values is read.
        DataType::Dictionary(..) => return,
        DataType::RunEndEncoded(..) if array.is_empty() => return,
        _ => {}
    }
    for child in array.to_data().child_data() {
        count(&make_array(child.clone()), counted);
    }
}

fn wide(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}
