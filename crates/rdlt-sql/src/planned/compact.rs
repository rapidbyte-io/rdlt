//! Text cut from a larger buffer, copied to hold its own rows alone: some kernels size what they
//! make by the whole buffer a slice was cut from, which the bound of a slice does not hold.

use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::ByteArrayType;
use arrow_array::{Array, ArrayRef, GenericByteArray, UInt32Array};
use arrow_buffer::{ArrowNativeType as _, Buffer};
use arrow_schema::{ArrowError, DataType};
use arrow_select::take::take;

/// Whether a column of `data_type` holds text or bytes of any length, which a slice may cut from
/// a larger buffer.
pub(super) fn variable(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
    )
}

/// `array` holding its own rows alone, where it holds bytes beyond them; none where it holds
/// none, or is no text or bytes.
pub(super) fn compacted(array: &ArrayRef) -> Result<Option<ArrayRef>, ArrowError> {
    match array.data_type() {
        DataType::Utf8 => offsets(array, array.as_string::<i32>()),
        DataType::LargeUtf8 => offsets(array, array.as_string::<i64>()),
        DataType::Binary => offsets(array, array.as_binary::<i32>()),
        DataType::LargeBinary => offsets(array, array.as_binary::<i64>()),
        DataType::Utf8View => {
            let views = array.as_string_view();
            let held: usize = views.data_buffers().iter().map(Buffer::len).sum();
            let compact = held > views.total_buffer_bytes_used();
            Ok(compact.then(|| Arc::new(views.gc()) as ArrayRef))
        }
        DataType::BinaryView => {
            let views = array.as_binary_view();
            let held: usize = views.data_buffers().iter().map(Buffer::len).sum();
            let compact = held > views.total_buffer_bytes_used();
            Ok(compact.then(|| Arc::new(views.gc()) as ArrayRef))
        }
        _ => Ok(None),
    }
}

/// `array`, of offsets into `bytes`' values, taken anew where its offsets name less than every
/// value byte it holds.
fn offsets<T: ByteArrayType>(
    array: &ArrayRef,
    bytes: &GenericByteArray<T>,
) -> Result<Option<ArrayRef>, ArrowError> {
    let offsets = bytes.value_offsets();
    let (Some(first), Some(last)) = (offsets.first(), offsets.last()) else {
        return Ok(None);
    };
    if first.as_usize() == 0 && last.as_usize() == bytes.value_data().len() {
        return Ok(None);
    }
    let rows = u32::try_from(array.len())
        .map_err(|_| ArrowError::ComputeError("a batch of more rows than u32 holds".into()))?;
    take(
        array.as_ref(),
        &UInt32Array::from_iter_values(0..rows),
        None,
    )
    .map(Some)
}
