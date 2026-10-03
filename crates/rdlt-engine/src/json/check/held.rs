//! What checking a batch's columns of JSON holds beside the batch, charged before the check runs.
//!
//! Only two things are held: the values of a dictionary its keys name, and the items of a list
//! view whose rows name them out of order. Nothing else is held a row.

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field};

use super::field_holds_json;
use super::rows::Views;

/// Bytes: what a dictionary's named values take a key, where a bitmap of its values would take
/// more: a key's position, sorted.
const LISTED_KEY: u64 = 8;

/// Bytes: what a list view's span takes, where its rows name its items out of order.
const GATHERED_SPAN: u64 = 16;

/// Whether a dictionary of `values` values and `keys` keys has its named values held as a bitmap
/// of its values: where that takes no more than its keys, listed, would.
pub(super) fn bitmapped(values: usize, keys: usize) -> bool {
    values.div_ceil(8) as u64 <= LISTED_KEY.saturating_mul(keys as u64)
}

/// Bytes: the most checking `batch`'s columns of JSON holds beside it at once.
pub(crate) fn held(batch: &RecordBatch) -> u64 {
    let fields = batch.schema();
    let columns = fields.fields().iter().zip(batch.columns());
    columns
        .filter(|(field, _)| field_holds_json(field))
        .map(|(_, column)| array(column.as_ref()))
        .max()
        .unwrap_or(0)
}

/// Bytes: what checking `array`, a column of JSON or one holding them, holds at once, its
/// values' included.
fn array(array: &dyn Array) -> u64 {
    let below = |field: &Field, values: &dyn Array| {
        if field_holds_json(field) {
            self::array(values)
        } else {
            0
        }
    };
    match array.data_type() {
        DataType::Dictionary(_, _) => {
            let values = dictionary_values(array);
            let keys = array.len();
            let named = if bitmapped(values.len(), keys) {
                values.len().div_ceil(8) as u64
            } else {
                LISTED_KEY.saturating_mul(keys as u64)
            };
            named.saturating_add(self::array(values))
        }
        DataType::RunEndEncoded(_, _) => self::array(run_values(array)),
        DataType::Struct(fields) => fields
            .iter()
            .zip(array.as_struct().columns())
            .map(|(field, column)| below(field, column.as_ref()))
            .fold(0, u64::saturating_add),
        DataType::List(item) => below(item, array.as_list::<i32>().values().as_ref()),
        DataType::LargeList(item) => below(item, array.as_list::<i64>().values().as_ref()),
        DataType::Map(item, _) => below(item, array.as_map().entries()),
        DataType::FixedSizeList(item, _) => below(item, array.as_fixed_size_list().values()),
        DataType::ListView(item) => {
            let views = array.as_list_view::<i32>();
            let spans = Views::Small(views.offsets(), views.sizes());
            gathered(spans, array.len()).saturating_add(below(item, views.values().as_ref()))
        }
        DataType::LargeListView(item) => {
            let views = array.as_list_view::<i64>();
            let spans = Views::Large(views.offsets(), views.sizes());
            gathered(spans, array.len()).saturating_add(below(item, views.values().as_ref()))
        }
        _ => 0,
    }
}

/// Bytes: what a list view of `len` rows holds to name its items, where they are out of order.
fn gathered(views: Views<'_>, len: usize) -> u64 {
    if views.ordered(len) {
        0
    } else {
        GATHERED_SPAN.saturating_mul(len as u64)
    }
}

fn dictionary_values(array: &dyn Array) -> &dyn Array {
    use arrow_array::types::{
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    };
    match array.data_type() {
        DataType::Dictionary(key, _) => match key.as_ref() {
            DataType::Int8 => array.as_dictionary::<Int8Type>().values().as_ref(),
            DataType::Int16 => array.as_dictionary::<Int16Type>().values().as_ref(),
            DataType::Int32 => array.as_dictionary::<Int32Type>().values().as_ref(),
            DataType::Int64 => array.as_dictionary::<Int64Type>().values().as_ref(),
            DataType::UInt8 => array.as_dictionary::<UInt8Type>().values().as_ref(),
            DataType::UInt16 => array.as_dictionary::<UInt16Type>().values().as_ref(),
            DataType::UInt32 => array.as_dictionary::<UInt32Type>().values().as_ref(),
            _ => array.as_dictionary::<UInt64Type>().values().as_ref(),
        },
        _ => array,
    }
}

fn run_values(array: &dyn Array) -> &dyn Array {
    use arrow_array::types::{Int16Type, Int32Type, Int64Type};
    match array.data_type() {
        DataType::RunEndEncoded(ends, _) => match ends.data_type() {
            DataType::Int16 => array.as_run::<Int16Type>().values().as_ref(),
            DataType::Int32 => array.as_run::<Int32Type>().values().as_ref(),
            _ => array.as_run::<Int64Type>().values().as_ref(),
        },
        _ => array,
    }
}
