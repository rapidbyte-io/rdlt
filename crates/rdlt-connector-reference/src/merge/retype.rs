//! A column as the wider type its table took since, exactly: every value as it is, or the
//! conversion refused.
//!
//! Arrow's own casts null a value the wider type cannot hold and multiply times of day without a
//! check. Here temporal values widen as the engine widens them, through
//! [`rdlt_connector::instants`], each the instant, time or duration it was, and every other
//! conversion is one that loses nothing or fails.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use super::refused::{TYPE_UNCONVERTIBLE, VALUE_UNHOLDABLE, caused, refused};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, ListArray, StructArray, new_null_array};
use arrow_cast::CastOptions;
use arrow_schema::{ArrowError, DataType, FieldRef, Fields};
use rdlt_connector::instants;

/// `array` as `to`: every value exactly as it is, or an error naming the conversion.
pub(super) fn retyped(array: &ArrayRef, to: &DataType) -> Result<ArrayRef, ArrowError> {
    let from = array.data_type();
    if from == to {
        return Ok(Arc::clone(array));
    }
    match (from, to) {
        (DataType::Null, _) => Ok(new_null_array(to, array.len())),
        // An encoded column converts as the values it encodes.
        (DataType::Dictionary(_, values), _) => retyped(&checked(array, values)?, to),
        (DataType::RunEndEncoded(_, values), _) => {
            retyped(&checked(array, values.data_type())?, to)
        }
        // Temporal values widen as the engine widens them: each the instant, time or duration
        // it was; no other conversion of one keeps every value.
        _ if instants::widens(from, to) => instants::widened(array, to).map_err(|error| {
            let message = format!("a value of {from} does not fit {to}");
            caused(VALUE_UNHOLDABLE, message, error)
        }),
        (DataType::Struct(_), DataType::Struct(fields)) => structs(array.as_struct(), fields),
        (DataType::List(_), DataType::List(item)) => lists(array.as_list::<i32>(), item),
        (
            DataType::LargeList(item)
            | DataType::FixedSizeList(item, _)
            | DataType::ListView(item)
            | DataType::LargeListView(item),
            DataType::List(_),
        ) => retyped(&checked(array, &DataType::List(Arc::clone(item)))?, to),
        _ if lossless(from, to) => checked(array, to),
        _ => Err(inexact(from, to)),
    }
}

/// Refuses `batches` unless every column of theirs that `schema` names converts to its type
/// there with every value kept: what a table holds must fit a type before a column takes it.
pub(crate) fn holds<'a>(
    batches: impl IntoIterator<Item = &'a arrow_array::RecordBatch>,
    schema: &arrow_schema::SchemaRef,
) -> Result<(), ArrowError> {
    for batch in batches {
        for field in schema.fields() {
            if let Some(column) = batch.column_by_name(field.name()) {
                retyped(column, field.data_type())?;
            }
        }
    }
    Ok(())
}

/// Whether Arrow's checked cast from `from` to `to` keeps every value it does not refuse: wider
/// or equal integers, integers a float holds exactly, wider floats and decimals, and text or
/// bytes in another encoding.
fn lossless(from: &DataType, to: &DataType) -> bool {
    use DataType as T;
    let small = matches!(
        from,
        T::Int8 | T::Int16 | T::Int32 | T::UInt8 | T::UInt16 | T::UInt32
    );
    let text = |kind: &T| matches!(kind, T::Utf8 | T::LargeUtf8 | T::Utf8View);
    let bytes = |kind: &T| matches!(kind, T::Binary | T::LargeBinary | T::BinaryView);
    match (from, to) {
        (from, to) if from.is_integer() && to.is_integer() => true,
        (_, T::Float64) if small || *from == T::Float32 => true,
        (from, T::Decimal128(..) | T::Decimal256(..)) if from.is_integer() => true,
        (
            T::Decimal128(_, from_scale) | T::Decimal256(_, from_scale),
            T::Decimal128(_, scale) | T::Decimal256(_, scale),
        ) => scale >= from_scale,
        (from, to) if text(from) && text(to) => true,
        (T::FixedSizeBinary(_), to) if bytes(to) => true,
        (from, to) => bytes(from) && bytes(to),
    }
}

/// Whether an id or a sequence of `kind` compares as a number: an integer, or a dictionary of
/// them.
pub(super) fn numbered(kind: &DataType) -> bool {
    match kind {
        DataType::Dictionary(_, values) => numbered(values),
        kind => kind.is_integer(),
    }
}

/// The values of `array`, an id or a sequence, as the bytes they compare by: bytes as they are,
/// text, which a destination without a type for bytes keeps them as, as the bytes it is, and
/// integers as sixteen bytes that order as the numbers do; a dictionary's as the values its
/// keys stand for.
pub(super) fn compared(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Dictionary(_, values) => compared(&checked(array, values)?),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            checked(array, &DataType::Binary)
        }
        kind if kind.is_integer() => {
            // Every integer fits a decimal of 38 digits; its sign bit flipped, the number's
            // bytes from the greatest order as the numbers do.
            let wide = checked(array, &DataType::Decimal128(38, 0))?;
            let wide = wide.as_primitive::<arrow_array::types::Decimal128Type>();
            let ordered = wide
                .iter()
                .map(|value| value.map(|value| (value ^ i128::MIN).to_be_bytes()));
            Ok(Arc::new(arrow_array::BinaryArray::from_iter(ordered)))
        }
        _ => retyped(array, &DataType::Binary),
    }
}

/// Arrow's cast of `array` to `to`, failing where a value does not fit instead of nulling it.
fn checked(array: &ArrayRef, to: &DataType) -> Result<ArrayRef, ArrowError> {
    let options = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    // Every pair cast here keeps the values it takes: what it refuses is a value that does
    // not fit.
    arrow_cast::cast_with_options(array, to, &options).map_err(|error| {
        let message = format!("a value of {} does not fit {to}", array.data_type());
        caused(VALUE_UNHOLDABLE, message, error)
    })
}

fn inexact(from: &DataType, to: &DataType) -> ArrowError {
    let message = format!("no conversion from {from} to {to} keeps every value");
    refused(TYPE_UNCONVERTIBLE, message)
}

/// `source` as a struct of `fields`: each field its column of that name, converted, or nulls;
/// a struct holding a field `fields` lacks is refused, since its values would go.
fn structs(source: &StructArray, fields: &Fields) -> Result<ArrayRef, ArrowError> {
    if let Some(extra) = source
        .fields()
        .iter()
        .find(|held| fields.find(held.name()).is_none())
    {
        let message = format!("field {} has no place in the wider struct", extra.name());
        return Err(refused(TYPE_UNCONVERTIBLE, message));
    }
    let columns = fields
        .iter()
        .map(|field| match source.column_by_name(field.name()) {
            Some(column) => retyped(column, field.data_type()),
            None => Ok(new_null_array(field.data_type(), source.len())),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let converted = StructArray::try_new(fields.clone(), columns, source.nulls().cloned())?;
    Ok(Arc::new(converted))
}

/// `source` as a list of `item`: its items converted, its lists as they are.
fn lists(source: &ListArray, item: &FieldRef) -> Result<ArrayRef, ArrowError> {
    let values = retyped(source.values(), item.data_type())?;
    let converted = ListArray::try_new(
        Arc::clone(item),
        source.offsets().clone(),
        values,
        source.nulls().cloned(),
    )?;
    Ok(Arc::new(converted))
}
