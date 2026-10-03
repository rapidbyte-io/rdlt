//! The days of dates and the instants of dates and timestamps: a `Date64` holding part of a day
//! is the day it is within, never the day its milliseconds round toward zero to.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, GenericListArray, OffsetSizeTrait, StructArray,
};
use arrow_schema::{ArrowError, DataType, FieldRef};
use rdlt_connector::instants;

use super::super::convert::retyped;

/// `array` with every `Date64` in it, at any depth, a `Date32` of the day it is within.
///
/// # Errors
///
/// A day a `Date32` cannot hold.
pub(crate) fn dated(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    Ok(match array.data_type() {
        DataType::Date64 => days(array)?,
        DataType::Struct(fields) => {
            let held = array.as_struct();
            let columns = held
                .columns()
                .iter()
                .map(dated)
                .collect::<Result<Vec<_>, _>>()?;
            if columns
                .iter()
                .zip(held.columns())
                .all(|(new, old)| Arc::ptr_eq(new, old))
            {
                return Ok(Arc::clone(array));
            }
            let fields = fields
                .iter()
                .zip(&columns)
                .map(|(field, column)| retyped(field, column))
                .collect();
            Arc::new(StructArray::try_new(
                fields,
                columns,
                held.nulls().cloned(),
            )?)
        }
        DataType::List(field) => list(array, array.as_list::<i32>(), field)?,
        DataType::LargeList(field) => list(array, array.as_list::<i64>(), field)?,
        DataType::FixedSizeList(field, size) => {
            let list = array.as_fixed_size_list();
            let values = dated(list.values())?;
            if Arc::ptr_eq(&values, list.values()) {
                return Ok(Arc::clone(array));
            }
            let field = retyped(field, &values);
            let nulls = list.nulls().cloned();
            Arc::new(FixedSizeListArray::try_new(field, *size, values, nulls)?)
        }
        _ => Arc::clone(array),
    })
}

/// `array`, `Date64`s, as the `Date32`s of the days they are within.
fn days(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    instants::widened(array, &DataType::Date32)
}

/// `array`, `list`, with its items' `Date64`s dated, its items' field `field`.
fn list<O: OffsetSizeTrait>(
    array: &ArrayRef,
    list: &GenericListArray<O>,
    field: &FieldRef,
) -> Result<ArrayRef, ArrowError> {
    let values = dated(list.values())?;
    if Arc::ptr_eq(&values, list.values()) {
        return Ok(Arc::clone(array));
    }
    let field = retyped(field, &values);
    let (offsets, nulls) = (list.offsets().clone(), list.nulls().cloned());
    Ok(Arc::new(GenericListArray::<O>::try_new(
        field, offsets, values, nulls,
    )?))
}

/// The microseconds since the epoch of the instant at `row` of `array`, a date or a timestamp: a
/// date its midnight in UTC, and an instant between two microseconds the earlier; `None` where
/// an `i64` of microseconds cannot hold it.
pub(crate) fn micros_at(array: &dyn Array, row: usize) -> Option<i64> {
    if !matches!(
        array.data_type(),
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(..)
    ) {
        return None;
    }
    let value = instants::stored(array, row)?;
    let nanos = instants::nanos(array.data_type(), i128::from(value))?;
    i64::try_from(nanos.div_euclid(1_000)).ok()
}
