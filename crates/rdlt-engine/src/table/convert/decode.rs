//! Arrays cut down to what their rows name, before anything converts them.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int16Type, Int32Type, Int64Type, RunEndIndexType, UInt64Type};
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, GenericListArray, MapArray, OffsetSizeTrait,
    StructArray, UInt32Array, UInt64Array,
};
use arrow_buffer::{ArrowNativeType, OffsetBuffer};
use arrow_schema::{ArrowError, DataType, FieldRef};
use arrow_select::take::{TakeOptions, take};

use super::retyped;

/// `array` holding only what its rows name; an array that already does is returned as it is.
pub(super) fn decoded(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Dictionary(_, values) => decoded(&keyed(array, values)?),
        DataType::RunEndEncoded(ends, _) => {
            let (values, runs) = match ends.data_type() {
                DataType::Int16 => runs::<Int16Type>(array)?,
                DataType::Int32 => runs::<Int32Type>(array)?,
                _ => runs::<Int64Type>(array)?,
            };
            decoded(&take(values.as_ref(), &runs, CHECKED)?)
        }
        DataType::List(field) => listed::<i32>(array, field),
        DataType::LargeList(field) => listed::<i64>(array, field),
        // A list view's items are named a row at a time, so they are taken into a list.
        DataType::ListView(field) => viewed::<i32>(array, field),
        DataType::LargeListView(field) => viewed::<i64>(array, field),
        DataType::FixedSizeList(field, size) => fixed(array, field, *size),
        DataType::Map(field, sorted) => mapped(array, field, *sorted),
        DataType::Struct(fields) => {
            let rows = array.as_struct();
            let columns = rows
                .columns()
                .iter()
                .map(decoded)
                .collect::<Result<Vec<_>, _>>()?;
            if columns
                .iter()
                .zip(rows.columns())
                .all(|(new, old)| Arc::ptr_eq(new, old))
            {
                return Ok(Arc::clone(array));
            }
            let fields = fields
                .iter()
                .zip(&columns)
                .map(|(field, column)| retyped(field, column))
                .collect();
            Ok(Arc::new(StructArray::try_new(
                fields,
                columns,
                rows.nulls().cloned(),
            )?))
        }
        _ => Ok(Arc::clone(array)),
    }
}

/// The lists the rows of `array`, a list view of `field`, name: each row's items taken in order
/// into a list, a null row's none.
///
/// Only the items named are looked at, however long the view's child is: what is held beside
/// the list is a place for each item named.
fn viewed<O: OffsetSizeTrait>(array: &ArrayRef, field: &FieldRef) -> Result<ArrayRef, ArrowError> {
    let views = array.as_list_view::<O>();
    let beyond =
        || ArrowError::ComputeError("a list view names more items than a list holds".into());
    let named = |row: usize| {
        let (first, size) = (
            views.offsets()[row].as_usize(),
            views.sizes()[row].as_usize(),
        );
        if views.is_null(row) {
            first..first
        } else {
            first..first.saturating_add(size)
        }
    };
    let total = (0..views.len())
        .map(|row| named(row).len())
        .fold(0, usize::saturating_add);
    let mut places = Vec::with_capacity(total);
    let mut offsets = Vec::with_capacity(views.len() + 1);
    offsets.push(O::zero());
    for row in 0..views.len() {
        places.extend(named(row).map(|item| item as u64));
        offsets.push(O::from_usize(places.len()).ok_or_else(beyond)?);
    }
    let items = take(views.values().as_ref(), &UInt64Array::from(places), CHECKED)?;
    let items = decoded(&items)?;
    Ok(Arc::new(GenericListArray::<O>::try_new(
        retyped(field, &items),
        OffsetBuffer::new(offsets.into()),
        items,
        views.nulls().cloned(),
    )?))
}

/// Takes with its indices checked: a key naming no value is an error, not a panic.
const CHECKED: Option<TakeOptions> = Some(TakeOptions { check_bounds: true });

/// The values of `array`, a dictionary of `values`, its keys name, a null for a null key.
fn keyed(array: &ArrayRef, values: &DataType) -> Result<ArrayRef, ArrowError> {
    let dictionary = array.as_any_dictionary();
    let keys = dictionary.keys();
    // A run-end encoding has no nulls of its own for a null key to become, so each key names
    // its run's value.
    if let DataType::RunEndEncoded(ends, _) = values {
        let keys = arrow_cast::cast(keys, &DataType::UInt64)?;
        let keys = keys.as_primitive::<UInt64Type>();
        let (values, runs) = match ends.data_type() {
            DataType::Int16 => keyed_runs::<Int16Type>(dictionary.values(), keys),
            DataType::Int32 => keyed_runs::<Int32Type>(dictionary.values(), keys),
            _ => keyed_runs::<Int64Type>(dictionary.values(), keys),
        };
        return take(values.as_ref(), &runs, CHECKED);
    }
    let Some(nulls) = keys.nulls().filter(|_| holds_runs(values)) else {
        return take(dictionary.values().as_ref(), keys, CHECKED);
    };
    // Taken through a null key, a run-end encoding nested in the values would hold whatever
    // run the key's bytes name. The keys that name values are taken and decoded alone, and the
    // rows of null keys are nulls of the decoded type.
    let named = BooleanArray::new(nulls.inner().clone(), None);
    let present = arrow_select::filter::filter(keys, &named)?;
    let decoded = decoded(&take(dictionary.values().as_ref(), &present, CHECKED)?)?;
    let mut next = 0_u32;
    let places: UInt32Array = nulls
        .iter()
        .map(|named| {
            named.then(|| {
                next += 1;
                next - 1
            })
        })
        .collect();
    take(decoded.as_ref(), &places, CHECKED)
}

/// Whether a value of `data_type`, taken through a null key, takes a run-end encoding from the
/// place the key's bytes name: one of its own, or one in a struct's or a union's members.
///
/// Taken through a null key, a list takes none of its items and a dictionary none of its values,
/// whatever they hold.
fn holds_runs(data_type: &DataType) -> bool {
    match data_type {
        DataType::RunEndEncoded(..) => true,
        DataType::Struct(fields) => fields.iter().any(|field| holds_runs(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| holds_runs(field.data_type())),
        _ => false,
    }
}

/// `array`, a list of `size` items a row, holding only the items its rows name, decoded.
fn fixed(array: &ArrayRef, field: &FieldRef, size: i32) -> Result<ArrayRef, ArrowError> {
    let list = array.as_fixed_size_list();
    let width = usize::try_from(size).unwrap_or(0);
    let first = usize::try_from(list.value_offset(0)).unwrap_or(0);
    let named = list.len().saturating_mul(width);
    let old = list.values();
    let values = decoded(&within(old, first, named))?;
    if Arc::ptr_eq(&values, old) {
        return Ok(Arc::clone(array));
    }
    let field = retyped(field, &values);
    let nulls = list.nulls().cloned();
    Ok(Arc::new(FixedSizeListArray::try_new(
        field, size, values, nulls,
    )?))
}

/// `array`, a map, holding only the entries its rows name, decoded.
fn mapped(array: &ArrayRef, field: &FieldRef, sorted: bool) -> Result<ArrayRef, ArrowError> {
    let map = array.as_map();
    let old: ArrayRef = Arc::new(map.entries().clone());
    let (offsets, first, named) = rebased(map.offsets());
    let entries = decoded(&within(&old, first, named))?;
    if Arc::ptr_eq(&entries, &old) {
        return Ok(Arc::clone(array));
    }
    let field = retyped(field, &entries);
    let nulls = map.nulls().cloned();
    let entries = entries.as_struct().clone();
    Ok(Arc::new(MapArray::try_new(
        field, offsets, entries, nulls, sorted,
    )?))
}

/// `array`, a list of offsets of width `O`, holding only the items its rows name, decoded.
fn listed<O: OffsetSizeTrait>(array: &ArrayRef, field: &FieldRef) -> Result<ArrayRef, ArrowError> {
    let list = array.as_list::<O>();
    let old = list.values();
    let (offsets, first, named) = rebased(list.offsets());
    let values = decoded(&within(old, first, named))?;
    if Arc::ptr_eq(&values, old) {
        return Ok(Arc::clone(array));
    }
    let field = retyped(field, &values);
    Ok(Arc::new(GenericListArray::<O>::try_new(
        field,
        offsets,
        values,
        list.nulls().cloned(),
    )?))
}

/// `values` from `first` for `named` items; `values` itself where that is all of it.
fn within(values: &ArrayRef, first: usize, named: usize) -> ArrayRef {
    if first == 0 && named == values.len() {
        Arc::clone(values)
    } else {
        let first = first.min(values.len());
        values.slice(first, named.min(values.len().saturating_sub(first)))
    }
}

/// `offsets` counted from their first, where the items they name start, and how many they name.
fn rebased<O: OffsetSizeTrait>(offsets: &OffsetBuffer<O>) -> (OffsetBuffer<O>, usize, usize) {
    let first = offsets.first().map_or(0, |offset| offset.as_usize());
    let last = offsets.last().map_or(0, |offset| offset.as_usize());
    if first == 0 {
        return (offsets.clone(), first, last);
    }
    let start = offsets[0];
    let rebased = offsets.iter().map(|offset| *offset - start).collect();
    // Offsets that only grew still only grow once each is less the first.
    (OffsetBuffer::new(rebased), first, last - first)
}

/// The values of the run-end encoded `array`, and which of them each of its rows holds.
fn runs<R: RunEndIndexType>(array: &ArrayRef) -> Result<(ArrayRef, UInt32Array), ArrowError> {
    let runs = array.as_run::<R>();
    let ends = runs.run_ends();
    let mut run = ends.get_start_physical_index();
    let rows = (ends.offset()..ends.offset() + ends.len()).map(|row| {
        while ends
            .values()
            .get(run)
            .is_some_and(|end| end.as_usize() <= row)
        {
            run += 1;
        }
        u32::try_from(run)
    });
    let rows = rows
        .collect::<Result<Vec<u32>, _>>()
        .map_err(|_| ArrowError::ComputeError("a run-end encoding of too many runs".to_owned()))?;
    Ok((Arc::clone(runs.values()), UInt32Array::from(rows)))
}

/// The values of the run-end encoded `array`, and which of them each of `keys` names: a null key
/// none, and so does a key beyond the array's rows, which then fails the take.
fn keyed_runs<R: RunEndIndexType>(array: &ArrayRef, keys: &UInt64Array) -> (ArrayRef, UInt32Array) {
    let runs = array.as_run::<R>();
    let ends = runs.run_ends();
    let named = keys.iter().map(|key| {
        let row = usize::try_from(key?).unwrap_or(usize::MAX);
        if row >= ends.len() {
            return Some(u32::MAX);
        }
        Some(u32::try_from(ends.get_physical_index(row)).unwrap_or(u32::MAX))
    });
    (Arc::clone(runs.values()), named.collect())
}

#[cfg(test)]
mod tests;
