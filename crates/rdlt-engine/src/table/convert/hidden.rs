//! What a null row holds beneath it is no value: a struct's fields and a fixed-size list's items
//! under a null row are null too, and a list's or a map's items under one are dropped, so nothing
//! converts, checks or counts them.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, GenericListArray, MapArray, OffsetSizeTrait, StructArray,
    UInt64Array, make_array,
};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow_schema::{ArrowError, DataType};
use arrow_select::take::take;

/// `array`, holding only what its rows name, with nothing beneath a null row at any depth;
/// `array` itself where nothing is.
pub(super) fn unhidden(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Struct(fields) => {
            let rows = array.as_struct();
            let columns = rows
                .columns()
                .iter()
                .map(|column| unhidden(&masked(column, rows.nulls())?))
                .collect::<Result<Vec<_>, _>>()?;
            if unchanged(&columns, rows.columns()) {
                return Ok(Arc::clone(array));
            }
            let nulls = rows.nulls().cloned();
            Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                columns,
                nulls,
            )?))
        }
        DataType::List(field) => listed(array, array.as_list::<i32>(), field),
        DataType::LargeList(field) => listed(array, array.as_list::<i64>(), field),
        DataType::FixedSizeList(field, size) => {
            let list = array.as_fixed_size_list();
            let width = usize::try_from(*size).unwrap_or(0);
            let items = list.nulls().map(|nulls| expanded(nulls, width));
            let values = unhidden(&masked(list.values(), items.as_ref())?)?;
            if Arc::ptr_eq(&values, list.values()) {
                return Ok(Arc::clone(array));
            }
            let nulls = list.nulls().cloned();
            let list = FixedSizeListArray::try_new(Arc::clone(field), *size, values, nulls)?;
            Ok(Arc::new(list))
        }
        DataType::Map(field, sorted) => {
            let map = array.as_map();
            let entries: ArrayRef = Arc::new(map.entries().clone());
            let (offsets, named) = match spanned(map.offsets(), map.nulls()) {
                Some((offsets, named)) => (offsets, take(entries.as_ref(), &named, None)?),
                None => (map.offsets().clone(), Arc::clone(&entries)),
            };
            let named = unhidden(&named)?;
            if Arc::ptr_eq(&named, &entries) {
                return Ok(Arc::clone(array));
            }
            let (entries, nulls) = (named.as_struct().clone(), map.nulls().cloned());
            let map = MapArray::try_new(Arc::clone(field), offsets, entries, nulls, *sorted)?;
            Ok(Arc::new(map))
        }
        _ => Ok(Arc::clone(array)),
    }
}

/// `list`, `array`, with the items its null rows span dropped and the rest unhidden.
fn listed<O: OffsetSizeTrait>(
    array: &ArrayRef,
    list: &GenericListArray<O>,
    field: &arrow_schema::FieldRef,
) -> Result<ArrayRef, ArrowError> {
    let (offsets, named) = match spanned(list.offsets(), list.nulls()) {
        Some((offsets, named)) => (offsets, take(list.values().as_ref(), &named, None)?),
        None => (list.offsets().clone(), Arc::clone(list.values())),
    };
    let values = unhidden(&named)?;
    if Arc::ptr_eq(&values, list.values()) {
        return Ok(Arc::clone(array));
    }
    let nulls = list.nulls().cloned();
    let list = GenericListArray::<O>::try_new(Arc::clone(field), offsets, values, nulls)?;
    Ok(Arc::new(list))
}

/// Where a null row of a list bounded by `offsets` spans items: the offsets of the list holding
/// only its other rows' items, and the positions of those items; `None` where none does.
fn spanned<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    nulls: Option<&NullBuffer>,
) -> Option<(OffsetBuffer<O>, UInt64Array)> {
    let nulls = nulls.filter(|nulls| nulls.null_count() > 0)?;
    let span = |row: usize| offsets[row].as_usize()..offsets[row + 1].as_usize();
    let rows = offsets.len().saturating_sub(1);
    if !(0..rows).any(|row| nulls.is_null(row) && !span(row).is_empty()) {
        return None;
    }
    let mut named = Vec::new();
    let mut kept = Vec::with_capacity(offsets.len());
    kept.push(O::usize_as(0));
    for row in 0..rows {
        if nulls.is_valid(row) {
            named.extend(span(row).map(|item| item as u64));
        }
        kept.push(O::usize_as(named.len()));
    }
    Some((OffsetBuffer::new(kept.into()), UInt64Array::from(named)))
}

/// `array` null wherever `nulls` says, beside its own nulls; `array` itself where that adds none
/// or its type keeps no nulls of its own.
fn masked(array: &ArrayRef, nulls: Option<&NullBuffer>) -> Result<ArrayRef, ArrowError> {
    let Some(nulls) = nulls.filter(|nulls| nulls.null_count() > 0) else {
        return Ok(Arc::clone(array));
    };
    if matches!(array.data_type(), DataType::Null | DataType::Union(..)) {
        return Ok(Arc::clone(array));
    }
    let both = NullBuffer::union(Some(nulls), array.nulls());
    if both.as_ref().map(NullBuffer::null_count) == array.nulls().map(NullBuffer::null_count) {
        return Ok(Arc::clone(array));
    }
    let data = array.to_data().into_builder().nulls(both).build()?;
    Ok(make_array(data))
}

/// `nulls`, of rows of `width` items each, for the items.
fn expanded(nulls: &NullBuffer, width: usize) -> NullBuffer {
    let items = (0..nulls.len() * width).map(|item| nulls.is_valid(item / width.max(1)));
    NullBuffer::new(BooleanBuffer::from_iter(items))
}

/// Whether each array of `new` is the very array of `old` beside it.
fn unchanged(new: &[ArrayRef], old: &[ArrayRef]) -> bool {
    new.iter().zip(old).all(|(new, old)| Arc::ptr_eq(new, old))
}
