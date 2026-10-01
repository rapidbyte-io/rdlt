//! Narrows a column sliced from a larger one to what its rows name, so that its frame carries
//! nothing else: Arrow's writer sends the data buffers of views and the children of list views,
//! dense unions and run-end columns whole, however few rows name them.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::{Int16Type, Int32Type, Int64Type, RunEndIndexType};
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, GenericListArray, GenericListViewArray, MapArray,
    OffsetSizeTrait, PrimitiveArray, RecordBatch, RecordBatchOptions, RunArray, StructArray,
    UInt64Array, UnionArray, make_array,
};
use arrow_buffer::{ArrowNativeType as _, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType};
use arrow_select::take::take;

/// `batch` with each column holding only what its rows name.
pub(super) fn compacted(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    let columns: Result<Vec<_>, _> = batch.columns().iter().map(column).collect();
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(batch.schema(), columns?, &options)
}

/// `array` holding only what its rows name, the columns nested in it too.
fn column(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    Ok(match array.data_type() {
        DataType::Utf8View => Arc::new(array.as_string_view().gc()),
        DataType::BinaryView => Arc::new(array.as_binary_view().gc()),
        DataType::List(_) => list(array.as_list::<i32>())?,
        DataType::LargeList(_) => list(array.as_list::<i64>())?,
        DataType::ListView(_) => list_view(array.as_list_view::<i32>())?,
        DataType::LargeListView(_) => list_view(array.as_list_view::<i64>())?,
        DataType::Map(field, sorted) => {
            let map = array.as_map();
            let (offsets, entries) = within(map.offsets(), map.entries())?;
            let nulls = map.nulls().cloned();
            let entries = entries.as_struct().clone();
            Arc::new(MapArray::try_new(
                Arc::clone(field),
                offsets,
                entries,
                nulls,
                *sorted,
            )?)
        }
        DataType::FixedSizeList(field, size) => {
            let items = column(array.as_fixed_size_list().values())?;
            let nulls = array.nulls().cloned();
            let field = Arc::clone(field);
            Arc::new(FixedSizeListArray::try_new_with_length(
                field,
                *size,
                items,
                nulls,
                array.len(),
            )?)
        }
        DataType::Struct(fields) => {
            let parent = array.as_struct();
            let columns: Result<Vec<_>, _> = parent.columns().iter().map(column).collect();
            let nulls = parent.nulls().cloned();
            Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                columns?,
                nulls,
                array.len(),
            )?)
        }
        DataType::Union(..) => union(array.as_union())?,
        DataType::RunEndEncoded(ends, _) => match ends.data_type() {
            DataType::Int16 => runs(array.as_run::<Int16Type>())?,
            DataType::Int32 => runs(array.as_run::<Int32Type>())?,
            _ => runs(array.as_run::<Int64Type>())?,
        },
        // Arrow's writer cuts every other column's buffers to its rows; a dictionary's values
        // travel once, in a frame of their own.
        _ => Arc::clone(array),
    })
}

/// `offsets` counted from zero, and the items of `items` they span, compacted.
fn within<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    items: &dyn Array,
) -> Result<(OffsetBuffer<O>, ArrayRef), ArrowError> {
    let (first, last) = (offsets[0], offsets[offsets.len() - 1]);
    let spanned = items.slice(first.as_usize(), (last - first).as_usize());
    let rebased: ScalarBuffer<O> = offsets.iter().map(|offset| *offset - first).collect();
    Ok((OffsetBuffer::new(rebased), column(&spanned)?))
}

fn list<O: OffsetSizeTrait>(lists: &GenericListArray<O>) -> Result<ArrayRef, ArrowError> {
    let (DataType::List(field) | DataType::LargeList(field)) = lists.data_type() else {
        return Ok(Arc::new(lists.clone()));
    };
    let (offsets, items) = within(lists.offsets(), lists.values())?;
    let nulls = lists.nulls().cloned();
    Ok(Arc::new(GenericListArray::try_new(
        Arc::clone(field),
        offsets,
        items,
        nulls,
    )?))
}

/// The list views as lists laid end to end over only the items they name, in their order.
fn list_view<O: OffsetSizeTrait>(lists: &GenericListViewArray<O>) -> Result<ArrayRef, ArrowError> {
    let (DataType::ListView(field) | DataType::LargeListView(field)) = lists.data_type() else {
        return Ok(Arc::new(lists.clone()));
    };
    let (mut named, mut offsets, mut sizes) = (Vec::new(), Vec::new(), Vec::new());
    for row in 0..lists.len() {
        let (offset, size) = if lists.is_null(row) {
            (0, 0)
        } else {
            (
                lists.offsets()[row].as_usize(),
                lists.sizes()[row].as_usize(),
            )
        };
        offsets.push(O::usize_as(named.len()));
        sizes.push(O::usize_as(size));
        named.extend((offset..offset + size).map(wide));
    }
    let items = column(&take(lists.values(), &UInt64Array::from(named), None)?)?;
    let nulls = lists.nulls().cloned();
    let field = Arc::clone(field);
    Ok(Arc::new(GenericListViewArray::try_new(
        field,
        offsets.into(),
        sizes.into(),
        items,
        nulls,
    )?))
}

/// The union with each child holding only the values its rows name, in their order.
fn union(union: &UnionArray) -> Result<ArrayRef, ArrowError> {
    let DataType::Union(fields, _) = union.data_type() else {
        return Ok(Arc::new(union.clone()));
    };
    let Some(offsets) = union.offsets() else {
        let children = fields.iter().map(|(id, _)| column(union.child(id)));
        let children: Result<Vec<_>, _> = children.collect();
        let ids = union.type_ids().clone();
        return Ok(Arc::new(UnionArray::try_new(
            fields.clone(),
            ids,
            None,
            children?,
        )?));
    };
    let mut children = Vec::new();
    let mut moved = vec![0_i32; union.len()];
    for (id, _) in fields.iter() {
        let mut named = Vec::new();
        for row in (0..union.len()).filter(|row| union.type_ids()[*row] == id) {
            moved[row] = i32::try_from(named.len()).unwrap_or(i32::MAX);
            named.push(wide(usize::try_from(offsets[row]).unwrap_or(usize::MAX)));
        }
        children.push(column(&take(
            union.child(id),
            &UInt64Array::from(named),
            None,
        )?)?);
    }
    let ids = union.type_ids().clone();
    Ok(Arc::new(UnionArray::try_new(
        fields.clone(),
        ids,
        Some(moved.into()),
        children,
    )?))
}

/// The runs that reach into the column's rows, ending where the rows count them.
fn runs<R: RunEndIndexType>(runs: &RunArray<R>) -> Result<ArrayRef, ArrowError> {
    if runs.is_empty() {
        return Ok(arrow_array::new_empty_array(runs.data_type()));
    }
    let (first, last) = (
        runs.get_start_physical_index(),
        runs.get_end_physical_index(),
    );
    let (offset, rows) = (runs.run_ends().offset(), runs.len());
    let ends = runs.run_ends().values()[first..=last].iter().map(|end| {
        let end = end.as_usize().saturating_sub(offset).min(rows);
        R::Native::from_usize(end).unwrap_or_default()
    });
    let ends = PrimitiveArray::<R>::from_iter_values(ends);
    let values = column(&runs.values_slice())?;
    // Rebuilt under the column's own type: its fields may be named, nullable or described
    // otherwise than a new run-end array's.
    let rebuilt = runs
        .to_data()
        .into_builder()
        .offset(0)
        .len(rows)
        .child_data(vec![ends.into_data(), values.into_data()]);
    Ok(make_array(rebuilt.build()?))
}

/// An index as `take` reads it.
fn wide(index: usize) -> u64 {
    u64::try_from(index).unwrap_or(u64::MAX)
}
