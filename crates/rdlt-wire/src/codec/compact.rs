//! Narrows a column to what its rows name, so that its frame carries nothing else and is
//! counted by its receiver as its sender weighed it.
//!
//! Arrow's writer sends the data buffers of views and the children of list views and dense
//! unions whole, however few rows name them, and does not move a union under a list whose
//! first offset is not zero to where the list's items begin. Only a column of a layout the
//! writer sends as its rows goes as it is; every other is rebuilt.

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

/// Whether Arrow's writer sends a column of `data_type` as the rows it names and nothing
/// else, whatever the column was sliced from.
///
/// Fixed-width values, bytes by offsets, and structs, lists and dictionaries of those do; a
/// type not named here is narrowed.
pub(super) fn plain(data_type: &DataType) -> bool {
    use DataType as T;
    match data_type {
        T::Null
        | T::Boolean
        | T::FixedSizeBinary(_)
        | T::Utf8
        | T::Binary
        | T::LargeUtf8
        | T::LargeBinary => true,
        T::List(item) | T::LargeList(item) | T::FixedSizeList(item, _) => plain(item.data_type()),
        T::Struct(fields) => fields.iter().all(|field| plain(field.data_type())),
        T::Dictionary(_, values) => plain(values),
        other => other.is_primitive(),
    }
}

/// `batch` with each column holding only what its rows name.
#[cfg(test)]
pub(super) fn compacted(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    Narrower::default().batch(batch)
}

/// The values of a batch's dictionaries that are not plain, and each rebuilt.
type Rebuilt = Vec<(ArrayRef, ArrayRef)>;

/// Narrows the pieces of one batch, rebuilding each dictionary's values once for them all.
#[derive(Debug, Default)]
pub(super) struct Narrower {
    rebuilt: Rebuilt,
}

impl Narrower {
    /// `batch` with each column holding only what its rows name.
    pub(super) fn batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
        let columns = batch.columns().iter();
        let columns = columns.map(|array| column(array, &mut self.rebuilt));
        let columns: Result<Vec<_>, _> = columns.collect();
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        RecordBatch::try_new_with_options(batch.schema(), columns?, &options)
    }
}

/// The dictionary `array` over its values holding only what their rows name: the same values
/// for every piece, so that they are sent once.
fn keyed(array: &ArrayRef, rebuilt: &mut Rebuilt) -> Result<ArrayRef, ArrowError> {
    let values = array.as_any_dictionary().values();
    let mut known = rebuilt.iter();
    let known = known.find(|(original, _)| Arc::ptr_eq(original, values));
    let narrowed = if let Some((_, narrowed)) = known {
        Arc::clone(narrowed)
    } else {
        let narrowed = column(values, rebuilt)?;
        rebuilt.push((Arc::clone(values), Arc::clone(&narrowed)));
        narrowed
    };
    Ok(array.as_any_dictionary().with_values(narrowed))
}

/// `array` holding only what its rows name, the columns nested in it too.
fn column(array: &ArrayRef, rebuilt: &mut Rebuilt) -> Result<ArrayRef, ArrowError> {
    if plain(array.data_type()) {
        return Ok(Arc::clone(array));
    }
    Ok(match array.data_type() {
        DataType::Utf8View => Arc::new(array.as_string_view().gc()),
        DataType::BinaryView => Arc::new(array.as_binary_view().gc()),
        DataType::List(_) => list(array.as_list::<i32>(), rebuilt)?,
        DataType::LargeList(_) => list(array.as_list::<i64>(), rebuilt)?,
        DataType::ListView(_) => list_view(array.as_list_view::<i32>(), rebuilt)?,
        DataType::LargeListView(_) => list_view(array.as_list_view::<i64>(), rebuilt)?,
        DataType::Map(field, sorted) => {
            let map = array.as_map();
            let (offsets, entries) = within(map.offsets(), map.entries(), rebuilt)?;
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
            let items = column(array.as_fixed_size_list().values(), rebuilt)?;
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
            let columns = parent.columns().iter();
            let columns: Result<Vec<_>, _> = columns.map(|child| column(child, rebuilt)).collect();
            let nulls = parent.nulls().cloned();
            Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                columns?,
                nulls,
                array.len(),
            )?)
        }
        DataType::Union(..) => union(array.as_union(), rebuilt)?,
        DataType::RunEndEncoded(ends, _) => match ends.data_type() {
            DataType::Int16 => runs(array.as_run::<Int16Type>(), rebuilt)?,
            DataType::Int32 => runs(array.as_run::<Int32Type>(), rebuilt)?,
            _ => runs(array.as_run::<Int64Type>(), rebuilt)?,
        },
        DataType::Dictionary(..) => keyed(array, rebuilt)?,
        // A layout no arm above rebuilds goes as it is.
        _ => Arc::clone(array),
    })
}

/// `offsets` counted from zero, and the items of `items` they span, compacted.
fn within<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    items: &dyn Array,
    rebuilt: &mut Rebuilt,
) -> Result<(OffsetBuffer<O>, ArrayRef), ArrowError> {
    let (first, last) = (offsets[0], offsets[offsets.len() - 1]);
    let spanned = items.slice(first.as_usize(), (last - first).as_usize());
    let rebased: ScalarBuffer<O> = offsets.iter().map(|offset| *offset - first).collect();
    Ok((OffsetBuffer::new(rebased), column(&spanned, rebuilt)?))
}

fn list<O: OffsetSizeTrait>(
    lists: &GenericListArray<O>,
    rebuilt: &mut Rebuilt,
) -> Result<ArrayRef, ArrowError> {
    let (DataType::List(field) | DataType::LargeList(field)) = lists.data_type() else {
        return Ok(Arc::new(lists.clone()));
    };
    let (offsets, items) = within(lists.offsets(), lists.values(), rebuilt)?;
    let nulls = lists.nulls().cloned();
    Ok(Arc::new(GenericListArray::try_new(
        Arc::clone(field),
        offsets,
        items,
        nulls,
    )?))
}

/// The list views as lists laid end to end over only the items they name, in their order.
fn list_view<O: OffsetSizeTrait>(
    lists: &GenericListViewArray<O>,
    rebuilt: &mut Rebuilt,
) -> Result<ArrayRef, ArrowError> {
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
    let items = take(lists.values(), &UInt64Array::from(named), None)?;
    let items = column(&items, rebuilt)?;
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
fn union(union: &UnionArray, rebuilt: &mut Rebuilt) -> Result<ArrayRef, ArrowError> {
    let DataType::Union(fields, _) = union.data_type() else {
        return Ok(Arc::new(union.clone()));
    };
    let Some(offsets) = union.offsets() else {
        let children = fields
            .iter()
            .map(|(id, _)| column(union.child(id), rebuilt));
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
        let child = take(union.child(id), &UInt64Array::from(named), None)?;
        children.push(column(&child, rebuilt)?);
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
fn runs<R: RunEndIndexType>(
    runs: &RunArray<R>,
    rebuilt: &mut Rebuilt,
) -> Result<ArrayRef, ArrowError> {
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
    let values = column(&runs.values_slice(), rebuilt)?;
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
