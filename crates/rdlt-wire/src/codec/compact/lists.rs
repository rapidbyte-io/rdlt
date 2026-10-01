//! Rebuilds lists, maps and list views from the rows named: their items end to end, in the
//! order the rows name them.

use std::sync::Arc;

use arrow_array::{
    Array as _, ArrayRef, GenericListArray, GenericListViewArray, MapArray, OffsetSizeTrait,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType};

use super::leaf::validity;
use super::{Narrower, Ranges, name, sized};

/// Offsets counted from zero, and the ranges of items they span.
type Spans<O> = (OffsetBuffer<O>, Vec<(usize, usize)>);

/// The offsets of the rows `ranges` name counted from zero, and the ranges of items they
/// span: a null row spans none.
fn spans<O: OffsetSizeTrait + TryFrom<usize>>(
    offsets: &OffsetBuffer<O>,
    nulls: Option<&NullBuffer>,
    ranges: &Ranges,
) -> Result<Spans<O>, ArrowError> {
    let (mut ends, mut items, mut total) = (vec![O::default()], Vec::new(), 0_usize);
    for row in ranges.iter().flat_map(|(start, end)| *start..*end) {
        let (Some(start), Some(end)) = (offsets.get(row), offsets.get(row + 1)) else {
            return Err(ArrowError::InvalidArgumentError(format!(
                "row {row} is beyond the column's offsets"
            )));
        };
        if nulls.is_none_or(|nulls| nulls.is_valid(row)) {
            let (start, end) = (start.as_usize(), end.as_usize());
            name(&mut items, start, end);
            total = total.saturating_add(end.saturating_sub(start));
        }
        ends.push(sized::<O>(total)?);
    }
    Ok((OffsetBuffer::new(ScalarBuffer::from(ends)), items))
}

impl Narrower {
    /// Lists: the items of each row named that is not null.
    pub(super) fn lists<O: OffsetSizeTrait + TryFrom<usize>>(
        &mut self,
        lists: &GenericListArray<O>,
        ranges: &Ranges,
    ) -> Result<ArrayRef, ArrowError> {
        let (DataType::List(field) | DataType::LargeList(field)) = lists.data_type() else {
            return Ok(Arc::new(lists.clone()));
        };
        let (offsets, items) = spans(lists.offsets(), lists.nulls(), ranges)?;
        let items = self.gathered(lists.values(), &items)?;
        let nulls = validity(lists.nulls(), ranges);
        let lists = GenericListArray::try_new(Arc::clone(field), offsets, items, nulls)?;
        Ok(Arc::new(lists))
    }

    /// Maps: the entries of each row named that is not null.
    pub(super) fn maps(
        &mut self,
        maps: &MapArray,
        ranges: &Ranges,
    ) -> Result<ArrayRef, ArrowError> {
        let DataType::Map(field, sorted) = maps.data_type() else {
            return Ok(Arc::new(maps.clone()));
        };
        let (offsets, entries) = spans(maps.offsets(), maps.nulls(), ranges)?;
        let entries = self.each(maps.entries(), &entries)?;
        let nulls = validity(maps.nulls(), ranges);
        let entries = arrow_array::cast::AsArray::as_struct(&entries).clone();
        let maps = MapArray::try_new(Arc::clone(field), offsets, entries, nulls, *sorted)?;
        Ok(Arc::new(maps))
    }

    /// List views: the items each row named that is not null names, end to end.
    pub(super) fn list_views<O: OffsetSizeTrait + TryFrom<usize>>(
        &mut self,
        lists: &GenericListViewArray<O>,
        ranges: &Ranges,
    ) -> Result<ArrayRef, ArrowError> {
        let (DataType::ListView(field) | DataType::LargeListView(field)) = lists.data_type() else {
            return Ok(Arc::new(lists.clone()));
        };
        let (mut offsets, mut sizes, mut items, mut total) =
            (Vec::new(), Vec::new(), Vec::new(), 0);
        for row in ranges.iter().flat_map(|(start, end)| *start..*end) {
            let (offset, size) = match (lists.offsets().get(row), lists.sizes().get(row)) {
                (Some(offset), Some(size)) if lists.is_valid(row) => {
                    (offset.as_usize(), size.as_usize())
                }
                _ => (0, 0),
            };
            name(&mut items, offset, offset.saturating_add(size));
            offsets.push(sized::<O>(total)?);
            sizes.push(sized::<O>(size)?);
            total = total.saturating_add(size);
        }
        let items = self.gathered(lists.values(), &items)?;
        let nulls = validity(lists.nulls(), ranges);
        let field = Arc::clone(field);
        let lists =
            GenericListViewArray::try_new(field, offsets.into(), sizes.into(), items, nulls)?;
        Ok(Arc::new(lists))
    }
}
