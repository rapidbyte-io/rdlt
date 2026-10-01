//! Builds each layout's column as its rows are weighed.

use arrow_array::cast::AsArray as _;
use arrow_array::types::{ArrowDictionaryKeyType, RunEndIndexType};
use arrow_array::{Array, GenericListArray, GenericListViewArray, OffsetSizeTrait, RunArray};
use arrow_buffer::{ArrowNativeType as _, NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, UnionFields, UnionMode};

use super::column::{Column, Counts, Expanded, Named};
use crate::codec::compact::plain;

/// Bits: a validity bit, which Arrow's writer sends for every value of a column that has one.
pub(super) const VALID: u64 = 1;

pub(super) fn wide(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

/// Where item `index` begins by `offsets`; nowhere where they do not reach.
fn offset<O: OffsetSizeTrait>(offsets: &OffsetBuffer<O>) -> Named<usize> {
    let offsets = offsets.clone();
    Box::new(move |index: usize| offsets.get(index).map_or(0, |offset| offset.as_usize()))
}

/// A column of the fixed-width type `data_type`: nulls, booleans and values of whole bytes.
pub(super) fn fixed(data_type: &DataType) -> Column {
    let bytes = match data_type {
        DataType::Null => return Column::Fixed { bits: 0 },
        DataType::Boolean => return Column::Fixed { bits: 1 + VALID },
        DataType::FixedSizeBinary(bytes) => usize::try_from(*bytes).unwrap_or(0),
        fixed => fixed.primitive_width().unwrap_or(0),
    };
    Column::Fixed {
        bits: 8 * wide(bytes) + VALID,
    }
}

pub(super) fn bytes<O: OffsetSizeTrait>(offsets: &OffsetBuffer<O>) -> Column {
    Column::Bytes {
        bits: 8 * wide(size_of::<O>()) + VALID,
        offset: offset(offsets),
    }
}

pub(super) fn lists<O: OffsetSizeTrait>(
    lists: &GenericListArray<O>,
    rebuilt: bool,
    counts: &mut Counts,
) -> Column {
    let items = lists.values().as_ref();
    listed(lists.offsets(), lists.nulls(), items, rebuilt, counts)
}

/// Lists of `items` by `offsets`; where `rebuilt`, a row null by `nulls` spans none.
pub(super) fn listed<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    nulls: Option<&NullBuffer>,
    items: &dyn Array,
    rebuilt: bool,
    counts: &mut Counts,
) -> Column {
    let nulls = nulls.filter(|nulls| rebuilt && nulls.null_count() > 0);
    Column::List {
        bits: 8 * wide(size_of::<O>()) + VALID,
        offset: offset(offsets),
        nulls: nulls.cloned(),
        item: Box::new(Column::of(items, rebuilt, counts)),
    }
}

/// Fixed-size lists of `size` items a row.
pub(super) fn fixed_lists(
    array: &dyn Array,
    size: i32,
    rebuilt: bool,
    counts: &mut Counts,
) -> Column {
    let items = array.as_fixed_size_list().values().as_ref();
    Column::Sized {
        size: usize::try_from(size).unwrap_or(0),
        item: Box::new(Column::of(items, rebuilt, counts)),
    }
}

pub(super) fn list_views<O: OffsetSizeTrait>(
    lists: &GenericListViewArray<O>,
    rebuilt: bool,
    counts: &mut Counts,
) -> Column {
    let (offsets, sizes, nulls) = (
        lists.offsets().clone(),
        lists.sizes().clone(),
        lists.nulls().cloned(),
    );
    // A null row names nothing, whatever its offset and size say.
    let range = move |row: usize| {
        let null = nulls.as_ref().is_some_and(|nulls| nulls.is_null(row));
        match (offsets.get(row), sizes.get(row)) {
            (Some(offset), Some(size)) if !null => (
                offset.as_usize(),
                offset.as_usize().saturating_add(size.as_usize()),
            ),
            _ => (0, 0),
        }
    };
    Column::ListView {
        bits: 16 * wide(size_of::<O>()) + VALID,
        range: Box::new(range),
        item: Box::new(Column::of(lists.values().as_ref(), rebuilt, counts)),
    }
}

pub(super) fn union(
    array: &dyn Array,
    fields: &UnionFields,
    mode: UnionMode,
    rebuilt: bool,
    counts: &mut Counts,
) -> Column {
    let union = array.as_union();
    let children = fields
        .iter()
        .map(|(id, _)| Column::of(union.child(id).as_ref(), rebuilt, counts));
    let children: Vec<_> = children.collect();
    let Some(offsets) = union
        .offsets()
        .filter(|_| mode == UnionMode::Dense)
        .cloned()
    else {
        return Column::Each { bits: 8, children };
    };
    let ids: Vec<i8> = fields.iter().map(|(id, _)| id).collect();
    let of = union.type_ids().clone();
    let named = move |row: usize| {
        let child = of
            .get(row)
            .and_then(|of| ids.iter().position(|id| id == of));
        let item = offsets.get(row).map(|item| usize::try_from(*item));
        match (child, item) {
            (Some(child), Some(Ok(item))) => (child, item),
            _ => (usize::MAX, 0),
        }
    };
    Column::Dense {
        named: Box::new(named),
        children,
    }
}

pub(super) fn runs<R: RunEndIndexType>(
    runs: &RunArray<R>,
    rebuilt: bool,
    counts: &mut Counts,
) -> Column {
    // The run ends are a column of their own in a frame.
    counts.nodes += 1;
    counts.buffers += 2;
    let place = counts.runs;
    counts.runs += 1;
    let ends = runs.run_ends().clone();
    // The run a row is in, and the row that run ends before.
    let reach = move |row: usize| {
        let run = ends.get_physical_index(row);
        let end = ends.values().get(run).map_or(0, |end| end.as_usize());
        (run, end.saturating_sub(ends.offset()))
    };
    Column::Runs {
        bits: 8 * wide(size_of::<R::Native>()) + VALID,
        reach: Box::new(reach),
        place,
        values: Box::new(Column::of(runs.values().as_ref(), rebuilt, counts)),
        expanded: Expanded::default(),
    }
}

pub(super) fn keyed<K: ArrowDictionaryKeyType>(array: &dyn Array, counts: &mut Counts) -> Column {
    let keyed = array.as_dictionary::<K>();
    let keys = keyed.keys().clone();
    let key = move |row: usize| {
        (row < keys.len() && keys.is_valid(row)).then(|| keys.value(row).as_usize())
    };
    // A dictionary's values are in a frame of their own: none of their nodes or buffers is in
    // the batch's.
    let mut apart = Counts {
        runs: counts.runs,
        ..Counts::default()
    };
    let values = keyed.values();
    let values = Column::of(values.as_ref(), !plain(values.data_type()), &mut apart);
    counts.runs = apart.runs;
    Column::Keyed {
        bits: 8 * wide(size_of::<K::Native>()) + VALID,
        key: Box::new(key),
        values: Box::new(values),
        expanded: Expanded::default(),
    }
}
