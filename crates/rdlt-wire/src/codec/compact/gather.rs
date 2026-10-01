//! Rebuilds a column from ranges of its rows: each layout from the ranges of items its rows
//! name, so that nothing is copied but what they name.

use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::{Int16Type, Int32Type, Int64Type};
use arrow_array::{Array as _, ArrayRef, FixedSizeListArray, StructArray, make_array};
use arrow_schema::{ArrowError, DataType};

use super::leaf::{leaf, validity};
use super::{Narrower, Ranges, count, name};

impl Narrower {
    /// The rows of `array` that `ranges` name, in their order, holding only what they name.
    pub(super) fn gathered(
        &mut self,
        array: &ArrayRef,
        ranges: &Ranges,
    ) -> Result<ArrayRef, ArrowError> {
        use DataType as T;
        match array.data_type() {
            T::Utf8View => {
                let views = leaf(array, ranges)?;
                Ok(Arc::new(views.as_string_view().gc()))
            }
            T::BinaryView => {
                let views = leaf(array, ranges)?;
                Ok(Arc::new(views.as_binary_view().gc()))
            }
            T::List(_) => self.lists(array.as_list::<i32>(), ranges),
            T::LargeList(_) => self.lists(array.as_list::<i64>(), ranges),
            T::Map(..) => self.maps(array.as_map(), ranges),
            T::ListView(_) => self.list_views(array.as_list_view::<i32>(), ranges),
            T::LargeListView(_) => self.list_views(array.as_list_view::<i64>(), ranges),
            T::FixedSizeList(..) => self.fixed(array.as_fixed_size_list(), ranges),
            T::Struct(_) => self.each(array.as_struct(), ranges),
            T::Union(..) => self.unions(array.as_union(), ranges),
            T::RunEndEncoded(ends, _) => match ends.data_type() {
                T::Int16 => self.runs(array.as_run::<Int16Type>(), ranges),
                T::Int32 => self.runs(array.as_run::<Int32Type>(), ranges),
                _ => self.runs(array.as_run::<Int64Type>(), ranges),
            },
            T::Dictionary(..) => self.keyed(array, ranges),
            _ => leaf(array, ranges),
        }
    }

    /// Fixed-size lists: the items of each row named, null rows' too.
    fn fixed(
        &mut self,
        lists: &FixedSizeListArray,
        ranges: &Ranges,
    ) -> Result<ArrayRef, ArrowError> {
        let DataType::FixedSizeList(field, size) = lists.data_type() else {
            return Ok(Arc::new(lists.clone()));
        };
        let width = usize::try_from(*size).map_err(|_| {
            ArrowError::InvalidArgumentError(format!("lists of {size} items have no size"))
        })?;
        let mut items = Vec::new();
        for (start, end) in ranges {
            name(
                &mut items,
                start.saturating_mul(width),
                end.saturating_mul(width),
            );
        }
        let items = self.gathered(lists.values(), &items)?;
        let nulls = validity(lists.nulls(), ranges);
        let rows = count(ranges);
        let field = Arc::clone(field);
        let lists = FixedSizeListArray::try_new_with_length(field, *size, items, nulls, rows)?;
        Ok(Arc::new(lists))
    }

    /// Structs: the same rows of each child.
    pub(super) fn each(
        &mut self,
        parent: &StructArray,
        ranges: &Ranges,
    ) -> Result<ArrayRef, ArrowError> {
        let children = parent.columns().iter();
        let children: Result<Vec<_>, _> =
            children.map(|child| self.gathered(child, ranges)).collect();
        let nulls = validity(parent.nulls(), ranges);
        let fields = parent.fields().clone();
        let parent = StructArray::try_new_with_length(fields, children?, nulls, count(ranges))?;
        Ok(Arc::new(parent))
    }

    /// Dictionaries: the keys named, over the dictionary's values rebuilt once.
    fn keyed(&mut self, array: &ArrayRef, ranges: &Ranges) -> Result<ArrayRef, ArrowError> {
        let keyed = array.as_any_dictionary();
        let values = self.values(keyed.values())?;
        // The keys are a column that nests nothing; the values stay those of every piece.
        let keys = make_array(keyed.keys().to_data());
        let keys = leaf(&keys, ranges)?.to_data();
        let rebuilt = keys
            .into_builder()
            .data_type(array.data_type().clone())
            .child_data(vec![values.to_data()]);
        Ok(make_array(rebuilt.build()?))
    }
}
