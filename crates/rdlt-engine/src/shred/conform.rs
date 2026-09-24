//! Columns a chunk built against its own shape, fitted to the joined shape without parsing the
//! chunk again.
//!
//! A chunk's own shape differs from the joined one whenever it lacks a column other chunks have,
//! holds only nulls in one, meets the columns in another order, or holds integers other chunks
//! widen to floats or decimals. None of those needs the values again: the columns are reordered
//! by name, the missing and null ones become nulls of the joined type, and integers are cast.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, ListArray, StructArray, new_null_array};
use arrow_schema::Fields;
use rdlt_connector::Field;

use super::ShredError;
use super::observe::{Observed, Shape};

/// Whether columns built as `local` fit `joined` without their values.
pub(crate) fn fits(local: &Observed, joined: &Observed) -> bool {
    match (local, joined) {
        (Observed::Null, _) | (Observed::Int { .. }, Observed::Float | Observed::Wide) => true,
        (Observed::Object(local), Observed::Object(joined)) => shape_fits(local, joined),
        (Observed::Array(local), Observed::Array(joined)) => fits(local, joined),
        (local, joined) => local.logical_type() == joined.logical_type(),
    }
}

/// Whether objects built as `local` fit `joined` without their values.
pub(crate) fn shape_fits(local: &Shape, joined: &Shape) -> bool {
    local.fields().iter().all(|(name, observed)| {
        joined
            .get(name)
            .is_some_and(|joined| fits(observed, joined))
    })
}

/// `columns`, built as `local`'s fields over `rows` rows, as `joined`'s fields, in its order.
pub(crate) fn columns(
    columns: &[ArrayRef],
    local: &Shape,
    joined: &Shape,
    rows: usize,
) -> Result<Vec<ArrayRef>, ShredError> {
    joined
        .fields()
        .iter()
        .map(|(name, observed)| match local.position(name) {
            Some(position) => fit(&columns[position], &local.fields()[position].1, observed),
            None => Ok(new_null_array(&observed.logical_type().to_arrow(), rows)),
        })
        .collect()
}

/// `array`, built as `local`, as `joined`.
fn fit(array: &ArrayRef, local: &Observed, joined: &Observed) -> Result<ArrayRef, ShredError> {
    let failed = |error: arrow_schema::ArrowError| {
        ShredError::Internal(format!("fitting a column to the joined shape: {error}"))
    };
    match (local, joined) {
        (Observed::Null, _) => Ok(new_null_array(
            &joined.logical_type().to_arrow(),
            array.len(),
        )),
        (Observed::Object(local), Observed::Object(joined)) => {
            let object = array.as_struct();
            let fields: Fields = joined
                .logical_fields()
                .iter()
                .map(Field::to_arrow)
                .collect();
            if fields.is_empty() {
                let empty = StructArray::new_empty_fields(object.len(), object.nulls().cloned());
                return Ok(Arc::new(empty));
            }
            let columns = columns(object.columns(), local, joined, object.len())?;
            StructArray::try_new(fields, columns, object.nulls().cloned())
                .map(|object| Arc::new(object) as ArrayRef)
                .map_err(failed)
        }
        (Observed::Array(local), Observed::Array(joined)) => {
            let list = array.as_list::<i32>();
            let values = fit(list.values(), local, joined)?;
            let item = Arc::new(Field::new("item", joined.logical_type(), true).to_arrow());
            ListArray::try_new(item, list.offsets().clone(), values, list.nulls().cloned())
                .map(|list| Arc::new(list) as ArrayRef)
                .map_err(failed)
        }
        (Observed::Int { .. }, Observed::Float | Observed::Wide) => {
            arrow_cast::cast(array, &joined.logical_type().to_arrow()).map_err(failed)
        }
        _ => Ok(Arc::clone(array)),
    }
}
