//! The metadata columns holding one value a load: built once, and sliced for each batch.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::builder::FixedSizeBinaryBuilder;
use arrow_array::types::Int8Type;
use arrow_array::{ArrayRef, DictionaryArray, Int8Array, TimestampMicrosecondArray};
use rdlt_connector::LoadId;

use super::{Stamp, lower_array};
use crate::table::TableView;
use crate::table::lower::{LOAD_ID_TYPE, loaded_at_type};

/// The load id and load start as dictionary arrays of one value, for up to `rows` rows.
#[derive(Debug)]
pub(super) struct Constants {
    pub(super) load_id: LoadId,
    pub(super) loaded_at: SystemTime,
    pub(super) rows: usize,
    pub(super) columns: [ArrayRef; 2],
}

impl Constants {
    /// The constant metadata columns of `view` for `rows` rows of the load `stamp` names.
    pub(super) fn new(
        view: &TableView,
        stamp: &Stamp,
        rows: usize,
    ) -> Result<Self, arrow_schema::ArrowError> {
        let lowered = |index: usize| view.physical[view.model.columns.len() + index].logical_type();
        let mut load_ids = FixedSizeBinaryBuilder::with_capacity(1, 16);
        load_ids.append_value(stamp.load_id.as_bytes())?;
        let load_id: ArrayRef = Arc::new(load_ids.finish());
        let micros = stamp
            .loaded_at
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| {
                i64::try_from(since.as_micros()).unwrap_or(i64::MAX)
            });
        let loaded_at: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC"));
        let constant = |value: ArrayRef| -> Result<ArrayRef, arrow_schema::ArrowError> {
            let keys = Int8Array::from(vec![0; rows]);
            Ok(Arc::new(DictionaryArray::<Int8Type>::try_new(keys, value)?))
        };
        Ok(Self {
            load_id: stamp.load_id,
            loaded_at: stamp.loaded_at,
            rows,
            columns: [
                constant(lower_array(&load_id, &LOAD_ID_TYPE, lowered(0))?)?,
                constant(lower_array(&loaded_at, &loaded_at_type(), lowered(1))?)?,
            ],
        })
    }
}
