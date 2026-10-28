//! The metadata columns holding one value a session: built once, and sliced for each batch.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::types::Int8Type;
use arrow_array::{ArrayRef, DictionaryArray, Int8Array, Int64Array, TimestampMicrosecondArray};
use arrow_schema::ArrowError;
use rdlt_connector::Epoch;

use super::{Stamp, lower_array};
use crate::table::TableView;
use crate::table::lower::{LOAD_EPOCH_TYPE, loaded_at_type};

/// The session's epoch and the load start as dictionary arrays of one value, for up to `rows`
/// rows.
#[derive(Debug)]
pub(super) struct Constants {
    pub(super) epoch: Epoch,
    pub(super) loaded_at: SystemTime,
    /// The load start in microseconds since the epoch.
    pub(super) micros: i64,
    pub(super) rows: usize,
    pub(super) columns: [ArrayRef; 2],
}

impl Constants {
    /// The constant metadata columns of `view` for `rows` rows of the session `stamp` names.
    pub(super) fn new(view: &TableView, stamp: &Stamp, rows: usize) -> Result<Self, ArrowError> {
        let lowered = |index: usize| view.physical[view.model.columns.len() + index].logical_type();
        let epoch = i64::try_from(stamp.epoch.0).map_err(|_| {
            ArrowError::InvalidArgumentError(format!(
                "epoch {} is past what an Int64 column holds",
                stamp.epoch
            ))
        })?;
        let load_epoch: ArrayRef = Arc::new(Int64Array::from(vec![epoch]));
        let micros = micros_of(stamp.loaded_at)?;
        let loaded_at: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC"));
        let constant = |value: ArrayRef| -> Result<ArrayRef, ArrowError> {
            let keys = Int8Array::from(vec![0; rows]);
            Ok(Arc::new(DictionaryArray::<Int8Type>::try_new(keys, value)?))
        };
        Ok(Self {
            epoch: stamp.epoch,
            loaded_at: stamp.loaded_at,
            micros,
            rows,
            columns: [
                constant(lower_array(&load_epoch, &LOAD_EPOCH_TYPE, lowered(0))?)?,
                constant(lower_array(&loaded_at, &loaded_at_type(), lowered(1))?)?,
            ],
        })
    }

    /// Whether the columns are of the session `stamp` names and reach `rows` rows.
    pub(super) fn fits(&self, stamp: &Stamp, rows: usize) -> bool {
        self.epoch == stamp.epoch && self.loaded_at == stamp.loaded_at && self.rows >= rows
    }
}

/// `time` in microseconds since the epoch: before it negative, and a time between two
/// microseconds the earlier.
///
/// # Errors
///
/// A time beyond what an `i64` of microseconds holds, which no row can be stamped with.
pub(super) fn micros_of(time: SystemTime) -> Result<i64, ArrowError> {
    let beyond = || {
        ArrowError::ComputeError(format!(
            "the clock reads {time:?}, beyond what microseconds since the epoch hold"
        ))
    };
    match time.duration_since(UNIX_EPOCH) {
        Ok(since) => i64::try_from(since.as_micros()).map_err(|_| beyond()),
        Err(before) => {
            let before = before.duration();
            let part = u128::from(!before.subsec_nanos().is_multiple_of(1_000));
            let micros = i64::try_from(before.as_micros() + part).map_err(|_| beyond())?;
            Ok(-micros)
        }
    }
}
