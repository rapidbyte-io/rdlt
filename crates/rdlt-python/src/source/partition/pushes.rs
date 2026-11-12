//! What one item a read yields is as pushes, made one at a time, so a read that has no room for
//! the next keeps its place: an Arrow stream its reader, rows their iterator and the push being
//! filled.

use arrow_array::ffi_stream::ArrowArrayStreamReader;
use pyo3::{Bound, PyAny, Python};
use rdlt_connector::ConnectorError;

use crate::arrow::{Produced, produced};
use crate::errors::Failure;
use crate::json::{self, JsonLimits};
use crate::source::context::NativeCheckpoint;
use crate::source::partition::Pushed;

/// An item's pushes, those not yet made.
pub(crate) enum Pushes {
    /// A push the item is whole: a checkpoint, a batch or a notice; none once made.
    One(Option<Pushed>),
    /// An Arrow stream, read a batch at a time.
    Stream(ArrowArrayStreamReader),
    /// Rows, written as JSON a push at a time.
    Rows(json::Rows),
}

impl Pushes {
    /// What `item` pushes.
    ///
    /// # Errors
    ///
    /// The failure the read ends with where `item` cannot be pushed: an Arrow producer that
    /// raised, or what is neither Arrow nor rows.
    pub(crate) fn of(
        py: Python<'_>,
        item: &Bound<'_, PyAny>,
        limits: JsonLimits,
    ) -> Result<Self, ConnectorError> {
        if let Ok(checkpoint) = item.cast::<NativeCheckpoint>() {
            let cursor = checkpoint.get().cursor().to_owned();
            return Ok(Self::One(Some(Pushed::Checkpoint(cursor))));
        }
        match produced(item) {
            Ok(Some(Produced::Batch(batch))) => Ok(Self::One(Some(Pushed::Batch(batch)))),
            Ok(Some(Produced::Stream(reader))) => Ok(Self::Stream(reader)),
            Ok(None) => json::Rows::new(py, item, limits)
                .map(Self::Rows)
                .map_err(|unwritten| unwritten.connector_error()),
            Err(error) => Err(Failure::of(py, &error).connector_error()),
        }
    }

    /// The next push, none once every one was made.
    ///
    /// A stream reads its next batch with the GIL released; one that needs Python takes it.
    ///
    /// # Errors
    ///
    /// The failure the read ends with: a stream that yielded an error, or a row that cannot be
    /// written.
    pub(crate) fn next(&mut self, py: Python<'_>) -> Result<Option<Pushed>, ConnectorError> {
        match self {
            Self::One(push) => Ok(push.take()),
            Self::Stream(reader) => match py.detach(|| reader.next()) {
                None => Ok(None),
                Some(Ok(batch)) => Ok(Some(Pushed::Batch(batch))),
                Some(Err(error)) => Err(ConnectorError::data(format!(
                    "an Arrow stream yielded an error: {error}"
                ))
                .with_code("arrow_stream")),
            },
            Self::Rows(rows) => rows
                .next(py)
                .map(|json| json.map(Pushed::Json))
                .map_err(|unwritten| unwritten.connector_error()),
        }
    }
}
