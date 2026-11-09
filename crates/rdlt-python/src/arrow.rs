//! Arrow across the boundary without a copy, through the C Data Interface: what an object with
//! `__arrow_c_array__` or `__arrow_c_stream__` holds.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use arrow_array::ffi_stream::ArrowArrayStreamReader;
use arrow_pyarrow::FromPyArrow as _;
use pyo3::intern;
use pyo3::types::PyAnyMethods as _;
use pyo3::{Bound, PyAny, PyErr};

/// What a Python object holds of Arrow.
pub(crate) enum Produced {
    /// One batch: pyarrow's `RecordBatch`, or anything with `__arrow_c_array__`.
    Batch(RecordBatch),
    /// A stream of batches, read one at a time: a pyarrow `Table`, a polars `DataFrame`, a `DuckDB`
    /// relation, or anything with `__arrow_c_stream__`.
    Stream(ArrowArrayStreamReader),
}

/// What `object` holds of Arrow; none where it is no Arrow producer.
///
/// # Errors
///
/// What the producer raises, or `TypeError` for a batch whose struct array holds nulls.
pub(crate) fn produced(object: &Bound<'_, PyAny>) -> Result<Option<Produced>, PyErr> {
    let py = object.py();
    if object.hasattr(intern!(py, "__arrow_c_array__"))? {
        return RecordBatch::from_pyarrow_bound(object).map(|batch| Some(Produced::Batch(batch)));
    }
    if object.hasattr(intern!(py, "__arrow_c_stream__"))? {
        return ArrowArrayStreamReader::from_pyarrow_bound(object)
            .map(|reader| Some(Produced::Stream(reader)));
    }
    Ok(None)
}
