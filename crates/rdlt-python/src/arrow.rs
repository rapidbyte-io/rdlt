//! Arrow across the boundary without a copy, through the C Data Interface: what an object with
//! `__arrow_c_array__` or `__arrow_c_stream__` holds.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::ffi::{FFI_ArrowSchema, to_ffi};
use arrow_array::ffi_stream::ArrowArrayStreamReader;
use arrow_array::{Array as _, RecordBatch, StructArray};
use arrow_pyarrow::FromPyArrow as _;
use arrow_schema::{ArrowError, Schema, SchemaRef};
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyAnyMethods as _, PyCapsule, PyTuple};
use pyo3::{Bound, FromPyObject, Py, PyAny, PyErr, Python, intern, pyclass, pyfunction, pymethods};
use rdlt_connector::TableSchema;

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

/// A batch of Rust's that Python reads through the C Data Interface, sharing its buffers: what
/// `pyarrow.record_batch` imports from Python's own frames.
#[pyclass(module = "rdlt._native", name = "Batch", frozen)]
pub(crate) struct NativeBatch(RecordBatch);

impl NativeBatch {
    /// `batch`, as Python will read it.
    pub(crate) fn new(batch: RecordBatch) -> Self {
        Self(batch)
    }
}

#[pymethods]
impl NativeBatch {
    /// The batch as the capsules the C Data Interface's producers answer: its schema and its
    /// columns, a struct array.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyAny>>,
    ) -> Result<(Bound<'py, PyCapsule>, Bound<'py, PyCapsule>), PyErr> {
        let _ = requested_schema;
        let columns = StructArray::from(self.0.clone());
        let failed = |error: ArrowError| PyValueError::new_err(error.to_string());
        let (array, schema) = to_ffi(&columns.to_data()).map_err(failed)?;
        let schema = schema
            .with_metadata(self.0.schema_ref().metadata())
            .map_err(failed)?;
        Ok((
            PyCapsule::new_with_value(py, schema, c"arrow_schema")?,
            PyCapsule::new_with_value(py, array, c"arrow_array")?,
        ))
    }
}

/// What Python exported of an answer, as a C Data Interface producer: arrow-pyarrow reads its
/// capsules with no method of the answer's, and none of pyarrow's, run beneath Rust.
#[pyclass(module = "rdlt._native", name = "Exported", frozen)]
struct Exported(Py<PyAny>);

#[pymethods]
impl Exported {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyAny>>,
    ) -> Bound<'py, PyAny> {
        let _ = requested_schema;
        self.0.bind(py).clone()
    }
}

/// What `read`, an arrow-pyarrow conversion, makes of `exported`, the list of `schema` and
/// `array` capsules Python exported an answer as.
///
/// The capsules are taken out of the list, so what releases them runs here, before the answer
/// is given: a daemon thread still holding them could release them after its process exited.
///
/// # Errors
///
/// What `read` raises of the capsules.
pub(crate) fn imported<'py, T>(
    exported: &Bound<'py, PyAny>,
    read: impl FnOnce(&Bound<'py, PyAny>) -> Result<T, PyErr>,
) -> Result<T, PyErr> {
    let py = exported.py();
    let array = exported.call_method0(intern!(py, "pop"))?;
    let schema = exported.call_method0(intern!(py, "pop"))?;
    let capsules = PyTuple::new(py, [schema, array])?;
    let producer = Bound::new(py, Exported(capsules.into_any().unbind()))?;
    read(producer.as_any())
}

/// A schema's capsule, as a C Data Interface producer that answers it: arrow-pyarrow reads it with
/// no method of the schema's, and none of pyarrow's, run beneath Rust.
#[pyclass(module = "rdlt._native", name = "ExportedSchema", frozen)]
struct ExportedSchema(Py<PyAny>);

#[pymethods]
impl ExportedSchema {
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.0.bind(py).clone()
    }
}

/// The engine's table schema, as JSON, of the Arrow schema in `capsule`, which a schema's
/// `__arrow_c_schema__` answered in Python.
///
/// # Errors
///
/// `ValueError` where the schema has no table schema equivalent, as Rust's `from_arrow` says, or
/// what arrow-pyarrow raises of a capsule that holds no schema.
#[pyfunction]
pub(crate) fn table_schema(capsule: &Bound<'_, PyAny>) -> Result<String, PyErr> {
    let table = TableSchema::from_arrow(schema(capsule)?.as_ref())
        .map_err(|refused| PyValueError::new_err(refused.to_string()))?;
    Ok(serde_json::to_string(&table).expect("a table schema serializes"))
}

/// The Arrow schema in `capsule`, which a schema's `__arrow_c_schema__` answered in Python.
///
/// # Errors
///
/// What arrow-pyarrow raises of a capsule that holds no schema.
pub(crate) fn schema(capsule: &Bound<'_, PyAny>) -> Result<SchemaRef, PyErr> {
    let producer = Bound::new(capsule.py(), ExportedSchema(capsule.clone().unbind()))?;
    Ok(Arc::new(Schema::from_pyarrow_bound(producer.as_any())?))
}

/// The schema a `pyarrow.Schema` describes.
///
/// # Errors
///
/// `TypeError` where `object` is no schema Arrow can read.
pub(crate) fn schema_from_pyarrow(object: &Bound<'_, PyAny>) -> Result<Schema, PyErr> {
    Schema::from_pyarrow_bound(object)
}

/// Arrow Python exported in its own frames and handed over: a batch's schema and array capsules,
/// or a stream's capsule, as `__arrow_c_array__` and `__arrow_c_stream__` answer them.
#[derive(FromPyObject)]
pub(crate) enum Handed<'py> {
    /// One batch.
    Batch(Bound<'py, PyAny>, Bound<'py, PyAny>),
    /// A stream of batches of one schema.
    Stream(Bound<'py, PyAny>),
}

/// The batch whose capsules Python exported as `schema` and `array`.
///
/// # Errors
///
/// What arrow-pyarrow raises of the capsules.
pub(crate) fn batch<'py>(
    py: Python<'py>,
    schema: Bound<'py, PyAny>,
    array: Bound<'py, PyAny>,
) -> Result<RecordBatch, PyErr> {
    let capsules = PyTuple::new(py, [schema, array])?;
    let producer = Bound::new(py, Exported(capsules.into_any().unbind()))?;
    RecordBatch::from_pyarrow_bound(producer.as_any())
}

/// What Python exported of a stream, as a C Data Interface producer.
#[pyclass(module = "rdlt._native", name = "ExportedStream", frozen)]
struct ExportedStream(Py<PyAny>);

#[pymethods]
impl ExportedStream {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyAny>>,
    ) -> Bound<'py, PyAny> {
        let _ = requested_schema;
        self.0.bind(py).clone()
    }
}

/// The batches `handed` holds, in order, each stream's read a batch at a time with the GIL
/// held, since a Python-backed reader runs Python for its next batch.
///
/// # Errors
///
/// What arrow-pyarrow raises of a capsule, or `ValueError` where a stream fails.
pub(crate) fn batches(py: Python<'_>, handed: Vec<Handed<'_>>) -> Result<Vec<RecordBatch>, PyErr> {
    let mut batches = Vec::with_capacity(handed.len());
    for item in handed {
        match item {
            Handed::Batch(schema, array) => batches.push(batch(py, schema, array)?),
            Handed::Stream(capsule) => {
                let producer = Bound::new(py, ExportedStream(capsule.unbind()))?;
                let reader = ArrowArrayStreamReader::from_pyarrow_bound(producer.as_any())?;
                for batch in reader {
                    let batch = batch.map_err(|error| {
                        PyValueError::new_err(format!("a stream of batches failed: {error}"))
                    })?;
                    batches.push(batch);
                }
            }
        }
    }
    Ok(batches)
}

/// A schema of Rust's that Python reads through the C Data Interface.
#[pyclass(module = "rdlt._native", name = "Schema", frozen)]
pub(crate) struct NativeSchema(SchemaRef);

impl NativeSchema {
    /// `schema`, as Python will read it.
    pub(crate) fn new(schema: SchemaRef) -> Self {
        Self(schema)
    }
}

#[pymethods]
impl NativeSchema {
    /// The schema as the capsule the C Data Interface's producers answer.
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyCapsule>, PyErr> {
        let exported = FFI_ArrowSchema::try_from(self.0.as_ref())
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        PyCapsule::new_with_value(py, exported, c"arrow_schema")
    }
}
