//! `_native.Transform`: a Python callable as the engine's `Transform`, run on the compute pool,
//! the callable on a thread with only Python beneath it and the GIL taken only inside the call.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch};
use arrow_data::ArrayData;
use arrow_pyarrow::FromPyArrow as _;
use arrow_schema::{DataType, Schema, SchemaRef};
use pyo3::types::{PyAnyMethods as _, PyTuple};
use pyo3::{Bound, IntoPyObject as _, Py, PyAny, PyErr, Python, intern, pyclass, pymethods};
use rdlt::TransformError;

use crate::arrow::{NativeBatch, imported, schema_from_pyarrow};
use crate::calls;
use crate::errors::Failure;

/// A Python transform: its callable, whether it filters, and its declared output.
#[derive(Clone)]
pub(crate) struct PythonTransform {
    name: Arc<str>,
    function: Arc<Py<PyAny>>,
    filters: bool,
    output: Option<SchemaRef>,
}

fn failed(py: Python<'_>, error: &PyErr) -> TransformError {
    let failure = Failure::of(py, error);
    TransformError::new(format!("{}: {}", failure.type_name, failure.message))
        .with_cause(failure.traceback)
}

impl PythonTransform {
    /// What `read` makes of the callable's answer for `batch`: the batch reaches the callable as
    /// pyarrow's, and its answer comes back exported, both on the daemon thread.
    fn called<T: Send + 'static>(
        &self,
        batch: &RecordBatch,
        read: impl for<'py> FnOnce(&Bound<'py, PyAny>) -> Result<T, TransformError> + Send + 'static,
    ) -> Result<T, TransformError> {
        let (function, batch) = (Arc::clone(&self.function), batch.clone());
        calls::blocking(
            move |py| {
                let batch = Py::new(py, NativeBatch::new(batch))?;
                let arguments = (function.clone_ref(py), batch).into_pyobject(py)?;
                let transformed = calls::reading(py)?.getattr(intern!(py, "transformed"))?;
                Ok((transformed, arguments.cast_into::<PyTuple>()?))
            },
            move |py, answered| match answered {
                Ok(exported) => read(&exported),
                Err(error) => Err(failed(py, &error)),
            },
        )
        .map_err(|unanswered| {
            TransformError::new(format!(
                "a call into Python ended without its answer: {unanswered}"
            ))
        })?
    }
}

impl rdlt::Transform for PythonTransform {
    fn name(&self) -> &str {
        &self.name
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        if self.filters {
            return Ok(Arc::new(input.clone()));
        }
        if let Some(output) = &self.output {
            return Ok(Arc::clone(output));
        }
        let empty = RecordBatch::new_empty(Arc::new(input.clone()));
        Ok(self.apply(empty)?.schema())
    }

    fn filters(&self) -> bool {
        self.filters
    }

    fn keep(&self, batch: &RecordBatch) -> Result<BooleanArray, TransformError> {
        self.called(batch, |exported| {
            let data = imported(exported, ArrayData::from_pyarrow_bound)
                .map_err(|error| failed(exported.py(), &error))?;
            if data.data_type() != &DataType::Boolean {
                return Err(TransformError::new(format!(
                    "a filter answered a mask of {}, not of booleans",
                    data.data_type()
                )));
            }
            Ok(BooleanArray::from(data))
        })
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        if self.filters {
            return Ok(batch);
        }
        self.called(&batch, |exported| {
            imported(exported, RecordBatch::from_pyarrow_bound)
                .map_err(|error| failed(exported.py(), &error))
        })
    }
}

/// A transform written in Python, as the host registers it.
#[pyclass(module = "rdlt._native", name = "Transform", frozen)]
pub(crate) struct NativeTransform {
    transform: PythonTransform,
}

#[pymethods]
impl NativeTransform {
    #[new]
    pub(crate) fn new(
        py: Python<'_>,
        name: String,
        function: Py<PyAny>,
        filters: bool,
        output: Option<Py<PyAny>>,
    ) -> Result<Self, PyErr> {
        let output = output
            .map(|schema| schema_from_pyarrow(schema.bind(py)).map(Arc::new))
            .transpose()?;
        Ok(Self {
            transform: PythonTransform {
                name: Arc::from(name),
                function: Arc::new(function),
                filters,
                output,
            },
        })
    }

    /// The name errors and the document use.
    #[getter]
    fn name(&self) -> String {
        self.transform.name.to_string()
    }
}

impl NativeTransform {
    /// The transform, for the host.
    pub(crate) fn transform(&self) -> PythonTransform {
        self.transform.clone()
    }
}
