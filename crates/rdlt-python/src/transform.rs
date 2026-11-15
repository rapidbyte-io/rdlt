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

/// A Python transform: its callable, whether it filters, its declared output, and the output
/// columns it says it passes on, each with the input column it passes.
#[derive(Clone)]
pub(crate) struct PythonTransform {
    name: Arc<str>,
    function: Arc<Py<PyAny>>,
    filters: bool,
    output: Option<SchemaRef>,
    passes: Option<Arc<[(String, String)]>>,
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
        let output = match &self.output {
            Some(output) => Arc::clone(output),
            None => self
                .apply(RecordBatch::new_empty(Arc::new(input.clone())))?
                .schema(),
        };
        for (passed, arrived) in self.passes.iter().flat_map(|passes| passes.iter()) {
            let fits = match (
                output.field_with_name(passed),
                input.field_with_name(arrived),
            ) {
                (Ok(passed), Ok(arrived)) => {
                    passed.data_type() == arrived.data_type()
                        && (passed.is_nullable() || !arrived.is_nullable())
                }
                _ => false,
            };
            if !fits {
                let message = format!(
                    "it says it passes {arrived} on as {passed}, which its input and output do not \
                     hold alike"
                );
                return Err(TransformError::new(message).with_column(passed.clone()));
            }
        }
        Ok(output)
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

    fn passes(&self, input: &Schema) -> Option<Vec<Option<usize>>> {
        if self.filters {
            return Some((0..input.fields().len()).map(Some).collect());
        }
        let said = self.passes.as_ref()?;
        let output = self.output(input).ok()?;
        let places = output.fields().iter().map(|field| {
            let (_, arrived) = said.iter().find(|(passed, _)| passed == field.name())?;
            input.index_of(arrived).ok()
        });
        Some(places.collect())
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
    #[pyo3(signature = (name, function, filters, output, passes = None))]
    pub(crate) fn new(
        py: Python<'_>,
        name: String,
        function: Py<PyAny>,
        filters: bool,
        output: Option<Py<PyAny>>,
        passes: Option<Vec<(String, String)>>,
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
                passes: passes.map(Arc::from),
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
