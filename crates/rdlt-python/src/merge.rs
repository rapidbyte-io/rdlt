//! `rdlt.connector.merge`'s native half: `rdlt_connector::merge` over Arrow Python hands over.
//!
//! Python exports each argument in its own frames and hands the capsules over, so no method of
//! the argument itself runs beneath Rust, though a Python-backed stream's reader does, for its
//! schema, its next batch and its release. What the capsules hold is moved out of them with no
//! copy. Each merge runs with the GIL released, its inputs let go of only once it is attached
//! again, and its rows are handed back as batches Python reads through the C Data Interface,
//! sharing their buffers.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::types::{PyModule, PyModuleMethods as _};
use pyo3::{Bound, PyAny, PyErr, Python, create_exception, pyfunction};
use rdlt_connector::merge as kit;
use rdlt_connector::{
    ChangeColumns, ConnectorError, ConnectorErrorKind, MergeKey, RootKey, TableRef, TableSchema,
};
use serde::de::DeserializeOwned;

use crate::arrow::{Handed, NativeBatch, NativeSchema, batch, batches, schema};

/// Adds the kit's classes, its refusal and its functions to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> Result<(), PyErr> {
    module.add_class::<NativeBatch>()?;
    module.add_class::<NativeSchema>()?;
    module.add("MergeError", module.py().get_type::<MergeError>())?;
    for function in [
        pyo3::wrap_pyfunction!(merge_sparse, module)?,
        pyo3::wrap_pyfunction!(merge_children_sparse, module)?,
        pyo3::wrap_pyfunction!(read_back, module)?,
        pyo3::wrap_pyfunction!(written_schema, module)?,
        pyo3::wrap_pyfunction!(admitted, module)?,
        pyo3::wrap_pyfunction!(holds, module)?,
        pyo3::wrap_pyfunction!(tombstone_schema, module)?,
        pyo3::wrap_pyfunction!(folded, module)?,
        pyo3::wrap_pyfunction!(refuse_history_generation, module)?,
        pyo3::wrap_pyfunction!(to_arrow, module)?,
    ] {
        module.add_function(function)?;
    }
    Ok(())
}

create_exception!(
    _native,
    MergeError,
    PyException,
    "A connector error the merge kit met: its kind, its code and its message."
);

/// `error`, met by the function `doing`, as the connector error `failed` makes of it.
fn failed(doing: &str, error: ArrowError) -> PyErr {
    raised(&kit::failed(doing, error))
}

/// `error` as `MergeError`, whose arguments are its kind as a failure class of `rdlt.connector`
/// names it, its code and its message: the kit meets `Data` and `Internal` alone.
fn raised(error: &ConnectorError) -> PyErr {
    let kind = match error.kind() {
        ConnectorErrorKind::Data => "data",
        _ => "internal",
    };
    let code = error.code().map(str::to_owned);
    MergeError::new_err((kind, code, error.to_string()))
}

/// `text`, a `what` as the JSON a destination was told it in.
fn read<T: DeserializeOwned>(what: &str, text: &str) -> Result<T, PyErr> {
    serde_json::from_str(text)
        .map_err(|error| PyValueError::new_err(format!("no {what} rdlt can read: {error}")))
}

/// `batches` as Python reads them.
fn answered(batches: Vec<RecordBatch>) -> Vec<NativeBatch> {
    batches.into_iter().map(NativeBatch::new).collect()
}

/// The rows of a table that published `published`, its tombstones `buried`, once `incoming`
/// merges in by `key`, and its tombstones then: `rdlt_connector::merge::merge_sparse`.
///
/// # Errors
///
/// `MergeError` where the kit refuses the rows, else what reading the arguments raises.
#[pyfunction]
pub(crate) fn merge_sparse(
    py: Python<'_>,
    schema_of: &Bound<'_, PyAny>,
    published: Vec<Handed<'_>>,
    buried: Vec<Handed<'_>>,
    incoming: Vec<Handed<'_>>,
    key: &str,
) -> Result<(Vec<NativeBatch>, Vec<NativeBatch>), PyErr> {
    let schema = schema(schema_of)?;
    let key: MergeKey = read("merge key", key)?;
    let published = batches(py, published)?;
    let buried = batches(py, buried)?;
    let incoming = batches(py, incoming)?;
    let merged = py.detach(|| kit::merge_sparse(&schema, &published, &buried, &incoming, &key));
    let merged = merged.map_err(|error| failed("merge_sparse", error))?;
    Ok((answered(merged.rows), answered(merged.tombstones)))
}

/// A child table's rows once the roots `roots` publish replace their children:
/// `rdlt_connector::merge::merge_children_sparse`.
///
/// # Errors
///
/// `MergeError` where the kit refuses the rows, else what reading the arguments raises.
#[pyfunction]
pub(crate) fn merge_children_sparse(
    py: Python<'_>,
    schema_of: &Bound<'_, PyAny>,
    published: Vec<Handed<'_>>,
    incoming: Vec<Handed<'_>>,
    key: &str,
    root: &str,
    roots: Vec<Handed<'_>>,
) -> Result<Vec<NativeBatch>, PyErr> {
    let schema = schema(schema_of)?;
    let key: MergeKey = read("merge key", key)?;
    let root: RootKey = read("root key", root)?;
    let published = batches(py, published)?;
    let incoming = batches(py, incoming)?;
    let roots = batches(py, roots)?;
    let merged = py
        .detach(|| kit::merge_children_sparse(&schema, &published, &incoming, &key, &root, &roots));
    let rows = merged.map_err(|error| failed("merge_children_sparse", error))?;
    Ok(answered(rows))
}

/// `rows` as a reader is given them, every column of the schema in each batch:
/// `rdlt_connector::merge::read_back`.
///
/// # Errors
///
/// `MergeError` where a value no longer converts to its column's type.
#[pyfunction]
pub(crate) fn read_back(
    py: Python<'_>,
    schema_of: &Bound<'_, PyAny>,
    rows: Vec<Handed<'_>>,
) -> Result<Vec<NativeBatch>, PyErr> {
    let schema = schema(schema_of)?;
    let rows = batches(py, rows)?;
    let read = py.detach(|| kit::read_back(&schema, &rows));
    Ok(answered(read.map_err(|error| failed("read_back", error))?))
}

/// The schema a change stream's written batches have: `rdlt_connector::merge::written_schema`.
///
/// # Errors
///
/// What reading the arguments raises.
#[pyfunction]
pub(crate) fn written_schema(
    stored: &Bound<'_, PyAny>,
    changes: &str,
) -> Result<NativeSchema, PyErr> {
    let changes: ChangeColumns = read("change columns", changes)?;
    let written = kit::written_schema(&schema(stored)?, &changes);
    Ok(NativeSchema::new(written))
}

/// Refuses the batch Python `exported`, written for a table merged by `key` whose stored columns
/// are `stored` where they are known: `rdlt_connector::merge::admitted`.
///
/// # Errors
///
/// `MergeError` where the kit refuses the batch, else what reading the arguments raises.
#[pyfunction]
pub(crate) fn admitted<'py>(
    py: Python<'py>,
    exported: (Bound<'py, PyAny>, Bound<'py, PyAny>),
    stored: Option<&Bound<'py, PyAny>>,
    key: &str,
) -> Result<(), PyErr> {
    let key: MergeKey = read("merge key", key)?;
    let stored = stored.map(schema).transpose()?;
    let (schema_of, array) = exported;
    let written = batch(py, schema_of, array)?;
    kit::admitted(&written, stored.as_ref(), &key).map_err(|error| failed("admitted", error))
}

/// Refuses `held` unless each column `schema_of` names converts to its type there with every
/// value kept: `rdlt_connector::merge::holds`.
///
/// # Errors
///
/// `MergeError` where a value does not fit, else what reading the arguments raises.
#[pyfunction]
pub(crate) fn holds(
    py: Python<'_>,
    held: Vec<Handed<'_>>,
    schema_of: &Bound<'_, PyAny>,
) -> Result<(), PyErr> {
    let schema = schema(schema_of)?;
    let held = batches(py, held)?;
    py.detach(|| kit::holds(&held, &schema))
        .map_err(|error| failed("holds", error))
}

/// The schema of a table's tombstones: `rdlt_connector::merge::tombstone_schema`.
///
/// # Errors
///
/// `MergeError` where the table lacks a key column, else what reading the arguments raises.
#[pyfunction]
pub(crate) fn tombstone_schema(
    schema_of: &Bound<'_, PyAny>,
    key: &str,
) -> Result<NativeSchema, PyErr> {
    let key: MergeKey = read("merge key", key)?;
    let buried = kit::tombstone_schema(&schema(schema_of)?, &key)
        .map_err(|error| failed("tombstone_schema", error))?;
    Ok(NativeSchema::new(buried))
}

/// `held` as at most `shapes` batches, the smallest joined in groups of at most `cells` cells
/// without a value: `rdlt_connector::merge::folded`.
///
/// # Errors
///
/// `ValueError` where `shapes` is 0, `MergeError` where a join fails, else what reading the
/// arguments raises.
#[pyfunction]
pub(crate) fn folded(
    py: Python<'_>,
    schema_of: &Bound<'_, PyAny>,
    held: Vec<Handed<'_>>,
    shapes: usize,
    cells: u64,
) -> Result<Vec<NativeBatch>, PyErr> {
    let shapes = NonZeroUsize::new(shapes).ok_or_else(|| {
        PyValueError::new_err("shapes is the most batches to give back: at least 1")
    })?;
    let schema = schema(schema_of)?;
    let held = batches(py, held)?;
    // The detached fold takes clones, so the batches are let go of attached, as this returns.
    let folded = py.detach(|| kit::folded(&schema, held.clone(), shapes, cells));
    Ok(answered(folded.map_err(|error| failed("folded", error))?))
}

/// Refuses a table writer of `table` where it is a replace generation of a history table:
/// `rdlt_connector::merge::refuse_history_generation`.
///
/// # Errors
///
/// `MergeError` where it is one, else what reading `table` raises.
#[pyfunction]
pub(crate) fn refuse_history_generation(table: &str) -> Result<(), PyErr> {
    let table: TableRef = read("table", table)?;
    kit::refuse_history_generation(&table).map_err(|error| raised(&error))
}

/// The Arrow schema of `table`, a table schema's JSON: `TableSchema::to_arrow`.
///
/// # Errors
///
/// `ValueError` where `table` is no table schema rdlt can read.
#[pyfunction]
pub(crate) fn to_arrow(table: &str) -> Result<NativeSchema, PyErr> {
    let table: TableSchema = read("table schema", table)?;
    Ok(NativeSchema::new(Arc::new(table.to_arrow())))
}
