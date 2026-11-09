//! What a Python read is given and yields beside its data: its context, and its checkpoints.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pyo3::exceptions::PyValueError;
use pyo3::{PyErr, pyclass, pymethods};
use serde_json::value::RawValue;

/// Whether the engine waits for a checkpoint, as the read last saw it.
#[derive(Clone, Debug, Default)]
pub(crate) struct Due(Arc<AtomicBool>);

impl Due {
    /// Says whether a checkpoint is due now.
    pub(crate) fn set(&self, due: bool) {
        self.0.store(due, Ordering::Relaxed);
    }
}

/// What one partition's read is told: its partition, whether it follows and resumes, and
/// whether a checkpoint is due.
#[pyclass(module = "rdlt._native", name = "StreamContext", frozen)]
pub(crate) struct NativeStreamContext {
    partition: Option<String>,
    follows: bool,
    resumes: bool,
    due: Due,
}

impl NativeStreamContext {
    /// A read's context, and the flag its read sets after each push.
    pub(crate) fn new(partition: Option<String>, follows: bool, resumes: bool) -> (Self, Due) {
        let due = Due::default();
        let context = Self {
            partition,
            follows,
            resumes,
            due: due.clone(),
        };
        (context, due)
    }
}

#[pymethods]
impl NativeStreamContext {
    /// Whether the engine is waiting for a checkpoint at the next safe point.
    fn checkpoint_due(&self) -> bool {
        self.due.0.load(Ordering::Relaxed)
    }

    /// The partition read, where the stream has partitions.
    #[getter]
    fn partition(&self) -> Option<String> {
        self.partition.clone()
    }

    /// Whether the engine asks the read to follow an unbounded partition once caught up.
    #[getter]
    fn follows(&self) -> bool {
        self.follows
    }

    /// Whether the read was given a cursor to start from.
    #[getter]
    fn resumes(&self) -> bool {
        self.resumes
    }
}

/// A checkpoint a read yields: everything yielded before it is sealed, and a restart resumes
/// from its cursor, kept as the text it was given, so a number in it is not rounded.
#[pyclass(module = "rdlt._native", name = "Checkpoint", frozen)]
pub(crate) struct NativeCheckpoint {
    cursor: Box<RawValue>,
}

#[pymethods]
impl NativeCheckpoint {
    #[new]
    fn new(cursor: &str) -> Result<Self, PyErr> {
        let cursor = RawValue::from_string(cursor.to_owned()).map_err(|error| {
            PyValueError::new_err(format!("a checkpoint's cursor is not JSON: {error}"))
        })?;
        Ok(Self { cursor })
    }
}

impl NativeCheckpoint {
    /// The cursor, as its JSON text.
    pub(crate) fn cursor(&self) -> &RawValue {
        &self.cursor
    }
}
