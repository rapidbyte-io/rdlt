//! What a Python read is given and yields beside its data: its context, and its checkpoints.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::{Bound, PyErr, Python, pyclass, pymethods};
use rdlt_connector::{Emitter, Headroom, Partition};
use serde_json::value::RawValue;

use crate::source::PyCursor;
use crate::source::partition::{self, Outlet, Pushed};

/// Whether the engine waits for a checkpoint, as the read last saw it.
#[derive(Clone, Debug, Default)]
pub(crate) struct Due(Arc<AtomicBool>);

impl Due {
    /// Says whether a checkpoint is due now.
    pub(crate) fn set(&self, due: bool) {
        self.0.store(due, Ordering::Relaxed);
    }
}

/// What a read is told beside its partition, as its emitter says it.
#[derive(Clone, Debug, Default)]
pub(crate) struct Told {
    /// Whether the engine asks the read to follow an unbounded partition once caught up.
    pub(crate) follows: bool,
    /// Whether the read was given a cursor to start from.
    pub(crate) resumes: bool,
    /// The top-level columns the stream excludes, in order.
    pub(crate) excluded: Vec<String>,
    /// The limits the engine holds the read's pushes to, each by its field's name in the Rust
    /// SDK's `Limits`.
    pub(crate) limits: Vec<(&'static str, u64)>,
}

impl Told {
    /// What `out` tells its read.
    pub(crate) fn of(out: &Emitter<PyCursor>) -> Self {
        // Taken field by field, since this crate may not name `rdlt-wire`'s type.
        let limits = out.limits();
        Self {
            follows: out.follows(),
            resumes: out.resumes(),
            excluded: out.excluded().columns().map(str::to_owned).collect(),
            limits: vec![
                ("frame_bytes", limits.frame_bytes),
                ("batch_rows", limits.batch_rows),
                ("schema_columns", limits.schema_columns),
                ("nesting_depth", limits.nesting_depth),
                ("json_push_bytes", limits.json_push_bytes),
                ("cursor_bytes", limits.cursor_bytes),
                ("config_bytes", limits.config_bytes),
                ("control_string_bytes", limits.control_string_bytes),
                ("batch_values", limits.batch_values),
                ("schema_bytes", limits.schema_bytes),
                ("dictionary_bytes", limits.dictionary_bytes),
                ("catalog_bytes", limits.catalog_bytes),
                ("state_bytes", limits.state_bytes),
                ("control_message_bytes", limits.control_message_bytes),
            ],
        }
    }
}

/// What one partition's read is told: its partition, what its emitter says, and whether a
/// checkpoint is due or the read has gone.
#[pyclass(module = "rdlt._native", name = "StreamContext", frozen)]
pub(crate) struct NativeStreamContext {
    partition: String,
    unbounded: bool,
    told: Told,
    due: Due,
    /// Where the read sends, which says whether it has gone.
    outlet: Outlet,
}

impl NativeStreamContext {
    /// A read's context, and the flag its read sets after each push.
    pub(crate) fn new(partition: &Partition, told: Told) -> (Self, Due) {
        let due = Due::default();
        let context = Self {
            partition: partition.id().to_string(),
            unbounded: partition.is_unbounded(),
            told,
            due: due.clone(),
            outlet: Outlet::default(),
        };
        (context, due)
    }

    /// Where the read sends, which its thread opens.
    pub(crate) fn outlet(&self) -> &Outlet {
        &self.outlet
    }
}

#[pymethods]
impl NativeStreamContext {
    /// Whether the engine is waiting for a checkpoint at the next safe point.
    fn checkpoint_due(&self) -> bool {
        self.due.0.load(Ordering::Relaxed)
    }

    /// The id of the partition read, `whole` for a stream that is not split.
    #[getter]
    fn partition(&self) -> String {
        self.partition.clone()
    }

    /// Whether the partition read never ends.
    #[getter]
    fn unbounded(&self) -> bool {
        self.unbounded
    }

    /// Whether the engine asks the read to follow an unbounded partition once caught up.
    #[getter]
    fn follows(&self) -> bool {
        self.told.follows
    }

    /// Whether the read was given a cursor to start from.
    #[getter]
    fn resumes(&self) -> bool {
        self.told.resumes
    }

    /// The top-level columns the stream excludes, in order.
    #[getter]
    fn excluded(&self) -> Vec<String> {
        self.told.excluded.clone()
    }

    /// The limits the engine holds the read's pushes to, each by its field's name.
    #[getter]
    fn limits(&self) -> Vec<(&'static str, u64)> {
        self.told.limits.clone()
    }

    /// Whether the engine asked the read to stop, or stopped listening.
    fn stopped(&self) -> bool {
        self.outlet.gone()
    }

    /// Gives the engine `notice` after what the read yielded before, waiting for the engine with
    /// the GIL released; nothing once the read has gone, which its next yield then finds.
    fn notify(&self, py: Python<'_>, notice: &Bound<'_, NativeNotice>) {
        if let Some(sent) = self.outlet.sender() {
            partition::send(py, &sent, notice.get().pushed());
        }
    }
}

/// What a read tells the engine beside what it yields, made where the read says it, so a value
/// the engine cannot take fails the read there: its partitions changed, or how far behind it is.
#[pyclass(module = "rdlt._native", name = "Notice", frozen)]
pub(crate) struct NativeNotice {
    /// How far behind the read is, none for a change of its partitions.
    behind: Option<(u64, Headroom)>,
}

#[pymethods]
impl NativeNotice {
    /// That the stream's partitions changed.
    #[staticmethod]
    fn replan() -> Self {
        Self { behind: None }
    }

    /// That the read is `records` behind its source's newest, and that the source keeps `bytes`
    /// more, or for `time` more, before it drops where the read stands.
    #[staticmethod]
    fn behind(records: u64, bytes: Option<u64>, time: Option<Duration>) -> Self {
        Self {
            behind: Some((records, Headroom { bytes, time })),
        }
    }
}

impl NativeNotice {
    /// What it is pushed as.
    pub(crate) fn pushed(&self) -> Pushed {
        match self.behind {
            None => Pushed::Replan,
            Some((records, headroom)) => Pushed::Behind { records, headroom },
        }
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
