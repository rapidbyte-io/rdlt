//! Sources written in Python, read in process: a Python object's streams as the Rust SDK's,
//! each partition on a thread of its own.

pub(crate) mod context;
pub(crate) mod partition;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::types::{PyAnyMethods as _, PyTuple};
use pyo3::{Bound, IntoPyObject as _, Py, PyAny, PyErr, Python, pyclass, pymethods};
use rdlt_connector::{
    BoxFuture, Checkpointing, ConnectContext, ConnectorError, ConnectorId, ConnectorSpec, Emitter,
    Partition, PartitionId, ReadMode, ReadStream, Result, ResultExt as _, SourceConnector,
    SourceFactory, StreamName, StreamSpec, StreamState, Streams, source_factory_from,
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::calls;
use crate::errors::Failure;
use crate::json::JsonLimits;
use context::NativeStreamContext;
use partition::{Pushed, Reading};

/// How often a read that is waiting on its generator looks whether a checkpoint came due.
const DUE_REFRESH: Duration = Duration::from_millis(50);

/// A Python stream's cursor, as its JSON text, kept as text so a number in it is not rounded:
/// none before its first checkpoint.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct PyCursor(Option<Box<RawValue>>);

/// Calls `function` of `rdlt._reading` with the arguments `arguments` builds, its exception the
/// connector's error and `extract` reading what it returned.
async fn called<T: Send + 'static>(
    function: &'static str,
    arguments: impl for<'py> FnOnce(Python<'py>) -> std::result::Result<Bound<'py, PyTuple>, PyErr>
    + Send
    + 'static,
    extract: impl for<'py> FnOnce(Bound<'py, PyAny>) -> std::result::Result<T, PyErr> + Send + 'static,
) -> Result<T> {
    let answered = calls::call(
        move |py| Ok((calls::reading(py)?.getattr(function)?, arguments(py)?)),
        move |py, returned| {
            returned
                .and_then(extract)
                .map_err(|error| Failure::of(py, &error).connector_error())
        },
    )
    .await;
    answered.map_err(|unanswered| {
        ConnectorError::internal(format!(
            "a call into Python ended without its answer: {unanswered}"
        ))
    })?
}

/// What a Python stream says of itself, as its `spec` answers it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Declared {
    name: String,
    primary_key: Option<Vec<String>>,
    read_modes: Vec<ReadMode>,
    checkpointing: Checkpointing,
    partitioned: bool,
    /// Whether the stream is read from its committed cursor on: a read that ends on rows after
    /// its last checkpoint must not read as a partition finished.
    #[serde(default)]
    resumes: bool,
}

/// One stream of a Python source.
#[derive(Clone)]
pub(crate) struct PythonStream {
    spec: StreamSpec,
    stream: Arc<Py<PyAny>>,
    partitioned: bool,
    resumes: bool,
}

impl PythonStream {
    fn declared(declared: &str, stream: Py<PyAny>) -> Result<Self> {
        let declared: Declared = serde_json::from_str(declared).map_err(|error| {
            ConnectorError::internal(format!(
                "a Python stream described itself in JSON rdlt cannot read: {error}"
            ))
        })?;
        let name =
            StreamName::new(&declared.name).config(format!("stream name {:?}", declared.name))?;
        let mut spec = StreamSpec::new(name)
            .with_read_modes(declared.read_modes)
            .with_checkpointing(declared.checkpointing);
        if let Some(key) = &declared.primary_key {
            spec = spec.with_primary_key(key.iter().map(String::as_str));
        }
        Ok(Self {
            spec,
            stream: Arc::new(stream),
            partitioned: declared.partitioned,
            resumes: declared.resumes,
        })
    }
}

/// A connected Python source.
pub(crate) struct PythonSource {
    connected: Arc<Py<PyAny>>,
    streams: Vec<PythonStream>,
}

impl SourceConnector for PythonSource {
    // `source_factory_from` reads neither: each Python source's spec names it.
    const ID: &'static str = "io.rapidbyte.python";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    type Config = serde_json::Value;

    async fn connect(_config: serde_json::Value, _context: &ConnectContext) -> Result<Self> {
        Err(ConnectorError::internal(
            "a Python source connects through its factory",
        ))
    }

    async fn check(&self) -> Result<()> {
        let connected = Arc::clone(&self.connected);
        called(
            "checked",
            move |py| (connected.clone_ref(py),).into_pyobject(py),
            |_| Ok(()),
        )
        .await
    }

    fn streams(&self) -> Streams<Self> {
        self.streams
            .iter()
            .cloned()
            .fold(Streams::new(), Streams::with)
    }
}

impl ReadStream<PythonSource> for PythonStream {
    type Cursor = PyCursor;

    fn spec(&self) -> StreamSpec {
        self.spec.clone()
    }

    async fn partitions(
        &self,
        _source: &PythonSource,
        _state: &StreamState,
    ) -> Result<Vec<Partition>> {
        // A resuming stream's partitions are never finished: rows a read leaves after its last
        // checkpoint are read again from it, not committed and recorded done.
        let held = |partition: Partition| {
            if self.resumes {
                partition.unbounded()
            } else {
                partition
            }
        };
        if !self.partitioned {
            return Ok(vec![held(Partition::single())]);
        }
        let stream = Arc::clone(&self.stream);
        let keys: Vec<String> = called(
            "partitions",
            move |py| (stream.clone_ref(py),).into_pyobject(py),
            |answered| answered.extract(),
        )
        .await?;
        keys.iter()
            .map(|key| {
                PartitionId::parse(key)
                    .map(|id| held(Partition::new(id)))
                    .config(format!("partition {key:?}"))
            })
            .collect()
    }

    async fn read(
        &self,
        _source: &PythonSource,
        partition: &Partition,
        cursor: PyCursor,
        out: &mut Emitter<PyCursor>,
    ) -> Result<()> {
        let key = self.partitioned.then(|| partition.id().to_string());
        let cursor = cursor.0.map(|cursor| cursor.get().to_owned());
        let mut unmarked = self.resumes && cursor.is_none();
        let mut pushed_rows = false;
        let limits = out.limits();
        let limits = JsonLimits::new(limits.json_push_bytes, limits.nesting_depth);
        let (context, due) = NativeStreamContext::new(key.clone(), out.follows(), out.resumes());
        due.set(out.checkpoint_due());
        let name = format!("rdlt-read-{}", self.spec.name());
        let mut reading = Reading::start(
            &name,
            Arc::clone(&self.stream),
            key,
            cursor,
            context,
            limits,
        )
        .map_err(|error| {
            ConnectorError::internal(format!("a partition's thread did not start: {error}"))
        })?;
        // A generator that yields nothing for a while still answers a barrier that arrives.
        let mut refresh = tokio::time::interval(DUE_REFRESH);
        loop {
            let pushed = tokio::select! {
                // A stop wins: the read ends, and its thread at its next push.
                biased;
                () = out.stopped() => return Ok(()),
                pushed = reading.next() => pushed,
                _ = refresh.tick() => {
                    due.set(out.checkpoint_due());
                    continue;
                }
            };
            match pushed {
                None if unmarked && pushed_rows => return Err(no_checkpoint(&self.spec)),
                None => return Ok(()),
                Some(Pushed::Json(json)) => {
                    pushed_rows = true;
                    out.json(json).await?;
                }
                Some(Pushed::Batch(batch)) => {
                    pushed_rows |= batch.num_rows() > 0;
                    out.batch(batch).await?;
                }
                Some(Pushed::Checkpoint(cursor)) => {
                    unmarked = false;
                    out.checkpoint(&PyCursor(Some(cursor))).await?;
                }
                Some(Pushed::Failed(error)) => return Err(error),
            }
            due.set(out.checkpoint_due());
        }
    }
}

/// Why a read of a resuming stream that pushed rows and never checkpointed fails: only rows
/// before a checkpoint commit, so its rows would be read again by every run and none land.
fn no_checkpoint(spec: &StreamSpec) -> ConnectorError {
    ConnectorError::config(format!(
        "stream {}: yielded rows but no checkpoint; rows commit at a checkpoint, so end the \
         generator with `yield rdlt.checkpoint(cursor)`",
        spec.name()
    ))
    .with_code("checkpoint_missing")
}

/// Connects the source `connect` makes from the configuration's JSON.
async fn connected(connect: Arc<Py<PyAny>>, config: serde_json::Value) -> Result<PythonSource> {
    let (connected, declared) = called(
        "connected",
        move |py| (connect.clone_ref(py), config.to_string()).into_pyobject(py),
        |answered| answered.extract::<(Py<PyAny>, Vec<(String, Py<PyAny>)>)>(),
    )
    .await?;
    let streams = declared
        .into_iter()
        .map(|(spec, stream)| PythonStream::declared(&spec, stream))
        .collect::<Result<Vec<_>>>()?;
    Ok(PythonSource {
        connected: Arc::new(connected),
        streams,
    })
}

/// A source written in Python: its spec, and the callable that connects it.
#[pyclass(module = "rdlt._native", name = "SourceFactory", frozen)]
pub(crate) struct NativeSourceFactory {
    spec: ConnectorSpec,
    connect: Arc<Py<PyAny>>,
}

#[pymethods]
impl NativeSourceFactory {
    #[new]
    pub(crate) fn new(
        id: &str,
        version: String,
        config_schema: &str,
        connect: Py<PyAny>,
    ) -> std::result::Result<Self, PyErr> {
        let id = ConnectorId::parse(id).map_err(|error| {
            PyValueError::new_err(format!("{id:?} is no connector id: {error}"))
        })?;
        let config_schema = serde_json::from_str(config_schema).map_err(|error| {
            PyValueError::new_err(format!("the configuration's schema is not JSON: {error}"))
        })?;
        Ok(Self {
            spec: ConnectorSpec {
                id,
                version,
                config_schema,
            },
            connect: Arc::new(connect),
        })
    }

    /// The connector's id.
    #[getter]
    fn id(&self) -> String {
        self.spec.id.to_string()
    }
}

impl NativeSourceFactory {
    /// The Rust SDK's factory for the source: every barrier, checkpoint, frame, limit and credit
    /// the SDK's own.
    pub(crate) fn factory(&self) -> Box<dyn SourceFactory> {
        let connect = Arc::clone(&self.connect);
        source_factory_from::<PythonSource, _>(
            self.spec.clone(),
            move |config, _context| -> BoxFuture<'static, Result<PythonSource>> {
                Box::pin(connected(Arc::clone(&connect), config))
            },
        )
    }
}
