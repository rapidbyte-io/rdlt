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
    SourceFactory, StreamName, StreamSpec, StreamState, Streams, acknowledging_source_factory_from,
    source_factory_from,
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
pub(crate) async fn called<T: Send + 'static>(
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
#[expect(
    clippy::struct_excessive_bools,
    reason = "the JSON a stream says itself in, each flag a property of its own"
)]
struct Declared {
    name: String,
    primary_key: Option<Vec<String>>,
    read_modes: Vec<ReadMode>,
    checkpointing: Checkpointing,
    partitioned: bool,
    replayable: bool,
    /// Whether the stream is told which of its cursors are committed.
    committed: bool,
    /// Whether the stream tells where it stands outside the engine.
    acknowledged: bool,
    /// The format of the stream's cursors.
    cursor_version: u16,
}

/// One stream of a Python source.
#[derive(Clone)]
pub(crate) struct PythonStream {
    spec: StreamSpec,
    stream: Arc<Py<PyAny>>,
    partitioned: bool,
    committed: bool,
    acknowledged: bool,
    cursor_version: u16,
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
            .with_checkpointing(declared.checkpointing)
            .with_replayable(declared.replayable);
        if let Some(key) = &declared.primary_key {
            spec = spec.with_primary_key(key.iter().map(String::as_str));
        }
        Ok(Self {
            spec,
            stream: Arc::new(stream),
            partitioned: declared.partitioned,
            committed: declared.committed,
            acknowledged: declared.acknowledged,
            cursor_version: declared.cursor_version,
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

impl PythonSource {
    /// The format of `stream`'s cursors, as the stream says it.
    fn cursor_version(&self, stream: &StreamName) -> Option<u16> {
        self.streams
            .iter()
            .find(|held| held.spec.name() == stream)
            .map(|held| held.cursor_version)
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
        if !self.partitioned {
            return Ok(vec![Partition::single()]);
        }
        let stream = Arc::clone(&self.stream);
        let keys: Vec<(String, bool)> = called(
            "partitions",
            move |py| (stream.clone_ref(py),).into_pyobject(py),
            |answered| answered.extract(),
        )
        .await?;
        keys.iter()
            .map(|(key, unbounded)| {
                let id = PartitionId::parse(key).config(format!("partition {key:?}"))?;
                let partition = Partition::new(id);
                Ok(if *unbounded {
                    partition.unbounded()
                } else {
                    partition
                })
            })
            .collect()
    }

    async fn committed(
        &self,
        _source: &PythonSource,
        cursors: &[(PartitionId, PyCursor)],
    ) -> Result<()> {
        if !self.committed {
            return Ok(());
        }
        let told: Vec<(String, Option<String>)> = cursors
            .iter()
            .map(|(id, cursor)| {
                let cursor = cursor.0.as_ref().map(|text| text.get().to_owned());
                (id.to_string(), cursor)
            })
            .collect();
        let stream = Arc::clone(&self.stream);
        called(
            "committed",
            move |py| (stream.clone_ref(py), told).into_pyobject(py),
            |_| Ok(()),
        )
        .await
    }

    async fn acknowledged(
        &self,
        _source: &PythonSource,
        partition: &PartitionId,
    ) -> Result<Option<PyCursor>> {
        if !self.acknowledged {
            return Ok(None);
        }
        let stream = Arc::clone(&self.stream);
        let partition = partition.to_string();
        let told: Option<String> = called(
            "acknowledged",
            move |py| (stream.clone_ref(py), partition).into_pyobject(py),
            |answered| answered.extract(),
        )
        .await?;
        let Some(told) = told else {
            return Ok(None);
        };
        let cursor = RawValue::from_string(told).map_err(|error| {
            ConnectorError::internal(format!(
                "stream {}: where it stands is not JSON: {error}",
                self.spec.name()
            ))
        })?;
        Ok(Some(PyCursor(Some(cursor))))
    }

    async fn read(
        &self,
        _source: &PythonSource,
        partition: &Partition,
        cursor: PyCursor,
        out: &mut Emitter<PyCursor>,
    ) -> Result<()> {
        let key = partition.id().to_string();
        let cursor = cursor.0.map(|cursor| cursor.get().to_owned());
        let limits = out.limits();
        let limits = JsonLimits::new(limits.json_push_bytes, limits.nesting_depth);
        let (context, due) = NativeStreamContext::new(partition, out.follows(), out.resumes());
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
                None => return Ok(()),
                Some(Pushed::Json(json)) => out.json(json).await?,
                Some(Pushed::Batch(batch)) => out.batch(batch).await?,
                Some(Pushed::Checkpoint(cursor)) => {
                    out.checkpoint(&PyCursor(Some(cursor))).await?;
                }
                Some(Pushed::Failed(error)) => return Err(error),
            }
            due.set(out.checkpoint_due());
        }
    }
}

/// Connects the source `connect` makes from the configuration's JSON, for the host `host` names.
async fn connected(
    connect: Arc<Py<PyAny>>,
    config: serde_json::Value,
    host: Option<String>,
) -> Result<PythonSource> {
    let (connected, declared) = called(
        "connected",
        move |py| (connect.clone_ref(py), config.to_string(), host).into_pyobject(py),
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

/// A source written in Python: its spec, whether it tells where it stands outside the engine, and
/// the callable that connects it.
#[pyclass(module = "rdlt._native", name = "SourceFactory", frozen)]
pub(crate) struct NativeSourceFactory {
    spec: ConnectorSpec,
    acknowledges: bool,
    connect: Arc<Py<PyAny>>,
}

#[pymethods]
impl NativeSourceFactory {
    #[new]
    pub(crate) fn new(
        id: &str,
        version: String,
        config_schema: &str,
        acknowledges: bool,
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
            acknowledges,
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
    /// What connects the source: the callable, on a daemon thread, with the configuration's JSON
    /// and the host its connect context names.
    fn connecting(
        &self,
    ) -> impl Fn(serde_json::Value, ConnectContext) -> BoxFuture<'static, Result<PythonSource>>
    + Send
    + Sync
    + 'static {
        let connect = Arc::clone(&self.connect);
        move |config, context| -> BoxFuture<'static, Result<PythonSource>> {
            let host = context.host().map(str::to_owned);
            Box::pin(connected(Arc::clone(&connect), config, host))
        }
    }

    /// The Rust SDK's factory for the source, as a pipeline and served mode place it: every
    /// barrier, checkpoint, frame, limit and credit the SDK's own.
    pub(crate) fn factory(&self) -> Box<dyn SourceFactory> {
        source_factory_from(
            self.spec.clone(),
            self.connecting(),
            PythonSource::cursor_version,
        )
    }

    /// The factory that also tells where the source stands outside the engine, where its class
    /// says it does, for certification alone.
    pub(crate) fn acknowledging(&self) -> Box<dyn SourceFactory> {
        if !self.acknowledges {
            return self.factory();
        }
        acknowledging_source_factory_from(
            self.spec.clone(),
            self.connecting(),
            PythonSource::cursor_version,
        )
    }
}
