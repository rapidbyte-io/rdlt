//! Destinations written in Python: a Python object's sessions and table writers as the Rust
//! SDK's, each call on a daemon thread of Python's own.
//!
//! The Python side is `rdlt.connector._destination`'s wrappers, which take and answer the SDK's
//! types as their JSON, a state record's value as its JSON text, and batches as `pyarrow`'s.

#[cfg(test)]
mod tests;
mod turns;

use std::sync::Arc;

use arrow_array::RecordBatch;
use bytes::Bytes;
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyAnyMethods as _, PyTuple};
use pyo3::{Bound, IntoPyObject as _, Py, PyAny, PyErr, Python, pyclass, pyfunction, pymethods};
use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, ConnectContext, ConnectorError, ConnectorId,
    ConnectorSpec, DestinationConnector, DestinationFactory, Epoch, Field, LogicalType,
    OpenContext, Opened, PublishedRows, ReadBack, Receipt, Result, SegmentId, Session, StateChange,
    StateRecord, TableChange, TableRef, TableSchema, TableWriter, WriteStats,
    destination_factory_from, readable_destination_factory_from,
};
use serde::{Deserialize, Serialize};

use crate::arrow::{NativeBatch, batch_of, imported};
use crate::source::called;
use turns::Turns;

/// The method `name` of `object`, a wrapper `rdlt.connector._destination` made or the function
/// that connects one, called through `rdlt._reading` on a daemon thread with the arguments
/// `arguments` builds there; `extract` reads its answer.
///
/// `turn` is held until the call has returned, or until the call is let go unmade: a turn
/// [`Turns`] gave, or `()`.
async fn method<T: Send + 'static>(
    object: &Arc<Py<PyAny>>,
    name: &'static str,
    turn: impl Send + 'static,
    arguments: impl for<'py> FnOnce(Python<'py>) -> std::result::Result<Bound<'py, PyTuple>, PyErr>
    + Send
    + 'static,
    extract: impl for<'py> FnOnce(Bound<'py, PyAny>) -> std::result::Result<T, PyErr> + Send + 'static,
) -> Result<T> {
    let object = Arc::clone(object);
    called(
        "method",
        move |py| (object.clone_ref(py), name, arguments(py)?).into_pyobject(py),
        move |answered| {
            let read = extract(answered);
            drop(turn);
            read
        },
    )
    .await
}

fn json<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("the SDK's types serialize")
}

fn read<T: for<'de> Deserialize<'de>>(what: &str, text: &str) -> Result<T> {
    serde_json::from_str(text).map_err(|error| {
        ConnectorError::internal(format!(
            "a Python destination answered {what} rdlt cannot read: {error}"
        ))
    })
}

/// Closes `session`, whose open answered what rdlt cannot read, so it lets go of what it holds,
/// and returns why: it matters more than any error from closing.
async fn close_after(session: PythonSession, error: ConnectorError) -> ConnectorError {
    drop(session.close().await);
    error
}

/// A change to a table, as a Python destination is told it.
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Change<'a> {
    Create {
        table: &'a TableRef,
        schema: &'a TableSchema,
    },
    AddColumn {
        table: &'a TableRef,
        field: &'a Field,
    },
    Widen {
        table: &'a TableRef,
        column: &'a str,
        from: &'a LogicalType,
        to: &'a LogicalType,
    },
}

fn change(change: &TableChange) -> String {
    json(&match change {
        TableChange::Create { table, schema } => Change::Create { table, schema },
        TableChange::AddColumn { table, field } => Change::AddColumn { table, field },
        TableChange::Widen {
            table,
            column,
            from,
            to,
        } => Change::Widen {
            table,
            column,
            from,
            to,
        },
    })
}

/// A state record as a Python destination keeps it: its value as the text it is, so the record
/// reads back byte for byte.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Kept {
    key: String,
    value: String,
}

impl Kept {
    /// `record`, refused as invalid data where its value is not UTF-8 text, which Python's
    /// string cannot hold.
    fn of(record: &StateRecord) -> Result<Self> {
        let value = std::str::from_utf8(&record.value).map_err(|_| {
            ConnectorError::data(format!(
                "state record {:?}: its value is not UTF-8 text",
                record.key
            ))
        })?;
        Ok(Self {
            key: record.key.clone(),
            value: value.to_owned(),
        })
    }

    fn record(self) -> StateRecord {
        StateRecord {
            key: self.key,
            value: Bytes::from(self.value),
        }
    }
}

/// A state change as a Python destination is told it.
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum KeptChange<'a> {
    Put(Kept),
    Delete(&'a str),
}

/// `meta` as a Python destination is told it, each state record's value as its text.
fn commit(meta: &CommitMeta) -> Result<String> {
    let rest = CommitMeta {
        state_delta: Vec::new(),
        ..meta.clone()
    };
    let mut told = serde_json::to_value(&rest).expect("a commit's fields serialize");
    let delta = meta
        .state_delta
        .iter()
        .map(|change| match change {
            StateChange::Put(record) => Kept::of(record).map(KeptChange::Put),
            StateChange::Delete(key) => Ok(KeptChange::Delete(key)),
        })
        .collect::<Result<Vec<_>>>()?;
    told["state_delta"] = serde_json::to_value(delta).expect("state changes serialize");
    Ok(told.to_string())
}

/// What a Python destination's `capabilities()` answered as JSON, read as the Rust SDK's.
///
/// Capabilities rdlt cannot read, or that name a key it does not know, are refused as an
/// internal error: the destination is built wrong.
fn capabilities(answered: &str) -> Result<Capabilities> {
    let not_rdlt_s = |error: serde_json::Error| {
        ConnectorError::internal(format!("the capabilities are not rdlt's: {error}"))
    };
    let given: serde_json::Value = serde_json::from_str(answered).map_err(not_rdlt_s)?;
    let capabilities: Capabilities = serde_json::from_value(given.clone()).map_err(not_rdlt_s)?;
    let known = serde_json::to_value(&capabilities).expect("capabilities serialize");
    if let Some(key) = unknown(&given, &known) {
        return Err(ConnectorError::internal(format!(
            "the capabilities name {key}, which rdlt does not know"
        )));
    }
    Ok(capabilities)
}

/// The first key `given` holds, at any depth, that `known` does not: a capability rdlt does not
/// know, as its dotted path.
fn unknown(given: &serde_json::Value, known: &serde_json::Value) -> Option<String> {
    let (serde_json::Value::Object(given), serde_json::Value::Object(known)) = (given, known)
    else {
        return None;
    };
    given.iter().find_map(|(key, value)| match known.get(key) {
        None => Some(key.clone()),
        Some(held) => unknown(value, held).map(|below| format!("{key}.{below}")),
    })
}

/// What a Python destination's `open` answers beside its session.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenedJson {
    epoch: Epoch,
    landed: Option<Receipt>,
    state: Vec<Kept>,
}

/// A connected Python destination, and what its instance says it can store.
pub(crate) struct PythonDestination {
    connected: Arc<Py<PyAny>>,
    capabilities: Capabilities,
}

impl DestinationConnector for PythonDestination {
    // `destination_factory_from` reads neither: each Python destination's spec names it.
    const ID: &'static str = "io.rapidbyte.python";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    type Config = serde_json::Value;
    type Session = PythonSession;

    fn capabilities(&self) -> Capabilities {
        self.capabilities.clone()
    }

    async fn connect(_config: serde_json::Value, _context: &ConnectContext) -> Result<Self> {
        Err(ConnectorError::internal(
            "a Python destination connects through its factory",
        ))
    }

    async fn check(&self) -> Result<()> {
        method(
            &self.connected,
            "check",
            (),
            |py| Ok(PyTuple::empty(py)),
            |_| Ok(()),
        )
        .await
    }

    async fn open(&self, context: &OpenContext) -> Result<Opened<PythonSession>> {
        let pipeline = context.pipeline.to_string();
        let (session, opened): (Py<PyAny>, String) = method(
            &self.connected,
            "open",
            (),
            move |py| (pipeline,).into_pyobject(py),
            |answered| answered.extract(),
        )
        .await?;
        let at_once = self.capabilities.max_parallel_writers.get() > 1;
        let session = PythonSession {
            session: Arc::new(session),
            turns: Turns::new(),
            at_once,
        };
        let opened: OpenedJson = match read("an opened session", &opened) {
            Ok(opened) => opened,
            Err(error) => return Err(close_after(session, error).await),
        };
        Ok(Opened {
            session,
            epoch: opened.epoch,
            landed: opened.landed,
            state: opened.state.into_iter().map(Kept::record).collect(),
        })
    }
}

impl ReadBack for PythonDestination {
    async fn published(&self, table: &TableRef, rows: PublishedRows) -> Result<()> {
        let table = json(table);
        let batches: Vec<RecordBatch> = method(
            &self.connected,
            "published",
            (),
            move |py| (table,).into_pyobject(py),
            |answered| {
                answered
                    .try_iter()?
                    .map(|exported| imported(&exported?, batch_of))
                    .collect()
            },
        )
        .await?;
        for batch in batches {
            rows.send(batch).await?;
        }
        Ok(())
    }
}

/// One load's session of a Python destination, whose calls and its table writers' take their
/// turns as [`turns`] says.
pub(crate) struct PythonSession {
    session: Arc<Py<PyAny>>,
    turns: Turns,
    /// Whether its table writers run at once, beside each other and the session.
    at_once: bool,
}

impl Session for PythonSession {
    type Writer = PythonWriter;

    async fn apply_schema(&mut self, table_change: &TableChange) -> Result<()> {
        let told_change = change(table_change);
        let turn = self.turns.call().await;
        method(
            &self.session,
            "apply_schema",
            turn,
            move |py| (told_change,).into_pyobject(py),
            |_| Ok(()),
        )
        .await
    }

    async fn writer(&mut self, table: &TableRef) -> Result<PythonWriter> {
        let table = json(table);
        let turn = self.turns.call().await;
        let writer = method(
            &self.session,
            "writer",
            turn,
            move |py| (table,).into_pyobject(py),
            |answered| Ok(answered.unbind()),
        )
        .await?;
        Ok(PythonWriter {
            writer: Arc::new(writer),
            turns: self.turns.writer(self.at_once),
        })
    }

    async fn discard_staged(&mut self) -> Result<()> {
        let turn = self.turns.call().await;
        method(
            &self.session,
            "discard_staged",
            turn,
            |py| Ok(PyTuple::empty(py)),
            |_| Ok(()),
        )
        .await
    }

    async fn commit(&mut self, meta: &CommitMeta) -> Result<Receipt> {
        let meta = commit(meta)?;
        let turn = self.turns.whole().await;
        let receipt: String = method(
            &self.session,
            "commit",
            turn,
            move |py| (meta,).into_pyobject(py),
            |answered| answered.extract(),
        )
        .await?;
        read("a receipt", &receipt)
    }

    async fn close(self) -> Result<()> {
        let turn = self.turns.whole().await;
        method(
            &self.session,
            "close",
            turn,
            |py| Ok(PyTuple::empty(py)),
            |_| Ok(()),
        )
        .await
    }
}

/// A Python table writer, whose calls take their turns as [`turns`] says.
pub(crate) struct PythonWriter {
    writer: Arc<Py<PyAny>>,
    turns: Turns,
}

impl TableWriter for PythonWriter {
    async fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> Result<()> {
        let turn = self.turns.call().await;
        method(
            &self.writer,
            "write",
            turn,
            move |py| (segment.0, Py::new(py, NativeBatch::new(batch))?).into_pyobject(py),
            |_| Ok(()),
        )
        .await
    }

    async fn flush(&mut self) -> Result<WriteStats> {
        let turn = self.turns.call().await;
        let flushed = method(
            &self.writer,
            "flush",
            turn,
            |py| Ok(PyTuple::empty(py)),
            |answered| answered.extract(),
        );
        let (rows, bytes) = flushed.await?;
        Ok(WriteStats { rows, bytes })
    }
}

/// A destination written in Python: its spec, whether it reads back what it published, and the
/// callable that connects it.
#[pyclass(module = "rdlt._native", name = "DestinationFactory", frozen)]
pub(crate) struct NativeDestinationFactory {
    spec: ConnectorSpec,
    reads_back: bool,
    connect: Arc<Py<PyAny>>,
}

#[pymethods]
impl NativeDestinationFactory {
    #[new]
    pub(crate) fn new(
        id: &str,
        version: String,
        config_schema: &str,
        reads_back: bool,
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
            reads_back,
            connect: Arc::new(connect),
        })
    }

    /// The connector's id.
    #[getter]
    pub(crate) fn id(&self) -> String {
        self.spec.id.to_string()
    }
}

impl NativeDestinationFactory {
    /// What connects the destination: the callable, on a daemon thread, with the configuration's
    /// JSON and the host its connect context names, then the instance's `capabilities()`.
    fn connecting(
        &self,
    ) -> impl Fn(serde_json::Value, ConnectContext) -> BoxFuture<'static, Result<PythonDestination>>
    + Send
    + Sync
    + 'static {
        let connect = Arc::clone(&self.connect);
        move |config, context| {
            let connect = Arc::clone(&connect);
            let host = context.host().map(str::to_owned);
            Box::pin(async move {
                let config = config.to_string();
                let connected = method(
                    &connect,
                    "__call__",
                    (),
                    move |py| (config, host).into_pyobject(py),
                    |answered| Ok(answered.unbind()),
                )
                .await?;
                let connected = Arc::new(connected);
                let answered: String = method(
                    &connected,
                    "capabilities",
                    (),
                    |py| Ok(PyTuple::empty(py)),
                    |answered| answered.extract(),
                )
                .await?;
                Ok(PythonDestination {
                    connected,
                    capabilities: capabilities(&answered)?,
                })
            })
        }
    }

    /// The Rust SDK's factory for the destination, as a pipeline and served mode place it.
    pub(crate) fn factory(&self) -> Box<dyn DestinationFactory> {
        destination_factory_from(self.spec.clone(), self.connecting())
    }

    /// The factory that also reads back what the destination published, where it can, for
    /// certification alone.
    pub(crate) fn readable(&self) -> Box<dyn DestinationFactory> {
        if !self.reads_back {
            return self.factory();
        }
        readable_destination_factory_from(self.spec.clone(), self.connecting())
    }
}

/// The capabilities of an append-only destination for scalar types, as JSON: where an author's
/// declaration starts.
#[pyfunction]
pub(crate) fn minimal_capabilities() -> String {
    json(&Capabilities::minimal())
}

/// Why rdlt cannot read `answered`, capabilities as JSON; none where it can.
#[pyfunction]
pub(crate) fn unreadable_capabilities(answered: &str) -> Option<String> {
    capabilities(answered).err().map(|error| error.to_string())
}
