//! One partition of a Python source, read on a thread of its own.
//!
//! The thread is a Python daemon thread running `rdlt._reading`, so a generator runs with only
//! Python beneath it: an interpreter that exits ends the thread in Python's own frames. The
//! Rust side, [`PartitionOut`], holds the GIL only while Python runs and while it turns what
//! Python yielded into pushes; it releases it while it waits for the engine or reads a stream.

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::RecordBatch;
use bytes::Bytes;
use parking_lot::Mutex;
use pyo3::types::PyAnyMethods as _;
use pyo3::{Bound, Py, PyAny, PyErr, Python, intern, pyclass, pymethods};
use rdlt_connector::ConnectorError;
use serde_json::value::RawValue;
use tokio::sync::mpsc;

use crate::arrow::{Produced, produced};
use crate::calls;
use crate::errors::Failure;
use crate::json::{self, JsonLimits};
use crate::source::context::{NativeCheckpoint, NativeStreamContext};

/// What a partition's thread sends its read.
#[derive(Debug)]
pub(crate) enum Pushed {
    /// JSON lines, within one push's bytes.
    Json(Bytes),
    /// An Arrow batch.
    Batch(RecordBatch),
    /// A checkpoint's cursor, as its JSON text.
    Checkpoint(Box<RawValue>),
    /// The read failed: its generator raised, or yielded what cannot be pushed.
    Failed(ConnectorError),
}

/// A partition's thread, and what it sends.
pub(crate) struct Reading {
    received: mpsc::Receiver<Pushed>,
    /// Whether the thread said how the read ended.
    told: Arc<AtomicBool>,
}

impl Reading {
    /// Starts reading, on a thread named `name`, what `stream.read(partition, cursor, context)`
    /// returns: an iterator or an async iterator.
    ///
    /// Nothing here takes the GIL on the caller's thread.
    pub(crate) fn start(
        name: &str,
        stream: Arc<Py<PyAny>>,
        partition: Option<String>,
        cursor: Option<String>,
        context: NativeStreamContext,
        limits: JsonLimits,
    ) -> std::io::Result<Self> {
        // A yield waits for the engine: the channel holds one push while another is sent.
        let (sent, received) = mpsc::channel(1);
        let told = Arc::new(AtomicBool::new(false));
        let out = PartitionOut {
            sent: Mutex::new(Some(sent.clone())),
            limits,
            told: Arc::clone(&told),
        };
        let name = name.to_owned();
        std::thread::Builder::new()
            .name("rdlt-read-start".to_owned())
            .spawn(move || {
                let told_of_start = Arc::clone(&out.told);
                let started = Python::try_attach(|py| {
                    let context = Py::new(py, context)?;
                    let out = Py::new(py, out)?;
                    calls::reading(py)?.call_method1(
                        intern!(py, "start"),
                        (name, stream.as_ref(), partition, cursor, context, out),
                    )?;
                    Ok::<(), PyErr>(())
                });
                let failure = match started {
                    Some(Ok(())) => return,
                    Some(Err(error)) => {
                        Python::attach(|py| Failure::of(py, &error).connector_error())
                    }
                    None => ConnectorError::internal("the interpreter is exiting"),
                };
                told_of_start.store(true, Ordering::Release);
                sent.blocking_send(Pushed::Failed(failure)).ok();
            })?;
        Ok(Self { received, told })
    }

    /// The next push, none once the read ended.
    ///
    /// A thread that ended without saying how fails the read: a thread that died must not read
    /// as a partition read to its end.
    pub(crate) async fn next(&mut self) -> Option<Pushed> {
        let pushed = self.received.recv().await;
        if pushed.is_none() && !self.told.swap(true, Ordering::AcqRel) {
            return Some(Pushed::Failed(ConnectorError::internal(
                "a partition's thread ended without saying how its read ended",
            )));
        }
        pushed
    }
}

/// Sends `pushed`, the GIL released while it waits; false once the read has gone.
fn send(py: Python<'_>, sent: &mpsc::Sender<Pushed>, pushed: Pushed) -> bool {
    py.detach(|| sent.blocking_send(pushed).is_ok())
}

/// What a partition's thread pushes to: its read's channel, and the limits a read keeps within.
#[pyclass(module = "rdlt._native", name = "PartitionOut", frozen)]
pub(crate) struct PartitionOut {
    /// None once the read has said how it ended, which closes the channel at once: Python's
    /// collector may keep this object, and with it the channel, alive long after.
    sent: Mutex<Option<mpsc::Sender<Pushed>>>,
    limits: JsonLimits,
    told: Arc<AtomicBool>,
}

#[pymethods]
impl PartitionOut {
    /// Pushes what the read yielded, in order; false once the read has gone or the item failed
    /// it, the read then being told so.
    fn push(&self, py: Python<'_>, item: &Bound<'_, PyAny>) -> bool {
        // Cloned out of the lock, which is never held while the push waits for the engine.
        let Some(sent) = self.sent.lock().clone() else {
            return false;
        };
        pushed(py, item, self.limits, &sent)
    }

    /// Whether a push would be answered without waiting for the engine, as one is once the read
    /// has gone.
    fn has_room(&self) -> bool {
        self.sent
            .lock()
            .as_ref()
            .is_none_or(|sent| sent.is_closed() || sent.capacity() > 0)
    }

    /// Says the read yielded everything, and closes its channel.
    fn finished(&self) {
        self.told.store(true, Ordering::Release);
        self.sent.lock().take();
    }

    /// Says the read ended with `error`, and closes its channel.
    fn failed(&self, py: Python<'_>, error: &Bound<'_, PyAny>) {
        self.told.store(true, Ordering::Release);
        let Some(sent) = self.sent.lock().take() else {
            return;
        };
        let failure = Failure::of(py, &PyErr::from_value(error.clone()));
        send(py, &sent, Pushed::Failed(failure.connector_error()));
    }
}

/// Sends what `item` is as pushes; false once the read has gone or the item failed it.
fn pushed(
    py: Python<'_>,
    item: &Bound<'_, PyAny>,
    limits: JsonLimits,
    sent: &mpsc::Sender<Pushed>,
) -> bool {
    if let Ok(checkpoint) = item.cast::<NativeCheckpoint>() {
        return send(
            py,
            sent,
            Pushed::Checkpoint(checkpoint.get().cursor().to_owned()),
        );
    }
    match produced(item) {
        Ok(Some(Produced::Batch(batch))) => return send(py, sent, Pushed::Batch(batch)),
        Ok(Some(Produced::Stream(mut reader))) => {
            loop {
                // A stream reads its next batch with the GIL released; one that needs Python
                // takes it.
                let next = py.detach(|| reader.next());
                let pushed = match next {
                    None => return true,
                    Some(Ok(batch)) => Pushed::Batch(batch),
                    Some(Err(error)) => {
                        let error = ConnectorError::data(format!(
                            "an Arrow stream yielded an error: {error}"
                        ))
                        .with_code("arrow_stream");
                        send(py, sent, Pushed::Failed(error));
                        return false;
                    }
                };
                if !send(py, sent, pushed) {
                    return false;
                }
            }
        }
        Ok(None) => {}
        Err(error) => {
            send(
                py,
                sent,
                Pushed::Failed(Failure::of(py, &error).connector_error()),
            );
            return false;
        }
    }
    match json::each_push(py, item, limits, |json| send(py, sent, Pushed::Json(json))) {
        Ok(delivered) => delivered,
        Err(unwritten) => {
            send(py, sent, Pushed::Failed(unwritten.connector_error()));
            false
        }
    }
}
