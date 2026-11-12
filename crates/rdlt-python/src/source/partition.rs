//! One partition of a Python source, read as a call on one of the process's daemon threads.
//!
//! The daemon thread runs `rdlt._reading`, so a generator runs with only Python beneath it: an
//! interpreter that exits ends the thread in Python's own frames. The read holds its daemon
//! thread until it ends, which then waits for another call. The Rust side, [`PartitionOut`],
//! holds the GIL only while Python runs and while it turns what Python yielded into pushes; it
//! releases it while it reads a stream.
//!
//! A sync read's push waits for the engine with the GIL released. An async read's never waits:
//! a push that finds the channel full is sent by a task of the runtime, which then wakes the
//! read's event loop through a socket the loop watches, so the loop runs on meanwhile.

mod pushes;
mod room;
#[cfg(test)]
mod tests;

use std::mem;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::RecordBatch;
use bytes::Bytes;
use parking_lot::Mutex;
use pyo3::types::PyAnyMethods as _;
use pyo3::{Bound, IntoPyObject as _, Py, PyAny, PyErr, Python, intern, pyclass, pymethods};
use rdlt_connector::{ConnectorError, Headroom};
use serde_json::value::RawValue;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::calls::{self, Unanswered};
use crate::errors::Failure;
use crate::json::JsonLimits;
use crate::runtime;
use crate::source::context::{NativeNotice, NativeStreamContext};
use pushes::Pushes;
use room::Room;

/// What a partition's thread sends its read.
#[derive(Debug)]
pub(crate) enum Pushed {
    /// JSON lines, within one push's bytes.
    Json(Bytes),
    /// An Arrow batch.
    Batch(RecordBatch),
    /// A checkpoint's cursor, as its JSON text.
    Checkpoint(Box<RawValue>),
    /// The stream's partitions changed: the engine plans it again.
    Replan,
    /// How far behind its source's newest the read is, and how much more its source keeps.
    Behind {
        /// The records.
        records: u64,
        /// How much more the source keeps before it drops where the read stands.
        headroom: Headroom,
    },
    /// The read failed: its generator raised, or yielded what cannot be pushed.
    Failed(ConnectorError),
}

/// Where a partition's thread sends what it pushes, which its out and its context share: none
/// once the read has said how it ended, which closes the channel at once, since Python's collector
/// may keep either object, and with it the channel, alive long after.
#[derive(Clone, Default)]
pub(crate) struct Outlet(Arc<Mutex<Option<mpsc::Sender<Pushed>>>>);

impl Outlet {
    /// Opens the outlet onto `sent`.
    fn open(&self, sent: mpsc::Sender<Pushed>) {
        *self.0.lock() = Some(sent);
    }

    /// Where to send, cloned out of the lock, which is never held while a send waits for the
    /// engine; none once the read has said how it ended.
    pub(crate) fn sender(&self) -> Option<mpsc::Sender<Pushed>> {
        self.0.lock().clone()
    }

    /// Closes the outlet, answering where it sent.
    fn close(&self) -> Option<mpsc::Sender<Pushed>> {
        self.0.lock().take()
    }

    /// Whether the read has gone: it said how it ended, or the engine stopped listening.
    pub(crate) fn gone(&self) -> bool {
        self.0.lock().as_ref().is_none_or(mpsc::Sender::is_closed)
    }
}

/// A partition's read, and what it sends.
pub(crate) struct Reading {
    received: mpsc::Receiver<Pushed>,
    /// Whether the read said how it ended.
    told: Arc<AtomicBool>,
}

impl Reading {
    /// Starts reading, on a daemon thread named `name` meanwhile, what
    /// `stream.read(partition, cursor, context)` returns: an iterator or an async iterator.
    ///
    /// Nothing here takes the GIL or waits.
    ///
    /// # Errors
    ///
    /// [`Unanswered`] where no daemon thread could take the read.
    pub(crate) fn start(
        name: &str,
        stream: Arc<Py<PyAny>>,
        partition: String,
        cursor: Option<String>,
        context: NativeStreamContext,
        limits: JsonLimits,
    ) -> Result<Self, Unanswered> {
        // A yield waits for the engine: the channel holds one push while another is sent.
        let (sent, received) = mpsc::channel(1);
        let told = Arc::new(AtomicBool::new(false));
        let outlet = context.outlet().clone();
        outlet.open(sent);
        let out = PartitionOut {
            sent: outlet.clone(),
            limits,
            told: Arc::clone(&told),
            room: Mutex::default(),
        };
        let unread = (outlet, Arc::clone(&told));
        let name = name.to_owned();
        calls::start(
            move |py| {
                let context = Py::new(py, context)?;
                let out = Py::new(py, out)?;
                let read = calls::reading(py)?.getattr(intern!(py, "read"))?;
                let arguments = (name, stream.as_ref(), partition, cursor, context, out);
                Ok((read, arguments.into_pyobject(py)?))
            },
            move |py, answered| {
                // A read that never began, or raised past what it says itself, is told here.
                if let Err(error) = answered {
                    let (outlet, told) = unread;
                    failed(py, &outlet, &told, &error);
                }
                Box::new(|| ())
            },
        )?;
        Ok(Self { received, told })
    }

    /// The next push, none once the read ended.
    ///
    /// A read let go without saying how it ended fails: a read whose thread met a bug must not
    /// read as a partition read to its end.
    pub(crate) async fn next(&mut self) -> Option<Pushed> {
        let pushed = self.received.recv().await;
        if pushed.is_none() && !self.told.swap(true, Ordering::AcqRel) {
            return Some(Pushed::Failed(ConnectorError::internal(
                "a partition's read was let go without saying how it ended",
            )));
        }
        pushed
    }
}

/// Sends `pushed`, the GIL released while it waits; false once the read has gone.
pub(crate) fn send(py: Python<'_>, sent: &mpsc::Sender<Pushed>, pushed: Pushed) -> bool {
    py.detach(|| sent.blocking_send(pushed).is_ok())
}

/// What a partition's thread pushes to: its read's channel, the limits a read keeps within, and
/// what an async read has waiting for room.
#[pyclass(module = "rdlt._native", name = "PartitionOut", frozen)]
pub(crate) struct PartitionOut {
    sent: Outlet,
    limits: JsonLimits,
    told: Arc<AtomicBool>,
    /// Locked only by the read's own thread, for Rust alone: nothing under it runs Python.
    room: Mutex<Room>,
}

#[pymethods]
impl PartitionOut {
    /// Pushes what a sync read yielded, in order, waiting for the engine with the GIL released;
    /// false once the read has gone or the item failed it, the read then being told so.
    fn push(&self, py: Python<'_>, item: &Bound<'_, PyAny>) -> bool {
        let Some(sent) = self.sent.sender() else {
            return false;
        };
        pushed(py, item, self.limits, &sent)
    }

    /// Sends what an async read yielded without waiting for the engine: true once all of it is
    /// in the channel, false once the read has gone or the item failed it, and none where it
    /// waits for room, which `watched` then says has come.
    ///
    /// The runtime sends an item's push that found no room, and `resume` then sends its rest.
    fn send(&self, py: Python<'_>, item: &Bound<'_, PyAny>) -> Option<bool> {
        let Some(sender) = self.sent.sender() else {
            return Some(false);
        };
        match Pushes::of(py, item, self.limits) {
            Ok(pushes) => self.sending(py, &sender, pushes),
            Err(error) => Some(self.fail(py, &sender, error)),
        }
    }

    /// Gives the engine a notice an async read said, without waiting for it: true once it is in
    /// the channel, false once the read has gone, and none where there is no room, which
    /// `watched` then says has come; the notice is not taken, to be said again then.
    fn notify(&self, py: Python<'_>, notice: &Bound<'_, NativeNotice>) -> Option<bool> {
        let Some(sender) = self.sent.sender() else {
            return Some(false);
        };
        match sender.try_send(notice.get().pushed()) {
            Ok(()) => Some(true),
            Err(TrySendError::Closed(_)) => Some(false),
            Err(TrySendError::Full(_)) => self.waits(py, &sender, None, None),
        }
    }

    /// Sends the rest of the item whose push waited for room, as `send` does; none while that
    /// push still waits.
    fn resume(&self, py: Python<'_>) -> Option<bool> {
        let landed = self.room.lock().landed();
        let (sent, rest) = landed?;
        let sender = self.sent.sender().filter(|_| sent);
        match (sender, rest) {
            (None, _) => Some(false),
            (Some(_), None) => Some(true),
            (Some(sender), Some(pushes)) => self.sending(py, &sender, pushes),
        }
    }

    /// The descriptor of the socket that is readable once what waited for room was sent, or
    /// room came; it is made at the first ask, and closes once the read has said how it ended
    /// and the runtime has let go of it.
    fn watched(&self) -> Result<RawFd, PyErr> {
        Ok(self.room.lock().watched()?)
    }

    /// Takes what made the watched socket readable.
    fn woken(&self) {
        self.room.lock().woken();
    }

    /// Says the read yielded everything, lets go of what waits for room, and closes its channel.
    fn finished(&self) {
        self.told.store(true, Ordering::Release);
        let room = mem::take(&mut *self.room.lock());
        self.sent.close();
        drop(room);
    }

    /// Says the read ended with `error`, lets go of what waits for room, and closes its channel.
    fn failed(&self, py: Python<'_>, error: &Bound<'_, PyAny>) {
        let room = mem::take(&mut *self.room.lock());
        drop(room);
        failed(
            py,
            &self.sent,
            &self.told,
            &PyErr::from_value(error.clone()),
        );
    }
}

impl PartitionOut {
    /// Sends `pushes` while the channel has room; the first push that finds none waits for it,
    /// the rest kept for `resume`.
    fn sending(
        &self,
        py: Python<'_>,
        sender: &mpsc::Sender<Pushed>,
        mut pushes: Pushes,
    ) -> Option<bool> {
        loop {
            match pushes.next(py) {
                Ok(None) => return Some(true),
                Ok(Some(push)) => match sender.try_send(push) {
                    Ok(()) => {}
                    Err(TrySendError::Closed(_)) => return Some(false),
                    Err(TrySendError::Full(push)) => {
                        return self.waits(py, sender, Some(push), Some(pushes));
                    }
                },
                Err(error) => return Some(self.fail(py, sender, error)),
            }
        }
    }

    /// Has the runtime send `push`, or wait for room where there is none, keeping `rest`: none,
    /// the read waits, or false where nothing could wait for it, the read then failing.
    fn waits(
        &self,
        py: Python<'_>,
        sender: &mpsc::Sender<Pushed>,
        push: Option<Pushed>,
        rest: Option<Pushes>,
    ) -> Option<bool> {
        let waited = match runtime::get(py) {
            Ok(runtime) => self.room.lock().wait(runtime, sender, push),
            Err(error) => Err(std::io::Error::other(error.to_string())),
        };
        match waited {
            Ok(()) => {
                if let Some(rest) = rest {
                    self.room.lock().keep(rest);
                }
                None
            }
            Err(why) => {
                // What was left of the item goes outside the lock: dropping it may run Python.
                drop(rest);
                let error = ConnectorError::internal(format!(
                    "an async read could not wait for room in the engine: {why}"
                ));
                Some(self.fail(py, sender, error))
            }
        }
    }

    /// Ends the read with `error`: nothing is sent after it, and it reaches the engine once the
    /// channel has room, without the loop waiting for it; false, the read has gone.
    fn fail(&self, py: Python<'_>, sender: &mpsc::Sender<Pushed>, error: ConnectorError) -> bool {
        self.told.store(true, Ordering::Release);
        self.sent.close();
        if let Err(TrySendError::Full(failure)) = sender.try_send(Pushed::Failed(error)) {
            match runtime::get(py) {
                // Not the read's to cancel: it ends once the engine takes it or stops.
                Ok(runtime) => drop(runtime.spawn({
                    let sender = sender.clone();
                    async move { sender.send(failure).await.ok() }
                })),
                Err(_) => {
                    send(py, sender, failure);
                }
            }
        }
        false
    }
}

/// Tells the read whose channel `outlet` holds that it ended with `error`, and closes it.
fn failed(py: Python<'_>, outlet: &Outlet, told: &AtomicBool, error: &PyErr) {
    told.store(true, Ordering::Release);
    let Some(sent) = outlet.close() else {
        return;
    };
    let failure = Failure::of(py, error);
    send(py, &sent, Pushed::Failed(failure.connector_error()));
}

/// Sends what `item` is as pushes, each waiting for the engine; false once the read has gone or
/// the item failed it.
fn pushed(
    py: Python<'_>,
    item: &Bound<'_, PyAny>,
    limits: JsonLimits,
    sent: &mpsc::Sender<Pushed>,
) -> bool {
    let mut pushes = match Pushes::of(py, item, limits) {
        Ok(pushes) => pushes,
        Err(error) => {
            send(py, sent, Pushed::Failed(error));
            return false;
        }
    };
    loop {
        match pushes.next(py) {
            Ok(None) => return true,
            Ok(Some(push)) => {
                if !send(py, sent, push) {
                    return false;
                }
            }
            Err(error) => {
                send(py, sent, Pushed::Failed(error));
                return false;
            }
        }
    }
}
