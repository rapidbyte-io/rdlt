//! `_native.Run` and `_native.Events`: a run driven on the runtime, answering its events, its
//! status and its stops while it runs, cancelled once every handle to it is dropped.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::exceptions::{PyKeyboardInterrupt, PyValueError};
use pyo3::types::PyAnyMethods as _;
use pyo3::{Bound, Py, PyAny, PyErr, PyRef, Python, pyclass, pymethods};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot, watch};

use crate::runtime::Interrupted;
use crate::{errors, runtime};

/// What a run ended with: its outcome as JSON, and its error report as JSON where it failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Ended {
    outcome: String,
    error: Option<String>,
}

/// An outcome as `rdlt.Outcome` reads it.
#[derive(Serialize)]
struct Told<'a> {
    report: &'a rdlt::Report,
    error: Option<&'a rdlt::ErrorReport>,
}

impl Ended {
    fn of(outcome: &rdlt::Outcome) -> Self {
        let report = outcome.error.as_ref().map(rdlt::Error::report);
        let error = report
            .as_ref()
            .map(|report| serde_json::to_string(report).expect("an error report serializes"));
        let told = Told {
            report: &outcome.report,
            error: report.as_ref(),
        };
        Self {
            outcome: serde_json::to_string(&told).expect("a run's report serializes"),
            error,
        }
    }
}

/// What a handle asks the run's driver.
enum Command {
    Events(oneshot::Sender<rdlt::Events>),
    Status(oneshot::Sender<String>),
    Stop(rdlt::StopMode),
    /// Stops the run after its next commit and lets it finish though every handle is dropped,
    /// holding what its connectors' secrets are resolved through until it ends.
    Detach(Py<PyAny>),
    /// Drops the run now, which cancels it.
    Cancel,
}

/// What a blocking wait says on standard error at a first Ctrl-C, as the CLI does.
const STOPPING: &str = "rdlt: stopping after the next commit; press Ctrl-C again to stop now\n";

/// How a wait on a run answers a signal's exception: a first `KeyboardInterrupt` asks the run
/// to stop after its next commit and waits on; a second, or any other exception, cancels the
/// run and is raised.
fn interrupted(
    py: Python<'_>,
    commands: &mpsc::UnboundedSender<Command>,
    stopping: &mut bool,
    raised: PyErr,
) -> Interrupted {
    if *stopping || !raised.is_instance_of::<PyKeyboardInterrupt>(py) {
        drop(commands.send(Command::Cancel));
        return Interrupted::Raise(raised);
    }
    *stopping = true;
    drop(commands.send(Command::Stop(rdlt::StopMode::AfterCommit)));
    let note = py
        .import("sys")
        .and_then(|sys| sys.getattr("stderr")?.call_method1("write", (STOPPING,)));
    drop(note);
    Interrupted::Continue
}

/// A pipeline's run, driven on the runtime.
#[pyclass(module = "rdlt._native", name = "Run", frozen)]
pub(crate) struct NativeRun {
    commands: mpsc::UnboundedSender<Command>,
    ended: watch::Receiver<Option<Arc<Ended>>>,
    /// The subscription made before the run's first poll, which the first `events` takes, so its
    /// reader sees every event from the first or is told by `Lagged` what it missed.
    first: Mutex<Option<rdlt::Events>>,
}

impl NativeRun {
    /// Subscribes to `run`, drives it on the runtime until it ends, and until every handle to it
    /// is dropped.
    pub(crate) fn started(py: Python<'_>, run: rdlt::Run) -> Result<Self, PyErr> {
        let (commands, asked) = mpsc::unbounded_channel();
        let (end, ended) = watch::channel(None);
        let first = Mutex::new(Some(run.events()));
        drop(runtime::get(py)?.spawn(drive(run, asked, end)));
        Ok(Self {
            commands,
            ended,
            first,
        })
    }

    /// A receiver of what the run ended with, which errs once the driver ended without it.
    pub(crate) fn ended(&self) -> watch::Receiver<Option<Arc<Ended>>> {
        self.ended.clone()
    }

    fn ask<T: Send + 'static>(
        &self,
        py: Python<'_>,
        command: impl FnOnce(oneshot::Sender<T>) -> Command,
    ) -> Result<Option<T>, PyErr> {
        let (reply, answer) = oneshot::channel();
        if self.commands.send(command(reply)).is_err() {
            return Ok(None);
        }
        runtime::wait(py, async move { answer.await.ok() })
    }
}

/// Polls `run` to its end, answering `asked` meanwhile and after; returns once every handle has
/// dropped its sender, which drops the run and so cancels one still running, unless the run was
/// detached, which it then polls to its end.
async fn drive(
    mut run: rdlt::Run,
    mut asked: mpsc::UnboundedReceiver<Command>,
    end: watch::Sender<Option<Arc<Ended>>>,
) {
    let mut running = true;
    let mut detached = None;
    loop {
        tokio::select! {
            // The run's end wins over a command beside it: a status asked as it ends sees it ended.
            biased;
            outcome = &mut run, if running => {
                running = false;
                detached = None;
                end.send_replace(Some(Arc::new(Ended::of(&outcome))));
            }
            command = asked.recv() => match command {
                Some(Command::Events(reply)) => drop(reply.send(run.events())),
                Some(Command::Status(reply)) => {
                    let status = serde_json::to_string(&run.status()).expect("a status serializes");
                    drop(reply.send(status));
                }
                Some(Command::Stop(mode)) => run.stop(mode),
                Some(Command::Detach(store)) => {
                    detached = running.then_some(store);
                    run.stop(rdlt::StopMode::AfterCommit);
                }
                None if detached.is_some() && running => {
                    // Every handle is gone: the run finishes for what is left awaiting it.
                    let outcome = (&mut run).await;
                    end.send_replace(Some(Arc::new(Ended::of(&outcome))));
                    return;
                }
                // A run that ended has nothing to cancel, and answers its handles on.
                Some(Command::Cancel) if !running => {}
                Some(Command::Cancel) | None => return,
            },
        }
    }
}

#[pymethods]
impl NativeRun {
    /// A subscription to the run's events; the first sees them from the first, a later one from
    /// now on; one asked after a cancelled run is empty, and it holds the run going, as a handle.
    pub(crate) fn events(&self, py: Python<'_>) -> Result<NativeEvents, PyErr> {
        let first = self.first.lock().take();
        let stream = match first {
            Some(first) => Some(first),
            None => self.ask(py, Command::Events)?,
        };
        Ok(NativeEvents {
            reader: Arc::new(Reader::new(stream)),
            _run: self.commands.clone(),
        })
    }

    /// Where the run stands now, as JSON.
    fn status(&self, py: Python<'_>) -> Result<String, PyErr> {
        self.ask(py, Command::Status)?
            .ok_or_else(|| errors::stopped(py))
    }

    /// Asks the run to stop: `after_commit`, or `now`; refused in a forked child, whose copy of
    /// the run has no driver to ask.
    fn stop(&self, py: Python<'_>, mode: &str) -> Result<(), PyErr> {
        runtime::get(py)?;
        let mode = match mode {
            "after_commit" => rdlt::StopMode::AfterCommit,
            "now" => rdlt::StopMode::Now,
            _ => return Err(PyValueError::new_err("no such stop mode")),
        };
        // A run that ended has nothing to stop.
        drop(self.commands.send(Command::Stop(mode)));
        Ok(())
    }

    /// Asks the run to stop after its next commit and lets it finish in the background, though
    /// every handle to it is then dropped, as a cancelled await of it does; `store`, what its
    /// connectors' secrets are resolved through, is held until it ends.
    fn detach(&self, store: Py<PyAny>) {
        drop(self.commands.send(Command::Detach(store)));
    }

    /// Waits, the GIL released, for the run's end: its outcome, and its error report where it
    /// failed.
    ///
    /// A first Ctrl-C stops the run after its next commit and waits on; a second cancels it now
    /// and raises `KeyboardInterrupt`.
    pub(crate) fn wait(&self, py: Python<'_>) -> Result<(String, Option<String>), PyErr> {
        let mut stopping = false;
        let on_signal = |raised| interrupted(py, &self.commands, &mut stopping, raised);
        let ended = runtime::wait_interrupted(py, ended(self.ended()), on_signal)?;
        let ended = ended.ok_or_else(|| errors::stopped(py))?;
        Ok((ended.outcome.clone(), ended.error.clone()))
    }

    /// Cancels the run now: what it staged and did not commit is discarded at the next open;
    /// refused in a forked child, whose copy of the run has no driver to ask.
    fn cancel(&self, py: Python<'_>) -> Result<(), PyErr> {
        runtime::get(py)?;
        drop(self.commands.send(Command::Cancel));
        Ok(())
    }

    /// An awaitable of what `wait` answers, completed on the event loop's thread; dropping it,
    /// as cancelling its asyncio task does, leaves the run to its handles.
    pub(crate) fn wait_async<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyAny>, PyErr> {
        runtime::get(py)?;
        let receiver = self.ended();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let ended = ended(receiver)
                .await
                .ok_or_else(|| Python::attach(errors::stopped))?;
            Ok((ended.outcome.clone(), ended.error.clone()))
        })
    }
}

/// What the run ended with; `None` where its driver ended without it, cancelled.
async fn ended(mut receiver: watch::Receiver<Option<Arc<Ended>>>) -> Option<Arc<Ended>> {
    let ended = receiver.wait_for(Option::is_some).await.ok()?;
    ended.clone()
}

/// One subscriber's events, as JSON: `{"event": …}`, or `{"lagged": {"dropped": n}}`.
#[pyclass(module = "rdlt._native", name = "Events", frozen)]
pub(crate) struct NativeEvents {
    reader: Arc<Reader>,
    /// A handle of the run: a subscriber keeps its run going until it is dropped too.
    _run: mpsc::UnboundedSender<Command>,
}

/// What a subscriber reads, and the item it has read and not yet taken.
///
/// A read waits for an item and then takes it, in two steps: an item read as its wait is
/// interrupted or cancelled stays here for the next read, rather than going with an answer
/// nobody is left to receive.
struct Reader {
    /// Held across the wait for the next item, so a subscription's readers read one at a time.
    events: tokio::sync::Mutex<Option<rdlt::Events>>,
    read: Mutex<Option<String>>,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Item<'a> {
    Event(&'a rdlt::Event),
    Lagged(&'a rdlt::Lagged),
}

impl Reader {
    fn new(events: Option<rdlt::Events>) -> Self {
        Self {
            events: tokio::sync::Mutex::new(events),
            read: Mutex::new(None),
        }
    }

    /// Waits for an item to take; false once the run ended and every event is read.
    async fn readable(&self) -> bool {
        let mut events = self.events.lock().await;
        if self.read.lock().is_some() {
            return true;
        }
        let Some(subscription) = events.as_mut() else {
            return false;
        };
        let Some(item) = subscription.next().await else {
            *events = None;
            return false;
        };
        let item = match &item {
            Ok(event) => Item::Event(event),
            Err(lagged) => Item::Lagged(lagged),
        };
        let item = serde_json::to_string(&item).expect("an event serializes");
        *self.read.lock() = Some(item);
        true
    }

    fn take(&self) -> Option<String> {
        self.read.lock().take()
    }
}

#[pymethods]
impl NativeEvents {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// The next event, the GIL released while it waits; none once the run ended.
    pub(crate) fn __next__(&self, py: Python<'_>) -> Result<Option<String>, PyErr> {
        loop {
            if let Some(item) = self.reader.take() {
                return Ok(Some(item));
            }
            let reader = Arc::clone(&self.reader);
            if !runtime::wait(py, async move { reader.readable().await })? {
                return Ok(None);
            }
        }
    }

    /// An awaitable of whether an item waits to be taken, false once the run ended and every
    /// event is read; cancelling it loses nothing.
    fn readable<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyAny>, PyErr> {
        runtime::get(py)?;
        let reader = Arc::clone(&self.reader);
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(reader.readable().await) })
    }

    /// The item that waits to be taken, if one does.
    fn take(&self) -> Option<String> {
        self.reader.take()
    }
}
