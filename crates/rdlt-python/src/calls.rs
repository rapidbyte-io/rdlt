//! One Python function called from Rust, on one of the process's daemon threads.
//!
//! A user's function never runs with Rust frames beneath it: an interpreter that exits ends the
//! thread that runs it, and on `CPython` 3.11, 3.12 and early 3.13 that unwind through Rust frames
//! aborts the process (ADR 0055). The caller holds no GIL and touches nothing of Python's: it
//! hands the call over as closures, and its arguments are built and its answer read on the
//! daemon thread. A call takes the daemon thread that went idle last, and one that takes the last
//! idle one has it start another before it calls, so no caller waits for a thread to start. No
//! daemon thread ends: one that ended would run its libraries' teardown, which races the
//! process's exit.

#[cfg(test)]
pub(crate) mod tests;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use std::{iter, mem};

use parking_lot::Mutex;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAnyMethods as _, PyModule, PyTuple};
use pyo3::{Bound, Py, PyAny, PyErr, Python, intern, pyclass, pymethods};
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::sync::{mpsc, oneshot};

use crate::errors;

/// What a call answered: what it returned, or what it raised.
pub(crate) type Answered<'py> = Result<Bound<'py, PyAny>, PyErr>;

/// What to call and its arguments, built on the daemon thread.
type Prepare = Box<
    dyn for<'py> FnOnce(Python<'py>) -> Result<(Bound<'py, PyAny>, Bound<'py, PyTuple>), PyErr>
        + Send,
>;

/// Reads a call's answer on the daemon thread, the GIL held, and gives what hands it over.
type Give = Box<dyn for<'py> FnOnce(Python<'py>, Answered<'py>) -> Handover + Send>;

/// Hands a call's answer, read, to its caller.
pub(crate) type Handover = Box<dyn FnOnce() + Send>;

/// Where a daemon thread is handed its calls.
type Daemon = mpsc::UnboundedSender<Call>;

/// The calls a daemon thread waits for.
type Waiting = mpsc::UnboundedReceiver<Call>;

/// A call as `rdlt._reading` unpacks it: the function, its arguments, and where to answer.
type Offered<'py> = (Bound<'py, PyAny>, Bound<'py, PyTuple>, Py<CallAnswer>);

/// A call no daemon thread took, to be dropped outside the pool's lock, and why.
type Unhanded = (Prepare, Give, Unanswered);

/// A call for a daemon thread to make.
///
/// Its fields drop in order, `give` last: a call dropped unanswered has let go of the spare it
/// carried before its caller is told.
struct Call {
    prepare: Prepare,
    /// The daemon thread it was handed to, put back once its answer is read.
    daemon: Daemon,
    /// The calls of the daemon thread to start before the call is made, where it took the last
    /// idle one.
    spare: Option<Waiting>,
    give: Give,
}

/// What a daemon thread does once a call has let go of what it held: it goes back on the pool,
/// then hands the answer over.
struct Done {
    daemon: Daemon,
    handover: Handover,
}

impl Done {
    fn hand_over(self) {
        put_back(self.daemon);
        (self.handover)();
    }
}

/// The process's daemon threads that wait for a call, the last to go idle on top.
///
/// Its lock is held only inside the functions below that take and give back Rust values alone,
/// since a daemon thread takes it with the GIL held: nothing under it runs Python, drops a Python
/// object, or waits. Every call is sent under it, and a daemon thread's calls are closed only
/// under it, so no call is sent to a channel as it closes.
struct Idle {
    /// The process whose daemon threads these are: a forked child's copy names its parent.
    pid: u32,
    daemons: Vec<Daemon>,
    /// Why the pool last lost a daemon thread it held: the thread's start failed, or a bug of
    /// rdlt's own parked it.
    unstarted: Option<String>,
}

static IDLE: Mutex<Idle> = Mutex::new(Idle {
    pid: 0,
    daemons: Vec::new(),
    unstarted: None,
});

/// The process whose daemon threads the pool holds, once one has started there; read before the
/// lock, which a forked child may find held by a thread its parent had.
static STARTED: AtomicU32 = AtomicU32::new(0);

/// How long a process that has started no daemon thread waits for the pool's lock, which a
/// concurrent first start holds a moment: one held as its parent forked is never released.
const CLAIM: Duration = Duration::from_secs(1);

/// What a caller is told where this process has started no daemon thread.
const NONE_STARTED: &str = "no daemon thread was started in this process";

/// Why a call finds the pool empty once the daemon thread it would have taken waits for good.
const ABANDONED: &str = "a daemon thread met a bug of rdlt's own and waits for good";

/// Hands a call to the daemon thread that went idle last, giving it a spare to start where it was
/// the last; one that is gone, a spare whose start failed after another was queued on it, is let
/// go and the next taken.
fn handed(prepare: Prepare, give: Give) -> Result<(), Unhanded> {
    let mut idle = IDLE.lock();
    let (mut prepare, mut give) = (prepare, give);
    loop {
        let Some(daemon) = idle.daemons.pop() else {
            let why = idle.unstarted.clone();
            let why = why.unwrap_or_else(|| NONE_STARTED.to_owned());
            return Err((prepare, give, Unanswered::Started(why)));
        };
        let (kept, spare) = idle
            .daemons
            .is_empty()
            .then(mpsc::unbounded_channel)
            .unzip();
        let call = Call {
            prepare,
            give,
            daemon: daemon.clone(),
            spare,
        };
        match daemon.send(call) {
            Ok(()) => {
                idle.daemons.extend(kept);
                return Ok(());
            }
            Err(unsent) => (prepare, give) = (unsent.0.prepare, unsent.0.give),
        }
    }
}

/// Puts `daemon` back on top of the pool, idle.
fn put_back(daemon: Daemon) {
    IDLE.lock().daemons.push(daemon);
}

/// Closes `waiting`, the calls of a daemon thread that will make none, with the calls of each
/// spare a call queued there carried, takes their senders off the pool, and keeps `why` for a call
/// that finds the pool empty where that took one; the calls queued are given back, to be dropped
/// once the lock is let go, unanswered.
fn closed(waiting: &mut Waiting, why: String) -> Vec<Call> {
    let mut idle = IDLE.lock();
    waiting.close();
    let mut queued: Vec<Call> = iter::from_fn(|| waiting.try_recv().ok()).collect();
    let mut index = 0;
    while let Some(call) = queued.get_mut(index) {
        if let Some(spare) = call.spare.as_mut() {
            spare.close();
            let carried: Vec<Call> = iter::from_fn(|| spare.try_recv().ok()).collect();
            queued.extend(carried);
        }
        index += 1;
    }
    let held = idle.daemons.len();
    idle.daemons.retain(|daemon| !daemon.is_closed());
    if idle.daemons.len() < held {
        idle.unstarted = Some(why);
    }
    queued
}

/// Makes the pool process `pid`'s, forgetting a parent's copy unread; false where its lock stays
/// held, as by a thread a fork left behind.
///
/// It waits for the lock with the GIL released, so its wait of up to [`CLAIM`], for a lock a fork
/// left held or a concurrent first start, stalls no other Python thread.
fn claimed(py: Python<'_>, pid: u32) -> bool {
    py.detach(|| {
        let Some(mut idle) = IDLE.try_lock_for(CLAIM) else {
            return false;
        };
        if idle.pid != pid {
            // Their channels may be mid-change on a thread the fork left behind: never touched.
            mem::forget(mem::take(&mut idle.daemons));
            idle.pid = pid;
            idle.unstarted = None;
        }
        true
    })
}

/// Puts `daemon`, started in process `pid`, on the pool, which callers there use from now on.
fn adopted(pid: u32, daemon: Daemon) {
    IDLE.lock().daemons.push(daemon);
    STARTED.store(pid, Ordering::Release);
}

/// The calls one daemon thread waits for, and what it hands over once a call has let go of what
/// it held.
#[pyclass(module = "rdlt._native", name = "Calls", frozen)]
#[derive(Default)]
struct Calls {
    /// Its calls, given it once it is made, so one whose thread does not start keeps them to close.
    waiting: Mutex<Option<Waiting>>,
    done: Arc<Mutex<Option<Done>>>,
}

#[pymethods]
impl Calls {
    /// Hands over the last call's answer, where one waits, without going back on the pool, and
    /// closes the daemon thread's calls: it is about to wait for good.
    fn abandon(&self) {
        let done = self.done.lock().take();
        if let Some(done) = done {
            (done.handover)();
        }
        // What was handed it goes unanswered, once the pool's lock is let go, and nothing is
        // handed it after.
        let queued = self
            .waiting
            .lock()
            .as_mut()
            .map(|waiting| closed(waiting, ABANDONED.to_owned()));
        drop(queued);
    }

    /// Hands over the last call's answer, then waits, the GIL released, for the next call.
    ///
    /// A call whose arguments cannot be built is answered with why, here.
    fn next<'py>(&self, py: Python<'py>) -> Result<Offered<'py>, PyErr> {
        let done = self.done.lock().take();
        if let Some(done) = done {
            done.hand_over();
        }
        loop {
            let call = py
                .detach(|| self.waiting.lock().as_mut()?.blocking_recv())
                .expect(
                    "a daemon thread waiting for a call is on the pool, which holds its sender",
                );
            if let Some(waiting) = call.spare {
                spare(py, waiting);
            }
            match (call.prepare)(py) {
                Ok((function, arguments)) => {
                    let answer = CallAnswer {
                        give: Mutex::new(Some((call.give, call.daemon))),
                        done: Arc::clone(&self.done),
                    };
                    return Ok((function, arguments, Py::new(py, answer)?));
                }
                Err(error) => {
                    let handover = (call.give)(py, Err(error));
                    Done {
                        daemon: call.daemon,
                        handover,
                    }
                    .hand_over();
                }
            }
        }
    }
}

/// Where a call's thread says how it ended.
#[pyclass(module = "rdlt._native", name = "CallAnswer", frozen)]
struct CallAnswer {
    /// How the answer is read, and the daemon thread reading it; none once it has been.
    give: Mutex<Option<(Give, Daemon)>>,
    done: Arc<Mutex<Option<Done>>>,
}

impl CallAnswer {
    /// Reads `answered`, which is handed over once the call has let go of what it held; a panic
    /// here leaves the daemon thread off the pool.
    fn give<'py>(&self, py: Python<'py>, answered: Answered<'py>) {
        let given = self.give.lock().take();
        if let Some((give, daemon)) = given {
            let handover = give(py, answered);
            *self.done.lock() = Some(Done { daemon, handover });
        }
    }
}

#[pymethods]
impl CallAnswer {
    /// Says the call returned `value`.
    fn returned<'py>(&self, py: Python<'py>, value: Bound<'py, PyAny>) {
        self.give(py, Ok(value));
    }

    /// Says the call raised `error`.
    fn raised<'py>(&self, py: Python<'py>, error: &Bound<'py, PyAny>) {
        self.give(py, Err(PyErr::from_value(error.clone())));
    }
}

/// The package's module of thread bodies, imported once: the package imports it as it loads.
pub(crate) fn reading(py: Python<'_>) -> Result<Bound<'_, PyModule>, PyErr> {
    static READING: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
    let reading =
        READING.get_or_try_init(py, || Ok::<_, PyErr>(py.import("rdlt._reading")?.unbind()))?;
    Ok(reading.bind(py).clone())
}

/// Starts a daemon thread making the calls `calls` waits for, through `_thread` called from here,
/// so no bytecode runs beneath this thread's Rust frames.
fn launched(py: Python<'_>, calls: &Bound<'_, Calls>) -> Result<(), PyErr> {
    let serve = reading(py)?.getattr(intern!(py, "serve"))?;
    py.import("_thread")?
        .getattr(intern!(py, "start_new_thread"))?
        .call1((serve, (calls,)))?;
    Ok(())
}

/// What `error` says: its type and its message.
fn said(py: Python<'_>, error: &PyErr) -> String {
    let message = error
        .value(py)
        .str()
        .map(|text| text.to_string())
        .unwrap_or_default();
    format!("{}: {message}", errors::type_name(py, error))
}

/// Starts the daemon thread that makes the calls `waiting` holds; one that does not start is
/// closed, saying why, and what was queued on it goes unanswered.
fn spare(py: Python<'_>, waiting: Waiting) {
    let mut waiting = Some(waiting);
    let started = Bound::new(py, Calls::default()).and_then(|calls| {
        *calls.get().waiting.lock() = waiting.take();
        launched(py, &calls).inspect_err(|_| waiting = calls.get().waiting.lock().take())
    });
    if let (Err(error), Some(mut waiting)) = (started, waiting) {
        let queued = closed(&mut waiting, said(py, &error));
        drop(queued);
    }
}

/// Starts this process's first daemon thread, on the asking thread, which Python owns: once per
/// process, so a child forked from one that had started its own starts afresh.
///
/// # Errors
///
/// [`Unanswered::Started`] where the thread did not start, or where the pool stays locked, as by
/// a thread the fork left behind.
pub(crate) fn ready(py: Python<'_>) -> Result<(), Unanswered> {
    let pid = std::process::id();
    if STARTED.load(Ordering::Acquire) == pid {
        return Ok(());
    }
    if !claimed(py, pid) {
        return Err(Unanswered::Started(
            "the process was forked while its parent handed a call over: start it with \
             multiprocessing's spawn or forkserver method"
                .to_owned(),
        ));
    }
    let (daemon, waiting) = mpsc::unbounded_channel();
    // Its sender is not on the pool yet, so no call can be sent as it closes where it fails.
    let calls = Calls {
        waiting: Mutex::new(Some(waiting)),
        done: Arc::default(),
    };
    let started = Bound::new(py, calls).and_then(|calls| launched(py, &calls));
    if let Err(error) = started {
        return Err(Unanswered::Started(said(py, &error)));
    }
    adopted(pid, daemon);
    Ok(())
}

/// Why a call has no answer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Unanswered {
    /// The call was let go unmade, or its answer before it was read: its daemon thread did not
    /// start, or met a bug of rdlt's own.
    #[error("the call was let go without its answer")]
    Dropped,
    /// No daemon thread was idle, the pool having lost the last it held to a start that failed
    /// or to a bug of rdlt's own that parked it, or none was started in this process: why, as
    /// text, since the exception would need the GIL wherever it was read.
    #[error("the call's thread did not start: {0}")]
    Started(String),
}

/// Hands the call `prepare` builds to the daemon thread that went idle last; `give` reads what it
/// returned or raised on that thread, and is dropped unanswered where the call is let go without
/// its answer.
///
/// The caller holds no GIL, and waits for nothing here.
///
/// # Errors
///
/// [`Unanswered`] where no daemon thread could take the call.
pub(crate) fn start<P, G>(prepare: P, give: G) -> Result<(), Unanswered>
where
    P: for<'py> FnOnce(Python<'py>) -> Result<(Bound<'py, PyAny>, Bound<'py, PyTuple>), PyErr>
        + Send
        + 'static,
    G: for<'py> FnOnce(Python<'py>, Answered<'py>) -> Handover + Send + 'static,
{
    if STARTED.load(Ordering::Acquire) != std::process::id() {
        return Err(Unanswered::Started(NONE_STARTED.to_owned()));
    }
    // A call no daemon thread took is dropped here, outside the pool's lock.
    handed(Box::new(prepare), Box::new(give)).map_err(|(_, _, why)| why)
}

/// Calls the function `prepare` names, with the arguments it builds, on a daemon thread, and
/// answers `finish` of what it returned or raised; the caller waits here, the GIL released, for a
/// thread with no runtime to await on, and where this thread is a runtime's worker, that
/// worker's other tasks move to another thread meanwhile.
///
/// # Errors
///
/// [`Unanswered`] where the call was let go without its answer, or its thread did not start.
pub(crate) fn blocking<T, P, F>(prepare: P, finish: F) -> Result<T, Unanswered>
where
    T: Send + 'static,
    P: for<'py> FnOnce(Python<'py>) -> Result<(Bound<'py, PyAny>, Bound<'py, PyTuple>), PyErr>
        + Send
        + 'static,
    F: for<'py> FnOnce(Python<'py>, Answered<'py>) -> T + Send + 'static,
{
    let (sender, received) = std::sync::mpsc::channel();
    away_from_the_runtime(move || {
        start(prepare, move |py, answered| {
            let finished = finish(py, answered);
            Box::new(move || {
                sender.send(finished).ok();
            })
        })?;
        received.recv().map_err(|_| Unanswered::Dropped)
    })
}

/// Runs `wait`, leaving the runtime its worker where this thread is one of a multi-thread
/// runtime's: a user's function takes as long as it takes.
fn away_from_the_runtime<R>(wait: impl FnOnce() -> R) -> R {
    match Handle::try_current().map(|handle| handle.runtime_flavor()) {
        Ok(RuntimeFlavor::MultiThread) => tokio::task::block_in_place(wait),
        _ => wait(),
    }
}

/// Calls the function `prepare` names, with the arguments it builds, on a daemon thread, and
/// answers `finish` of what it returned or raised on that thread.
///
/// A caller that stops waiting leaves the call to end, its answer dropped, since Python code
/// cannot be interrupted.
///
/// # Errors
///
/// [`Unanswered`] where the call was let go without its answer, or its thread did not start.
pub(crate) async fn call<T, P, F>(prepare: P, finish: F) -> Result<T, Unanswered>
where
    T: Send + 'static,
    P: for<'py> FnOnce(Python<'py>) -> Result<(Bound<'py, PyAny>, Bound<'py, PyTuple>), PyErr>
        + Send
        + 'static,
    F: for<'py> FnOnce(Python<'py>, Answered<'py>) -> T + Send + 'static,
{
    let (sender, received) = oneshot::channel();
    start(prepare, move |py, answered| {
        let finished = finish(py, answered);
        Box::new(move || {
            sender.send(finished).ok();
        })
    })?;
    received.await.map_err(|_| Unanswered::Dropped)
}
