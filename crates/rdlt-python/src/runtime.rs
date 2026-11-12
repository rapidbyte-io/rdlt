//! The process's tokio runtime, which runs every pipeline: built at first use, and refused in a
//! child forked after that, whose copy has no threads.

#[cfg(test)]
mod tests;

use std::future::Future;
use std::io;
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::{PyErr, Python};
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;

use crate::{calls, errors};

/// The runtime, once built, and the id of the process that built it.
static RUNTIME: Mutex<Option<(&'static Runtime, u32)>> = Mutex::new(None);

/// How often a wait on the runtime looks for a signal Python's main thread is to handle.
const SIGNALS: Duration = Duration::from_millis(100);

/// Whether a runtime the process `built` made is a copy in `pid`, a child forked from it.
const fn forked_from(built: u32, pid: u32) -> bool {
    built != pid
}

/// Why the process has no runtime to give.
enum Unavailable {
    Forked,
    NoThreads(io::Error),
}

/// The process's runtime, built now if none is, with a daemon thread started to take its calls.
///
/// # Errors
///
/// `rdlt.InternalError` in a child forked from a process whose runtime was built, and where the
/// runtime's threads or the daemon thread do not start.
pub(crate) fn get(py: Python<'_>) -> Result<&'static Runtime, PyErr> {
    let pid = std::process::id();
    // Released before any error is built: building one runs Python, which may switch the GIL.
    let answer = {
        let mut held = RUNTIME.lock();
        match *held {
            Some((_, built)) if forked_from(built, pid) => Err(Unavailable::Forked),
            Some((runtime, _)) => Ok(runtime),
            None => match Builder::new_multi_thread()
                .enable_all()
                .thread_name("rdlt")
                .build()
            {
                Ok(runtime) => {
                    // Lives as long as the process: its threads end with it, and nothing waits
                    // for them.
                    let runtime: &'static Runtime = Box::leak(Box::new(runtime));
                    *held = Some((runtime, pid));
                    Ok(runtime)
                }
                Err(error) => Err(Unavailable::NoThreads(error)),
            },
        }
    };
    let runtime = answer.map_err(|unavailable| match unavailable {
        Unavailable::Forked => errors::internal(
            py,
            "rdlt cannot run in a process forked from one whose runtime had started: start the \
             process with multiprocessing's spawn or forkserver method",
        ),
        Unavailable::NoThreads(error) => {
            errors::internal(py, &format!("the runtime's threads did not start: {error}"))
        }
    })?;
    // The calls a run makes need a daemon thread to take them, which this thread, Python's own,
    // starts.
    calls::ready(py).map_err(|unready| errors::internal(py, &unready.to_string()))?;
    Ok(runtime)
}

/// What a wait does with the exception a signal's handler raised.
pub(crate) enum Interrupted {
    /// Waits on: the handler's exception was answered otherwise, as a first Ctrl-C stops a run.
    Continue,
    /// Drops the future, cancelling what it started, and raises this.
    Raise(PyErr),
}

/// Runs `future` to its end on the runtime with the GIL released; a signal's exception, as
/// Ctrl-C's, drops the future and is raised.
pub(crate) fn wait<T: Send + 'static>(
    py: Python<'_>,
    future: impl Future<Output = T> + Send + 'static,
) -> Result<T, PyErr> {
    wait_interrupted(py, future, Interrupted::Raise)
}

/// Runs `future` to its end on the runtime with the GIL released, letting Python's handlers run
/// every [`SIGNALS`]: `interrupted` decides what an exception one raises does.
pub(crate) fn wait_interrupted<T: Send + 'static>(
    py: Python<'_>,
    future: impl Future<Output = T> + Send + 'static,
    interrupted: impl FnMut(PyErr) -> Interrupted,
) -> Result<T, PyErr> {
    let runtime = get(py)?;
    let task = runtime.spawn(future);
    waited(
        py,
        runtime,
        task,
        SIGNALS,
        || py.check_signals(),
        interrupted,
    )
}

/// Waits for `task` on `runtime` in slices of `every`, calling `signals` between them and
/// `interrupted` with what a signal raised.
fn waited<T: Send + 'static>(
    py: Python<'_>,
    runtime: &Runtime,
    mut task: JoinHandle<T>,
    every: Duration,
    mut signals: impl FnMut() -> Result<(), PyErr>,
    mut interrupted: impl FnMut(PyErr) -> Interrupted,
) -> Result<T, PyErr> {
    loop {
        let ended = py.detach(|| {
            // A timer is made within its runtime's context: `block_on` enters only for the poll.
            let _entered = runtime.enter();
            runtime.block_on(tokio::time::timeout(every, &mut task))
        });
        match ended {
            Ok(Ok(answer)) => return Ok(answer),
            Ok(Err(joined)) => {
                let message = format!("a task of the runtime ended without its answer: {joined}");
                return Err(errors::internal(py, &message));
            }
            Err(_) => {
                if let Err(raised) = signals()
                    && let Interrupted::Raise(raised) = interrupted(raised)
                {
                    task.abort();
                    return Err(raised);
                }
            }
        }
    }
}
