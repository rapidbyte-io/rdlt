//! One Python call made from async code: on tokio's blocking pool, the GIL taken inside the call
//! alone.

use pyo3::Python;
use tokio::task::JoinError;

/// Runs `call` attached to the interpreter on a thread of the blocking pool, and answers it.
///
/// The runtime owns the thread: a caller that stops waiting leaves the call to end, its answer
/// dropped, since Python code cannot be interrupted.
pub(crate) async fn blocking<T, F>(call: F) -> Result<T, JoinError>
where
    T: Send + 'static,
    F: for<'py> FnOnce(Python<'py>) -> T + Send + 'static,
{
    tokio::task::spawn_blocking(move || Python::attach(call)).await
}
