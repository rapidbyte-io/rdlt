//! WAL resume: single forward scan of the manifest, replay of the uncommitted span,
//! degradation to re-extraction on any damage (slower, never wrong).

mod replay;
mod scan;

pub(crate) use replay::replay;
pub(crate) use scan::{RecoverySpan, ScanOutcome, scan_off_runtime};

/// Run one piece of blocking file work off the async runtime.
///
/// Recovery is entirely file I/O — opening segments, decoding Arrow IPC — and
/// rdlt is an EMBEDDABLE engine, so this future may be polled on a host's
/// runtime alongside the host's own work. Doing that I/O inline occupies a
/// worker thread for the whole of recovery; on a single-threaded runtime it
/// stalls the host completely. Neither is ours to spend.
///
/// A panic inside the closure is re-raised on this thread rather than
/// translated: it is a bug in decode logic, not a damaged WAL, and the damage
/// arms exist to degrade from corrupt DATA. Turning a panic into "degrade to
/// re-extraction" would hide a defect behind a slower correct path.
async fn off_runtime<T, F>(work: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(work).await {
        Ok(value) => value,
        Err(joined) => match joined.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            // spawn_blocking tasks are never cancelled by this code, so a
            // non-panic join failure means the runtime itself is shutting down.
            Err(_) => panic!("WAL recovery task cancelled: runtime is shutting down"),
        },
    }
}
