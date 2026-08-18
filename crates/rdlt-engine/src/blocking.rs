//! The one seam onto the blocking pool: recovery's file work — the manifest
//! scan, segment opens, Arrow IPC decodes — runs here so an embedder's
//! runtime is never held for its duration.

/// Run one piece of blocking file work off the async runtime.
///
/// Recovery is entirely file I/O, and rdlt is an EMBEDDABLE engine, so this
/// future may be polled on a host's runtime alongside the host's own work.
/// Doing that I/O inline occupies a worker thread for the whole of recovery;
/// on a single-threaded runtime it stalls the host completely. Neither is
/// ours to spend.
///
/// Panic policy, both halves: a panic that reaches THIS seam is re-raised on
/// the calling thread — but the DECODE seats never let one reach it. Replay
/// wraps every Arrow IPC decode in `catch_unwind` INSIDE the closure it hands
/// over (`wal::segment::caught_decode` and the per-batch step), because
/// arrow's decoder has panic arms reachable from malformed but
/// FlatBuffer-valid segment bytes, and WAL bytes are external recovery input
/// — such an unwind IS damaged data and belongs on the same
/// degrade-to-re-extraction path as an ordinary decode error. For everything
/// else that crosses here (manifest line reads, fsyncs, filesystem walks) a
/// panic is a bug in our own logic, not corrupt data, and folding it into
/// "degrade to re-extraction" would hide the defect behind a slower correct
/// path — so the default posture stays re-raise, and a decode seat opts out
/// at its closure, never here.
pub(crate) async fn off_runtime<T, F>(work: F) -> T
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
