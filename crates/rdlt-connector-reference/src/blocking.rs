//! Running blocking file and database calls off the async runtime.

use rdlt_connector::{ConnectorError, Result};

#[cfg(test)]
mod tests;

/// Runs `work` on the blocking thread pool.
pub(crate) async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    // What a panic said is not this connector's to tell its host: it may quote what was sent.
    tokio::task::spawn_blocking(work).await.map_err(|error| {
        ConnectorError::internal(if error.is_panic() {
            "a blocking call panicked"
        } else {
            "a blocking call was cancelled"
        })
    })?
}
