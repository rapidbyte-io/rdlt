//! Running blocking file and database calls off the async runtime.

use rdlt_connector::{ConnectorError, Result};

/// Runs `work` on the blocking thread pool.
pub(crate) async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(work).await.map_err(|error| {
        ConnectorError::internal(format!("a blocking call did not finish: {error}"))
    })?
}
