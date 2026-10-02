//! A read that starts again from its source's earliest where the source's retention dropped
//! where it stood.

use rdlt_connector::{ConnectorErrorKind, RETENTION_LOST};

use super::{Ingested, PartitionContext, PartitionJob, Progress, abandon, read_and_ingest};
use crate::error::{Error, Side};

/// Reads `job` as [`read_and_ingest`] does; where the source's retention dropped where the read
/// stood and the stream says to reset, reads again from the source's earliest, counted.
///
/// A read had a place to lose where it resumed from a cursor or checkpointed since; one from the
/// beginning that never checkpointed fails instead, as nothing earlier is left to reset to. The
/// failed read's open segment is abandoned: no checkpoint seals the rows it holds.
///
/// A read that loses its place again after a reset, having sealed no row since, fails as
/// retryable: the attempt ends, and the retry policy's backoff and attempts bound what would
/// otherwise reset for ever.
pub(super) async fn read_resetting(
    job: &mut PartitionJob,
    context: &PartitionContext,
) -> Result<Ingested, Error> {
    let mut reset = false;
    loop {
        let (ingested, read) = read_and_ingest(job, context).await?;
        let error = match read {
            Ok(()) => return Ok(ingested),
            // A read the engine stopped may end saying so.
            Err(error) if ingested.stopped && error.kind() == ConnectorErrorKind::Stopped => {
                return Ok(ingested);
            }
            Err(error) => error,
        };
        let resets = job.reset_retention
            && ingested.last_cursor.is_some()
            && error.code() == Some(RETENTION_LOST);
        let stalled = resets && reset && ingested.sealed_rows == 0;
        if !resets || stalled {
            let failed = Error::connector(
                Side::Source,
                format!("reading stream {}", job.stream),
                error,
            )
            .with_stream(&job.stream);
            return Err(if stalled { failed.retryable() } else { failed });
        }
        abandon(job.index, &ingested.open, context).await?;
        context.report(Progress::RetentionReset {
            partition: job.index,
        })?;
        job.cursor = None;
        reset = true;
    }
}
