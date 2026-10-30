//! A commit sent again in place: where the destination answers it transiently, the same commit,
//! by the same key and expecting the same key landed, until it answers otherwise or the run's
//! retry policy gives up.

use rdlt_connector::wire::HOST_CODES;
use rdlt_connector::{CommitMeta, Receipt};

use super::{Coordinator, cancelled};
use crate::error::{Error, Origin, Side};
use crate::run::backoff;

impl Coordinator {
    /// The receipt of `meta`, sent until the destination answers it other than transiently, as
    /// many times at most as the run makes attempts, each wait between drawn as the run's are.
    ///
    /// The session is locked for each try alone, so writers open and schema changes apply
    /// between them; partitions read on, and what they seal waits for the next commit.
    ///
    /// # Errors
    ///
    /// The destination's error where it answered otherwise, or transiently on the last try;
    /// that error too where the run is asked to stop after committing during a wait; a cancelled
    /// error where the attempt is cancelled.
    pub(super) async fn tried(&self, meta: &CommitMeta) -> Result<Receipt, Error> {
        let retry = self.parts.retry;
        let mut tries = 0;
        loop {
            let session = self.parts.tables.session();
            let answered = tokio::select! {
                biased;
                // An answer and the cancel together: the answer wins, so a commit that landed
                // is recorded where it can be.
                answered = session.commit(meta) => answered?,
                () = self.parts.cancel.cancelled() => return Err(cancelled()),
            };
            let error = match answered {
                Ok(receipt) => return Ok(receipt),
                Err(error) => Error::connector(Side::Destination, "committing", error),
            };
            tries += 1;
            if !answered_transiently(&error) || tries >= retry.attempts().get() {
                return Err(error);
            }
            let wait = backoff(&retry, &error, tries, self.parts.env.as_ref());
            tokio::select! {
                biased;
                // Cancellation wins: the attempt is ending.
                () = self.parts.cancel.cancelled() => return Err(cancelled()),
                // A stop after committing ends the attempt with the commit's error, as it ends
                // the run's wait between attempts.
                () = self.parts.stop.cancelled() => return Err(error),
                () = self.parts.env.sleep(wait) => {}
            }
            self.parts.tally.add(|counters| {
                counters.commits.retried = counters.commits.retried.saturating_add(1);
            });
        }
    }
}

/// Whether the destination answered `error` transiently: a transient or rate-limited error its
/// connector raised, and none its host found of the connection, after which nothing tells
/// whether the commit applied; nor the engine's wait ending the call, whose call may still run.
fn answered_transiently(error: &Error) -> bool {
    error.origin() == Origin::Destination
        && error.is_retryable()
        && !error
            .connector_code()
            .is_some_and(|code| HOST_CODES.contains(&code))
}
