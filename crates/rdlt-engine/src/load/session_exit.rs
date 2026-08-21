//! The ONE owner of the session-exit discipline: which close a session
//! earns, and what a strict close's failure means.
//!
//! THE RULE, stated once because three call sites used to re-derive it:
//! a session whose LAST COMMIT SUCCEEDED closes STRICTLY — the close's
//! error propagates, classified non-retryable, behind the frozen
//! `session close failed AFTER all commits were durable` prefix. Every
//! commit is ALREADY durable by then, so the failure can never mean
//! lost data — it means some OTHER resource (a lock, a lease document)
//! failed to release, and the prefix says so explicitly rather than
//! leaving the operator to wonder whether the run's data survived. It
//! is classified NON-RETRYABLE unconditionally (`Error::destination`,
//! never `classify_dest_error`, which would trust the destination's OWN
//! transient/fatal classification — a destination has no way to know
//! this specific failure can never be helped by re-running the WHOLE
//! load from committed state, since retrying would re-execute a commit
//! that already landed).
//!
//! A session being ABANDONED — a failed, cancelled, or degraded path
//! whose last commit did not succeed — closes BEST-EFFORT: the error is
//! deliberately swallowed, because the run's REAL error must not be
//! masked by a cleanup failure on the way out. The lease (or whatever a
//! destination's close releases) protects CONCURRENT sessions, not dead
//! ones: once this session writes no more, holding it protects nothing,
//! and the next session's own reclaim runs under ITS OWN lease
//! regardless.

use rdlt_connector::destination::LoadSession;

use rdlt_core::error::Error;

/// Close a session whose last commit SUCCEEDED — the orderly end. The
/// error still propagates, never swallowed: an operator should know
/// close failed even though the run itself did not.
pub(crate) async fn strict(session: &mut dyn LoadSession) -> Result<(), Error> {
    session.close().await.map_err(|e| {
        Error::destination(format!(
            "session close failed AFTER all commits were durable (the data is committed): {e}"
        ))
    })
}

/// Close an abandoned session — best-effort, error swallowed, the
/// abandonment's own error still to be returned by the caller.
pub(crate) async fn best_effort(session: &mut dyn LoadSession) {
    let _ = session.close().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The strict close's frozen prefix and non-retryable
    /// classification, pinned at the one owner: a close failure after
    /// durable commits is `Error::Destination`, never a classified
    /// destination error, and the message says the data survived.
    #[tokio::test]
    async fn a_strict_close_failure_is_non_retryable_and_prefixed() {
        use crate::testing::FakeSession;
        let mut session = FakeSession::default();
        session.fail_close("lease release refused");
        let error = strict(&mut session)
            .await
            .expect_err("the injected close failure surfaces");
        let rendered = error.to_string();
        assert!(
            rendered.contains(
                "session close failed AFTER all commits were durable (the data is committed): ",
            ),
            "the frozen prefix says the data survived: {rendered}"
        );
        assert!(
            matches!(error, Error::Destination { .. }),
            "classified non-retryable, never through the destination's own taxonomy: {error:?}"
        );
    }

    /// The best-effort close swallows: an abandonment's cleanup failure
    /// must not mask the error the caller is about to return.
    #[tokio::test]
    async fn a_best_effort_close_swallows_its_failure() {
        use crate::testing::FakeSession;
        let mut session = FakeSession::default();
        session.fail_close("lease release refused");
        best_effort(&mut session).await;
    }
}
