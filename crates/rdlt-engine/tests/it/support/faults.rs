//! A destination whose commits fail where a test's rule says, before or after they land.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::{
    BoxFuture, Capabilities, CommitKey, CommitMeta, ConnectorError, ConnectorErrorKind,
    Destination, DestinationSession, DestinationWriter, OpenContext, OpenedSession, Receipt,
    Result, TableChange, TableRef,
};

/// How a try of a commit fails.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Fault {
    /// The try lands.
    None,
    /// The try fails before it lands.
    Before(Failure),
    /// The try lands, then fails.
    After(Failure),
    /// The try lands, then answers once the wait has passed.
    Late(Duration),
}

/// What a try that fails answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Failure {
    /// The destination's transient error.
    Transient,
    /// The destination's rate limit, asking for the wait.
    Limited(Duration),
    /// A transient error with the code a host gives its own finding about a connection, as a
    /// connection lost or the host's deadline does: the answer is lost.
    Host(&'static str),
    /// No answer, until the engine's wait ends the call.
    Hang,
}

/// Which fault the `n`th try of a commit (from 1, across sessions) with `meta` meets.
pub(crate) type Rule = dyn Fn(usize, &CommitMeta) -> Fault + Send + Sync;

/// The rule that faults the first try of each commit as `fault` says of its commit, and lands
/// every try sent again by the same key.
pub(crate) fn first_tries(
    fault: impl Fn(&CommitMeta) -> Fault + Send + Sync + 'static,
) -> Arc<Rule> {
    let tried: Mutex<BTreeSet<CommitKey>> = Mutex::default();
    Arc::new(move |_, meta: &CommitMeta| {
        if tried.lock().insert(meta.key) {
            fault(meta)
        } else {
            Fault::None
        }
    })
}

/// `inner`, whose commits fail as `rule` says.
pub(crate) fn failing_commits(
    inner: Arc<dyn Destination>,
    rule: Arc<Rule>,
) -> Arc<dyn Destination> {
    Arc::new(Faulty {
        inner,
        rule,
        commits: Arc::new(AtomicUsize::new(0)),
    })
}

struct Faulty {
    inner: Arc<dyn Destination>,
    rule: Arc<Rule>,
    commits: Arc<AtomicUsize>,
}

impl Destination for Faulty {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let opened = self.inner.open(context).await?;
            Ok(OpenedSession {
                session: Box::new(FaultySession {
                    inner: opened.session,
                    rule: Arc::clone(&self.rule),
                    commits: Arc::clone(&self.commits),
                }),
                ..opened
            })
        })
    }
}

struct FaultySession {
    inner: Box<dyn DestinationSession>,
    rule: Arc<Rule>,
    commits: Arc<AtomicUsize>,
}

impl Failure {
    /// What a try that fails so answers; one that hangs never does.
    async fn answered(self) -> ConnectorError {
        match self {
            Self::Transient => ConnectorError::new(ConnectorErrorKind::Transient, "injected"),
            Self::Limited(after) => ConnectorError::rate_limited("injected", Some(after)),
            Self::Host(code) => {
                ConnectorError::new(ConnectorErrorKind::Transient, "injected").with_code(code)
            }
            Self::Hang => std::future::pending().await,
        }
    }
}

impl DestinationSession for FaultySession {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        self.inner.apply_schema(change)
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        self.inner.writer(table)
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        let n = self.commits.fetch_add(1, Ordering::SeqCst) + 1;
        let fault = (self.rule)(n, meta);
        Box::pin(async move {
            if let Fault::Before(failure) = fault {
                return Err(failure.answered().await);
            }
            let receipt = self.inner.commit(meta).await?;
            match fault {
                Fault::After(failure) => Err(failure.answered().await),
                Fault::Late(wait) => {
                    tokio::time::sleep(wait).await;
                    Ok(receipt)
                }
                Fault::None | Fault::Before(_) => Ok(receipt),
            }
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.inner.close()
    }
}
