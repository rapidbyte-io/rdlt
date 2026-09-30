//! A destination whose commits fail where a test's rule says, before or after they land.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, ConnectorError, ConnectorErrorKind, Destination,
    DestinationSession, DestinationWriter, OpenContext, OpenedSession, Receipt, Result,
    StateChange, StateEntry, TableChange, TableRef,
};

/// How a commit fails.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Fault {
    /// The commit lands.
    None,
    /// The commit fails before it lands.
    Before,
    /// The commit lands, but its response is lost.
    After,
}

/// Which fault the `n`th commit (from 1, across sessions) with `meta` meets.
pub(crate) type Rule = dyn Fn(usize, &CommitMeta) -> Fault + Send + Sync;

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

/// The phase a commit with `meta` begins, if it begins one.
pub(crate) fn begins(meta: &CommitMeta) -> Option<u16> {
    meta.state_delta.iter().find_map(|change| match change {
        StateChange::Put(record) => match StateEntry::from_record(record) {
            Ok(StateEntry::Phase { phase, .. }) => Some(phase),
            _ => None,
        },
        StateChange::Delete(_) => None,
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

fn injected() -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Transient, "injected")
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
            if fault == Fault::Before {
                return Err(injected());
            }
            let receipt = self.inner.commit(meta).await?;
            if fault == Fault::After {
                return Err(injected());
            }
            Ok(receipt)
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.inner.close()
    }
}
