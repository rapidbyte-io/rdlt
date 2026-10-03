//! A destination that calls a test's hook once, at a chosen point of a commit.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::RecordBatch;
use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, ConnectorErrorKind, Destination, DestinationSession,
    DestinationWriter, OpenContext, OpenedSession, Receipt, Result, SegmentId, TableChange,
    TableRef, WriteStats,
};

/// When a [`Hooked`] destination calls its hook.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum At {
    /// Before the first flush of a writer: before a commit is logged or reported.
    Flush,
    /// Once the first commit has landed, before it is reported.
    Landed,
    /// At every close of a session, which then fails.
    Close,
}

pub(crate) type Hook = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

/// `inner`, calling `hook` once, at `at`.
struct Hooked {
    inner: Arc<dyn Destination>,
    at: At,
    hook: Hook,
    called: Arc<AtomicBool>,
}

/// `inner`, calling `hook` at `at`.
pub(crate) fn hooked(inner: Arc<dyn Destination>, at: At, hook: Hook) -> Arc<dyn Destination> {
    Arc::new(Hooked {
        inner,
        at,
        hook,
        called: Arc::new(AtomicBool::new(false)),
    })
}

impl Destination for Hooked {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let opened = self.inner.open(context).await?;
            let session = HookedSession {
                inner: opened.session,
                at: self.at,
                hook: Arc::clone(&self.hook),
                called: Arc::clone(&self.called),
            };
            Ok(OpenedSession {
                session: Box::new(session),
                ..opened
            })
        })
    }
}

struct HookedSession {
    inner: Box<dyn DestinationSession>,
    at: At,
    hook: Hook,
    called: Arc<AtomicBool>,
}

/// Calls `hook` the first time it is reached.
async fn once(called: &AtomicBool, hook: &Hook) {
    if !called.swap(true, Ordering::SeqCst) {
        hook().await;
    }
}

impl DestinationSession for HookedSession {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        self.inner.apply_schema(change)
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        Box::pin(async move {
            let inner = self.inner.writer(table).await?;
            let flushing = (self.at == At::Flush).then(|| Arc::clone(&self.hook));
            let writer = HookedWriter {
                inner,
                hook: flushing,
                called: Arc::clone(&self.called),
            };
            Ok(Box::new(writer) as Box<dyn DestinationWriter>)
        })
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(async move {
            let receipt = self.inner.commit(meta).await?;
            if self.at == At::Landed {
                once(&self.called, &self.hook).await;
            }
            Ok(receipt)
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            self.inner.close().await?;
            if self.at == At::Close {
                (self.hook)().await;
                let message = "the session did not close";
                return Err(rdlt_connector::ConnectorError::new(
                    ConnectorErrorKind::Transient,
                    message,
                ));
            }
            Ok(())
        })
    }
}

struct HookedWriter {
    inner: Box<dyn DestinationWriter>,
    hook: Option<Hook>,
    called: Arc<AtomicBool>,
}

impl DestinationWriter for HookedWriter {
    fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        self.inner.write(segment, batch)
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        Box::pin(async move {
            if let Some(hook) = &self.hook {
                once(&self.called, hook).await;
            }
            self.inner.flush().await
        })
    }
}
