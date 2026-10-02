//! Every call the engine makes into a connector, but a read, ended at the engine's wait for it;
//! and every call, reads too, made [charging](rdlt_wire::bounded::charging) what decoding its
//! answers holds to the run's memory budget, where the connector is remote.
//!
//! A read may be silent for as long as its source has nothing to send; any other call that does
//! not return keeps a run from ending, whether the connector is served out of process, where its
//! host's deadlines bound it too, or runs in the engine's own.

#[cfg(test)]
mod tests;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use rdlt_connector::{
    BoxFuture, Capabilities, Catalog, CommitMeta, ConnectorError, ConnectorErrorKind, Cursor,
    DEADLINE_EXCEEDED, Destination, DestinationSession, DestinationWriter, OpenContext,
    OpenedSession, PartitionId, PartitionPlan, PartitionSink, ReadRequest, Receipt, Result,
    SegmentId, Source, StreamName, StreamState, TableChange, TableRef, WriteStats,
};

use rdlt_wire::bounded::{Charge, charging};

use crate::budget::{Decoding, MemoryBudget};
use crate::env::Env;

/// The engine's wait for a call into a connector, on its clock, and what decoding its answers
/// is charged to.
#[derive(Clone)]
pub(crate) struct Waits {
    env: Arc<dyn Env>,
    wait: Duration,
    charge: Option<Arc<dyn Charge>>,
}

impl Waits {
    pub(crate) fn new(env: Arc<dyn Env>, wait: Duration) -> Self {
        Self {
            env,
            wait,
            charge: None,
        }
    }

    /// The waits, what decoding each call's answers holds charged to `budget`.
    pub(crate) fn charging(mut self, budget: &MemoryBudget) -> Self {
        self.charge = Some(Arc::new(Decoding(budget.clone())));
        self
    }

    /// `call`, what decoding its answers holds charged where the waits charge it.
    async fn charged<T>(&self, call: impl Future<Output = T>) -> T {
        match &self.charge {
            Some(charge) => charging(Arc::clone(charge), call).await,
            None => call.await,
        }
    }

    /// `source`, its calls but reads ended at the wait.
    pub(crate) fn source(&self, source: Arc<dyn Source>) -> Arc<dyn Source> {
        Arc::new(WaitedSource {
            inner: source,
            waits: self.clone(),
        })
    }

    /// `destination`, each of its calls ended at the wait, and those of its sessions and
    /// writers.
    pub(crate) fn destination(&self, destination: Arc<dyn Destination>) -> Arc<dyn Destination> {
        Arc::new(WaitedDestination {
            inner: destination,
            waits: self.clone(),
        })
    }

    /// What `call`, the connector's `what`, answers, unless the wait passes first.
    async fn within<T>(&self, what: &str, call: BoxFuture<'_, Result<T>>) -> Result<T> {
        let wait = self.wait;
        tokio::select! {
            biased;
            // An answer and the wait's end together: the answer wins.
            answer = self.charged(call) => answer,
            () = self.env.sleep(wait) => Err(ConnectorError::new(
                ConnectorErrorKind::Transient,
                format!("{what} took longer than the engine's wait of {wait:?}"),
            )
            .with_code(DEADLINE_EXCEEDED)),
        }
    }
}

struct WaitedSource {
    inner: Arc<dyn Source>,
    waits: Waits,
}

impl Source for WaitedSource {
    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.waits.within("the check", self.inner.check()))
    }

    fn discover(&self) -> BoxFuture<'_, Result<Catalog>> {
        Box::pin(self.waits.within("the discovery", self.inner.discover()))
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, Result<PartitionPlan>> {
        Box::pin(
            self.waits
                .within("the plan", self.inner.plan(stream, state)),
        )
    }

    fn read(&self, request: ReadRequest, sink: PartitionSink) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.waits.charged(self.inner.read(request, sink)))
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        let call = self.inner.committed(stream, cursors);
        Box::pin(self.waits.within("the committed report", call))
    }
}

struct WaitedDestination {
    inner: Arc<dyn Destination>,
    waits: Waits,
}

impl Destination for WaitedDestination {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.waits.within("the check", self.inner.check()))
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let opened = self
                .waits
                .within("the open", self.inner.open(context))
                .await?;
            Ok(OpenedSession {
                session: Box::new(WaitedSession {
                    inner: opened.session,
                    waits: self.waits.clone(),
                }),
                ..opened
            })
        })
    }
}

struct WaitedSession {
    inner: Box<dyn DestinationSession>,
    waits: Waits,
}

impl DestinationSession for WaitedSession {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        let call = self.inner.apply_schema(change);
        Box::pin(self.waits.within("the schema change", call))
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        let waits = self.waits.clone();
        let call = self.inner.writer(table);
        Box::pin(async move {
            let writer = waits.within("opening a writer", call).await?;
            Ok(Box::new(WaitedWriter {
                inner: writer,
                waits,
            }) as Box<dyn DestinationWriter>)
        })
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(self.waits.within("the commit", self.inner.commit(meta)))
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        let Self { inner, waits } = *self;
        Box::pin(async move { waits.within("the close", inner.close()).await })
    }
}

struct WaitedWriter {
    inner: Box<dyn DestinationWriter>,
    waits: Waits,
}

impl DestinationWriter for WaitedWriter {
    fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        Box::pin(
            self.waits
                .within("the write", self.inner.write(segment, batch)),
        )
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        Box::pin(self.waits.within("the flush", self.inner.flush()))
    }
}
