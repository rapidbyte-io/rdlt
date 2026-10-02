//! A connector placed in process, whose errors reach the engine as a host keeps a
//! connector's: scrubbed of the secrets it was sent, shown, and bounded, causes included.

use arrow_array::RecordBatch;
use rdlt_connector::{
    BoxFuture, Capabilities, Catalog, CommitMeta, ConnectorError, Cursor, Destination,
    DestinationSession, DestinationWriter, OpenContext, OpenedSession, PartitionId, PartitionPlan,
    PartitionSink, ReadRequest, Receipt, Result, SegmentId, Source, StreamName, StreamState,
    TableChange, TableRef, WriteStats,
};

use crate::secrets::Redactions;

#[cfg(test)]
mod tests;

/// `error` as the host keeps it: nothing `redactions` holds is in its message, its code or
/// any of its causes, which are text from here on.
pub(crate) fn received(error: &ConnectorError, redactions: &Redactions) -> ConnectorError {
    error.received(&|text| redactions.scrubbed(text))
}

/// A source, a destination, a session or a writer placed in process, with the secrets it
/// was sent.
pub(crate) struct Guarded<T> {
    pub(crate) inner: T,
    pub(crate) redactions: Redactions,
}

impl<T> Guarded<T> {
    fn kept<V>(&self, result: Result<V>) -> Result<V> {
        result.map_err(|error| received(&error, &self.redactions))
    }
}

impl Source for Guarded<Box<dyn Source>> {
    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.kept(self.inner.check().await) })
    }

    fn discover(&self) -> BoxFuture<'_, Result<Catalog>> {
        Box::pin(async move { self.kept(self.inner.discover().await) })
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, Result<PartitionPlan>> {
        Box::pin(async move { self.kept(self.inner.plan(stream, state).await) })
    }

    fn read(&self, request: ReadRequest, sink: PartitionSink) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.kept(self.inner.read(request, sink).await) })
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.kept(self.inner.committed(stream, cursors).await) })
    }
}

impl Destination for Guarded<Box<dyn Destination>> {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.kept(self.inner.check().await) })
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let opened = self.kept(self.inner.open(context).await)?;
            Ok(OpenedSession {
                session: Box::new(Guarded {
                    inner: opened.session,
                    redactions: self.redactions.clone(),
                }),
                ..opened
            })
        })
    }
}

impl DestinationSession for Guarded<Box<dyn DestinationSession>> {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let applied = self.inner.apply_schema(change).await;
            self.kept(applied)
        })
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        Box::pin(async move {
            let writer = self.inner.writer(table).await;
            let inner = self.kept(writer)?;
            let redactions = self.redactions.clone();
            Ok(Box::new(Guarded { inner, redactions }) as Box<dyn DestinationWriter>)
        })
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(async move {
            let committed = self.inner.commit(meta).await;
            self.kept(committed)
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        let Self { inner, redactions } = *self;
        Box::pin(async move {
            let closed = inner.close().await;
            closed.map_err(|error| received(&error, &redactions))
        })
    }
}

impl DestinationWriter for Guarded<Box<dyn DestinationWriter>> {
    fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let written = self.inner.write(segment, batch).await;
            self.kept(written)
        })
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        Box::pin(async move {
            let flushed = self.inner.flush().await;
            self.kept(flushed)
        })
    }
}
