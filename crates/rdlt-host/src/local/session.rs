//! A spawned destination's sessions and writers, whose transport errors carry the connector's
//! last words as its source's and destination's own calls do.

use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::{
    BoxFuture, CommitMeta, DestinationSession, DestinationWriter, Receipt, SegmentId, TableChange,
    TableRef, WriteStats,
};

use super::supervised::Supervisor;

/// A session of a spawned destination.
pub(crate) struct SupervisedSession {
    pub(crate) inner: Box<dyn DestinationSession>,
    pub(crate) supervisor: Arc<Supervisor>,
}

impl DestinationSession for SupervisedSession {
    fn apply_schema<'a>(
        &'a mut self,
        change: &'a TableChange,
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let result = self.inner.apply_schema(change).await;
            self.supervisor.explain(result).await
        })
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, rdlt_connector::Result<Box<dyn DestinationWriter>>> {
        Box::pin(async move {
            let result = self.inner.writer(table).await;
            let inner = self.supervisor.explain(result).await?;
            Ok(Box::new(SupervisedWriter {
                inner,
                supervisor: Arc::clone(&self.supervisor),
            }) as Box<dyn DestinationWriter>)
        })
    }

    fn commit<'a>(
        &'a mut self,
        meta: &'a CommitMeta,
    ) -> BoxFuture<'a, rdlt_connector::Result<Receipt>> {
        Box::pin(async move {
            let result = self.inner.commit(meta).await;
            self.supervisor.explain(result).await
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, rdlt_connector::Result<()>> {
        let Self { inner, supervisor } = *self;
        Box::pin(async move {
            let result = inner.close().await;
            supervisor.explain(result).await
        })
    }
}

/// A writer of a spawned destination.
struct SupervisedWriter {
    inner: Box<dyn DestinationWriter>,
    supervisor: Arc<Supervisor>,
}

impl DestinationWriter for SupervisedWriter {
    fn write(
        &mut self,
        segment: SegmentId,
        batch: RecordBatch,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let result = self.inner.write(segment, batch).await;
            self.supervisor.explain(result).await
        })
    }

    fn flush(&mut self) -> BoxFuture<'_, rdlt_connector::Result<WriteStats>> {
        Box::pin(async move {
            let result = self.inner.flush().await;
            self.supervisor.explain(result).await
        })
    }
}
