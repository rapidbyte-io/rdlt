//! A spawned destination's sessions and writers, whose transport errors carry the connector's
//! last words as its source's and destination's own calls do.

use arrow_array::RecordBatch;
use rdlt_connector::{
    BoxFuture, CommitMeta, DestinationSession, DestinationWriter, Receipt, SegmentId, TableChange,
    TableRef, WriteStats,
};

use super::Words;

/// A session of a spawned destination.
pub(crate) struct SupervisedSession {
    pub(crate) inner: Box<dyn DestinationSession>,
    pub(crate) words: Words,
}

impl DestinationSession for SupervisedSession {
    fn apply_schema<'a>(
        &'a mut self,
        change: &'a TableChange,
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let result = self.inner.apply_schema(change).await;
            self.words.explain(result).await
        })
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, rdlt_connector::Result<Box<dyn DestinationWriter>>> {
        Box::pin(async move {
            let result = self.inner.writer(table).await;
            let inner = self.words.explain(result).await?;
            Ok(Box::new(SupervisedWriter {
                inner,
                words: self.words.clone(),
            }) as Box<dyn DestinationWriter>)
        })
    }

    fn commit<'a>(
        &'a mut self,
        meta: &'a CommitMeta,
    ) -> BoxFuture<'a, rdlt_connector::Result<Receipt>> {
        Box::pin(async move {
            let result = self.inner.commit(meta).await;
            self.words.explain(result).await
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, rdlt_connector::Result<()>> {
        let Self { inner, words } = *self;
        Box::pin(async move {
            let result = inner.close().await;
            words.explain(result).await
        })
    }
}

/// A writer of a spawned destination.
struct SupervisedWriter {
    inner: Box<dyn DestinationWriter>,
    words: Words,
}

impl DestinationWriter for SupervisedWriter {
    fn write(
        &mut self,
        segment: SegmentId,
        batch: RecordBatch,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let result = self.inner.write(segment, batch).await;
            self.words.explain(result).await
        })
    }

    fn flush(&mut self) -> BoxFuture<'_, rdlt_connector::Result<WriteStats>> {
        Box::pin(async move {
            let result = self.inner.flush().await;
            self.words.explain(result).await
        })
    }
}
