//! A connector placed in process, whose errors and panics reach the engine as a host keeps a
//! connector's: scrubbed of the secrets it was sent, shown, and bounded, causes included.

use std::any::Any;
use std::future::{Future, poll_fn};
use std::num::NonZeroUsize;
use std::panic::{self, AssertUnwindSafe};
use std::pin::pin;

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

/// What `call` answers; should it panic, the panic unwinds on with nothing `redactions` holds
/// in its message, which whatever catches it may show.
pub(crate) async fn unwound<F: Future>(call: F, redactions: &Redactions) -> F::Output {
    let mut call = pin!(call);
    poll_fn(|context| {
        let polled = panic::catch_unwind(AssertUnwindSafe(|| call.as_mut().poll(context)));
        polled.unwrap_or_else(|payload| panic::resume_unwind(scrubbed(payload, redactions)))
    })
    .await
}

/// A panic's `payload`, its message scrubbed where it is text; any other payload says nothing.
fn scrubbed(payload: Box<dyn Any + Send>, redactions: &Redactions) -> Box<dyn Any + Send> {
    let message = match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_owned(),
            Err(payload) => return payload,
        },
    };
    Box::new(redactions.scrubbed(message))
}

/// What `call` answers, as the host keeps a connector's answer: its error and its panic
/// scrubbed of what `redactions` holds.
///
/// Each call is made in the future it is given as, so that what a connector does before its own
/// future is made is caught too.
async fn answered<V>(call: impl Future<Output = Result<V>>, redactions: &Redactions) -> Result<V> {
    let answer = unwound(call, redactions).await;
    answer.map_err(|error| received(&error, redactions))
}

/// A source, a destination, a session or a table writer placed in process, with the secrets it
/// was sent.
pub(crate) struct Guarded<T> {
    pub(crate) inner: T,
    pub(crate) redactions: Redactions,
}

impl Source for Guarded<Box<dyn Source>> {
    fn check(&self) -> BoxFuture<'_, Result<()>> {
        let Self { inner, redactions } = self;
        Box::pin(answered(async move { inner.check().await }, redactions))
    }

    fn discover(&self) -> BoxFuture<'_, Result<Catalog>> {
        let Self { inner, redactions } = self;
        Box::pin(answered(async move { inner.discover().await }, redactions))
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, Result<PartitionPlan>> {
        let Self { inner, redactions } = self;
        Box::pin(answered(
            async move { inner.plan(stream, state).await },
            redactions,
        ))
    }

    fn read(&self, request: ReadRequest, sink: PartitionSink) -> BoxFuture<'_, Result<()>> {
        let Self { inner, redactions } = self;
        Box::pin(answered(
            async move { inner.read(request, sink).await },
            redactions,
        ))
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        let Self { inner, redactions } = self;
        Box::pin(answered(
            async move { inner.committed(stream, cursors).await },
            redactions,
        ))
    }

    fn max_calls(&self) -> Option<NonZeroUsize> {
        self.inner.max_calls()
    }
}

impl Destination for Guarded<Box<dyn Destination>> {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        let Self { inner, redactions } = self;
        Box::pin(answered(async move { inner.check().await }, redactions))
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        let Self { inner, redactions } = self;
        Box::pin(async move {
            let opened = answered(async move { inner.open(context).await }, redactions).await?;
            Ok(OpenedSession {
                session: Box::new(Guarded {
                    inner: opened.session,
                    redactions: redactions.clone(),
                }),
                ..opened
            })
        })
    }

    fn max_calls(&self) -> Option<NonZeroUsize> {
        self.inner.max_calls()
    }
}

impl DestinationSession for Guarded<Box<dyn DestinationSession>> {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        let Self { inner, redactions } = self;
        Box::pin(answered(
            async move { inner.apply_schema(change).await },
            redactions,
        ))
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        let Self { inner, redactions } = self;
        Box::pin(async move {
            let writer = answered(async move { inner.writer(table).await }, redactions).await?;
            let redactions = redactions.clone();
            Ok(Box::new(Guarded {
                inner: writer,
                redactions,
            }) as Box<dyn DestinationWriter>)
        })
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        let Self { inner, redactions } = self;
        Box::pin(answered(
            async move { inner.commit(meta).await },
            redactions,
        ))
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        let Self { inner, redactions } = *self;
        Box::pin(async move { answered(async move { inner.close().await }, &redactions).await })
    }
}

impl DestinationWriter for Guarded<Box<dyn DestinationWriter>> {
    fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        let Self { inner, redactions } = self;
        Box::pin(answered(
            async move { inner.write(segment, batch).await },
            redactions,
        ))
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        let Self { inner, redactions } = self;
        Box::pin(answered(async move { inner.flush().await }, redactions))
    }
}
