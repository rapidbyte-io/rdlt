//! Unbounded partitions, as a change stream's changes: a read that ends only pauses one, so it is
//! never done, and rows it pushed after its last checkpoint are read again from there.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rdlt_connector::{
    BoxFuture, Catalog, Cursor, PartitionId, PartitionPlan, PartitionSink, ReadMode, ReadRequest,
    Result, Source, SourceEvent, StreamName, StreamState, partition_channel,
};
use rdlt_engine::{RunStatus, WriteMode};

use crate::changes::{changes, log, logged, orders};
use crate::support::{commit_every, engine, memory, pipeline, stream};

/// A source whose first read of an unbounded partition withholds its last checkpoint, as a source
/// that stops between pushing rows and checkpointing them does.
struct Trailing {
    inner: Arc<dyn Source>,
    trailing: AtomicBool,
}

impl Source for Trailing {
    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.check()
    }

    fn discover(&self) -> BoxFuture<'_, Result<Catalog>> {
        self.inner.discover()
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, Result<PartitionPlan>> {
        self.inner.plan(stream, state)
    }

    fn read(&self, request: ReadRequest, mut sink: PartitionSink) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            if !request.partition.is_unbounded() || !self.trailing.swap(false, Ordering::SeqCst) {
                return self.inner.read(request, sink).await;
            }
            let (inner, mut feed) = partition_channel(NonZeroUsize::new(1_024).expect("not zero"));
            let forwarding = async {
                let mut held = None;
                while let Some(event) = feed.recv().await {
                    if let Some(checkpoint) = held.take() {
                        sink.send(checkpoint).await?;
                    }
                    match event {
                        SourceEvent::Checkpoint { .. } => held = Some(event),
                        other => sink.send(other).await?,
                    }
                }
                Ok(())
            };
            let (read, forwarded) = tokio::join!(self.inner.read(request, inner), forwarding);
            read.and(forwarded)
        })
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        self.inner.committed(stream, cursors)
    }
}

#[tokio::test]
async fn rows_pushed_after_the_last_checkpoint_are_read_again_and_later_changes_too() {
    let store = "unbounded_trailing";
    let plan = || {
        pipeline(
            "unbounded",
            [stream("orders")
                .read(ReadMode::Cdc)
                .write(WriteMode::Append)],
        )
    };
    let mut first = orders(&[]);
    first.changes = 100;
    let trailing = Arc::new(Trailing {
        inner: changes(16, &first).await,
        trailing: AtomicBool::new(true),
    });
    let outcome = engine(commit_every(16))
        .run(plan(), trailing, memory(store).await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    // The source holds more changes by the next run, which reads them after the rows it pushed
    // without a checkpoint: each change lands once.
    let mut later = orders(&[]);
    later.changes = 150;
    let outcome = engine(commit_every(16))
        .run(plan(), changes(16, &later).await, memory(store).await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(logged(store, "orders"), log(16, &later));
}
