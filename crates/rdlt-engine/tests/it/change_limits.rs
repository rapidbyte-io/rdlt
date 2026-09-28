//! What a change stream needs of its source and destination: pushes of changes only, and a
//! destination that keeps what its merges ask.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use rdlt_connector::{
    BoxFuture, Catalog, Cursor, PartitionId, PartitionPlan, PartitionSink, Push, ReadMode,
    ReadRequest, Result, Source, SourceEvent, StreamName, StreamState,
};
use rdlt_connector_reference::changes::{Change, ChangedStream, change};
use rdlt_connector_reference::published;
use rdlt_engine::{
    DeleteMode, ErrorKind, Nested, OnTruncate, RunStatus, SchemaSettings, StreamPlan, WriteMode,
};

use crate::changes::{changes, orders};
use crate::support::destinations::limited;
use crate::support::{commit_every, engine, generator, memory, pipeline, stream};

/// A source that pushes `push` before each partition it reads.
struct Pushing {
    inner: Arc<dyn Source>,
    push: Push,
}

impl Source for Pushing {
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
            sink.send(SourceEvent::Push(self.push.clone())).await?;
            self.inner.read(request, sink).await
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

fn ids() -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    RecordBatch::try_from_iter([("id", ids)]).expect("a valid batch")
}

/// Runs `plan` from `source` into a memory destination `limit` narrows, into `store`.
async fn run_limited(
    plan: StreamPlan,
    source: Arc<dyn Source>,
    store: &str,
    limit: impl FnOnce(&mut rdlt_connector::Capabilities),
) -> rdlt_engine::RunOutcome {
    let destination = limited(memory(store).await, limit);
    engine(commit_every(16))
        .run(pipeline("changes", [plan]), source, destination)
        .await
}

/// Runs `plan` of the change source (seed 9) into a memory destination `limit` narrows, and
/// returns the error the run failed with.
async fn refused(
    plan: StreamPlan,
    limit: impl FnOnce(&mut rdlt_connector::Capabilities),
) -> rdlt_engine::Error {
    let source = changes(9, &orders(&[])).await;
    let outcome = run_limited(plan, source, "changes_refused", limit).await;
    assert_eq!(outcome.report.status, RunStatus::Failed);
    outcome.error.expect("the run failed")
}

#[tokio::test]
async fn a_destination_that_cannot_remove_rows_as_asked_is_refused_before_any_row() {
    let merge = || stream("orders").read(ReadMode::Cdc).write(WriteMode::Merge);
    let hard = refused(merge(), |capabilities| {
        capabilities.delete_modes.hard = false;
    })
    .await;
    assert_eq!(hard.code(), Some("delete_mode_unsupported"), "{hard}");
    let soft = refused(merge().deletes(DeleteMode::Soft), |capabilities| {
        capabilities.delete_modes.soft = false;
    })
    .await;
    assert_eq!(soft.code(), Some("delete_mode_unsupported"), "{soft}");
    // Ignored deletes remove nothing, but applied truncates remove rows outright.
    let truncated = refused(merge().deletes(DeleteMode::Ignore), |capabilities| {
        capabilities.delete_modes.hard = false;
    })
    .await;
    assert_eq!(truncated.code(), Some("delete_mode_unsupported"));
    assert_eq!(published("changes_refused", "orders").len(), 0);
}

#[tokio::test]
async fn a_destination_declaring_no_delete_mode_merges_no_change_stream() {
    // Ignoring every delete and truncate still needs the seq guard and the change columns, which
    // a destination declaring no delete mode does not know.
    let plan = stream("orders")
        .read(ReadMode::Cdc)
        .write(WriteMode::Merge)
        .deletes(DeleteMode::Ignore)
        .on_truncate(OnTruncate::Ignore);
    let error = refused(plan, |capabilities| {
        capabilities.delete_modes = rdlt_connector::DeleteModes::default();
    })
    .await;
    assert_eq!(error.code(), Some("delete_mode_unsupported"), "{error}");
    assert_eq!(published("changes_refused", "orders").len(), 0);
}

#[tokio::test]
async fn updates_leaving_columns_unchanged_are_refused_where_the_destination_cannot_keep_them() {
    let plan = stream("orders").read(ReadMode::Cdc).write(WriteMode::Merge);
    let error = refused(plan, |capabilities| capabilities.partial_updates = false).await;
    assert_eq!(error.kind(), ErrorKind::Config);
    assert_eq!(error.code(), Some("partial_updates_unsupported"), "{error}");
}

#[tokio::test]
async fn a_change_stream_cannot_normalize_yet() {
    let plan = stream("orders")
        .read(ReadMode::Cdc)
        .schema(SchemaSettings::new().nested(Nested::Normalize { max_depth: 2 }));
    let error = refused(plan, |_| {}).await;
    assert_eq!(
        error.code(),
        Some("normalize_changes_unsupported"),
        "{error}"
    );
}

#[tokio::test]
async fn a_destination_that_only_soft_deletes_merges_a_stream_ignoring_every_removal() {
    let plan = stream("orders")
        .read(ReadMode::Cdc)
        .write(WriteMode::Merge)
        .deletes(DeleteMode::Ignore)
        .on_truncate(OnTruncate::Ignore);
    let source = changes(9, &orders(&[50])).await;
    let outcome = run_limited(plan, source, "changes_soft_only", |capabilities| {
        capabilities.delete_modes.hard = false;
    })
    .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
}

#[tokio::test]
async fn a_destination_that_cannot_keep_columns_merges_changes_that_keep_none() {
    // The first seed whose changes all set every column.
    let mut stream_spec: ChangedStream = orders(&[]);
    stream_spec.changes = 12;
    let seed = (0..1_000)
        .find(|seed| {
            (1..=stream_spec.changes).all(|position| {
                !matches!(
                    change(*seed, &stream_spec, position),
                    Change::Upsert { value: None, .. }
                )
            })
        })
        .expect("a seed without partial updates");
    let plan = stream("orders").read(ReadMode::Cdc).write(WriteMode::Merge);
    let source = changes(seed, &stream_spec).await;
    let outcome = run_limited(plan, source, "changes_whole", |capabilities| {
        capabilities.partial_updates = false;
    })
    .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
}

#[tokio::test]
async fn a_change_stream_pushing_rows_fails_as_the_source_s_error() {
    let source = Arc::new(Pushing {
        inner: changes(9, &orders(&[])).await,
        push: Push::Arrow(ids()),
    });
    let plan = stream("orders").read(ReadMode::Cdc).write(WriteMode::Merge);
    let outcome = run_limited(plan, source, "changes_pushing_rows", |_| {}).await;
    assert_eq!(outcome.report.status, RunStatus::Failed);
    let error = outcome.error.expect("the run failed");
    assert_eq!(error.kind(), ErrorKind::Source, "{error}");
    assert_eq!(error.code(), Some("push_unexpected"), "{error}");
}

#[tokio::test]
async fn a_stream_not_read_as_changes_pushing_changes_fails_as_the_source_s_error() {
    let source = Arc::new(Pushing {
        inner: generator(&[("orders", 10, 1, 5)]).await,
        push: Push::Changes(ids()),
    });
    let outcome = run_limited(stream("orders"), source, "changes_pushed_changes", |_| {}).await;
    assert_eq!(outcome.report.status, RunStatus::Failed);
    let error = outcome.error.expect("the run failed");
    assert_eq!(error.kind(), ErrorKind::Source, "{error}");
    assert_eq!(error.code(), Some("push_unexpected"), "{error}");
}
