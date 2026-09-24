use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use bytes::Bytes;
use serde::Serialize;

use super::Emitter;
use crate::cursor::Cursor;
use crate::error::ConnectorErrorKind;
use crate::limits::{MAX_BATCH_ROWS, MAX_COLUMNS, MAX_JSON_PUSH_BYTES};
use crate::sink::{LogLevel, PartitionFeed, Push, SourceEvent, partition_channel};

#[derive(Serialize)]
struct Row {
    id: u32,
}

fn emitter() -> (Emitter<u64>, PartitionFeed) {
    let (sink, feed) = partition_channel(NonZeroUsize::new(16).unwrap());
    (Emitter::new(sink, 7), feed)
}

fn ids(values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(values)) as _)]).unwrap()
}

async fn drain(emitter: Emitter<u64>, mut feed: PartitionFeed) -> Vec<SourceEvent> {
    drop(emitter);
    let mut events = Vec::new();
    while let Some(event) = feed.recv().await {
        events.push(event);
    }
    events
}

#[tokio::test]
async fn rows_become_one_json_array_push() {
    let (mut out, feed) = emitter();
    out.rows(&[Row { id: 1 }, Row { id: 2 }]).await.unwrap();
    out.rows::<Row>(&[]).await.unwrap();
    assert_eq!(
        drain(out, feed).await,
        vec![SourceEvent::Push(Push::Json(Bytes::from_static(
            br#"[{"id":1},{"id":2}]"#
        )))]
    );
}

#[tokio::test]
async fn empty_batches_push_nothing() {
    let (mut out, feed) = emitter();
    out.batch(ids(vec![])).await.unwrap();
    out.batch(ids(vec![1])).await.unwrap();
    assert_eq!(
        drain(out, feed).await,
        vec![SourceEvent::Push(Push::Arrow(ids(vec![1])))]
    );
}

#[tokio::test]
async fn checkpoints_encode_the_cursor_and_answer_pending_barriers() {
    let (mut out, feed) = emitter();
    out.checkpoint(&5).await.unwrap();
    feed.request_checkpoint(1);
    assert!(out.checkpoint_due());
    out.checkpoint(&6).await.unwrap();
    assert!(!out.checkpoint_due());
    let events = drain(out, feed).await;
    assert_eq!(
        events,
        vec![
            SourceEvent::Checkpoint {
                cursor: Cursor::encode(7, &5u64).unwrap(),
                answers: None
            },
            SourceEvent::Checkpoint {
                cursor: Cursor::encode(7, &6u64).unwrap(),
                answers: Some(1)
            },
        ]
    );
}

#[tokio::test]
async fn logs_and_metrics_are_forwarded() {
    let (mut out, feed) = emitter();
    out.log(LogLevel::Warn, "slow page").await.unwrap();
    out.metric("pages", 3.0).await.unwrap();
    assert_eq!(
        drain(out, feed).await,
        vec![
            SourceEvent::Log {
                level: LogLevel::Warn,
                message: "slow page".to_owned()
            },
            SourceEvent::Metric {
                name: "pages".to_owned(),
                value: 3.0
            },
        ]
    );
}

#[tokio::test]
async fn json_over_the_limit_is_refused_before_it_is_sent() {
    let (mut out, feed) = emitter();
    let too_big = Bytes::from(vec![
        b' ';
        usize::try_from(MAX_JSON_PUSH_BYTES).unwrap() + 1
    ]);
    let error = out.json(too_big).await.unwrap_err();
    assert_eq!(error.limit().unwrap().name, "JSON push bytes");
    assert!(drain(out, feed).await.is_empty());
}

#[tokio::test]
async fn change_batches_are_validated() {
    let (mut out, _feed) = emitter();
    assert_eq!(
        out.changes(ids(vec![1])).await.unwrap_err().code(),
        Some("change_batch")
    );
}

#[tokio::test]
async fn emitting_after_a_stop_request_fails_with_stopped() {
    let (mut out, feed) = emitter();
    feed.stop();
    assert_eq!(
        out.rows(&[Row { id: 1 }]).await.unwrap_err().kind(),
        ConnectorErrorKind::Stopped
    );
}

#[tokio::test]
async fn a_json_push_exactly_at_the_limit_is_accepted() {
    let (mut out, _feed) = emitter();
    let at_limit = Bytes::from(vec![b' '; usize::try_from(MAX_JSON_PUSH_BYTES).unwrap()]);
    out.json(at_limit).await.unwrap();
}

#[tokio::test]
async fn batches_over_the_row_or_column_limit_are_refused() {
    let (mut out, feed) = emitter();
    let rows = usize::try_from(MAX_BATCH_ROWS).unwrap() + 1;
    let tall = RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![0; rows])) as _)])
        .unwrap();
    assert_eq!(
        out.batch(tall).await.unwrap_err().limit().unwrap().name,
        "batch rows"
    );
    let columns = usize::try_from(MAX_COLUMNS).unwrap() + 1;
    let wide = RecordBatch::try_from_iter((0..columns).map(|index| {
        (
            format!("c{index}"),
            Arc::new(Int64Array::from(vec![1])) as _,
        )
    }))
    .unwrap();
    assert_eq!(
        out.changes(wide).await.unwrap_err().limit().unwrap().name,
        "batch columns"
    );
    assert!(drain(out, feed).await.is_empty());
}

/// A row whose serialization fails.
struct Unserializable;

impl Serialize for Unserializable {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("this row cannot be serialized"))
    }
}

#[tokio::test]
async fn rows_that_fail_to_serialize_are_a_data_error() {
    let (mut out, _feed) = emitter();
    let error = out.rows(&[Unserializable]).await.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
    assert!(error.to_string().contains("serializing rows"), "{error}");
}
