use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, ListArray, NullArray, RecordBatch, StructArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Fields};
use bytes::Bytes;
use serde::Serialize;

use super::Emitter;
use crate::cursor::Cursor;
use crate::error::ConnectorErrorKind;
use crate::limits::{
    MAX_BATCH_BYTES, MAX_BATCH_ROWS, MAX_BATCH_VALUES, MAX_COLUMNS, MAX_JSON_PUSH_BYTES,
    MAX_VIEW_BYTES,
};
use crate::sink::{LogLevel, PartitionFeed, Push, SourceEvent, partition_channel};

#[derive(Serialize)]
struct Row {
    id: u32,
}

fn emitter() -> (Emitter<u64>, PartitionFeed) {
    let (sink, feed) = partition_channel(NonZeroUsize::new(16).unwrap());
    (Emitter::new(sink, 7, false), feed)
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
async fn rows_holding_raw_json_push_it_as_json() {
    #[derive(Serialize)]
    struct Document<'a> {
        id: u8,
        doc: &'a serde_json::value::RawValue,
    }
    let raw = serde_json::value::RawValue::from_string(r#"{"a":[1,2]}"#.to_owned()).unwrap();
    let (mut out, feed) = emitter();
    out.rows(&[Document { id: 1, doc: &raw }]).await.unwrap();
    assert_eq!(
        drain(out, feed).await,
        vec![SourceEvent::Push(Push::Json(Bytes::from_static(
            br#"[{"id":1,"doc":{"a":[1,2]}}]"#
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
async fn replan_and_lag_signals_are_forwarded() {
    let (mut out, feed) = emitter();
    out.behind(12).await.unwrap();
    out.replan().await.unwrap();
    out.behind(0).await.unwrap();
    assert_eq!(
        drain(out, feed).await,
        vec![
            SourceEvent::Behind { records: 12 },
            SourceEvent::Replan,
            SourceEvent::Behind { records: 0 },
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

/// A batch of one column nested `levels` deep, a top-level column being the first level.
fn nested(levels: usize) -> RecordBatch {
    let mut inner: ArrayRef = Arc::new(Int64Array::from(vec![1_i64]));
    for _ in 1..levels {
        let fields = Fields::from(vec![Field::new("a", inner.data_type().clone(), true)]);
        inner = Arc::new(StructArray::new(fields, vec![inner], None));
    }
    RecordBatch::try_from_iter([("doc", inner)]).unwrap()
}

#[tokio::test]
async fn a_batch_nested_deeper_than_a_frame_may_be_is_refused() {
    let (mut out, feed) = emitter();
    out.batch(nested(64)).await.unwrap();
    // Deep enough to overflow the stack of anything that walked it a level a call.
    for levels in [65, 3_000] {
        let error = out.batch(nested(levels)).await.unwrap_err();
        assert_eq!(error.code(), Some("limit_exceeded"));
        assert_eq!(error.limit().unwrap().name, "nesting depth");
        let error = out.changes(nested(levels)).await.unwrap_err();
        assert_eq!(error.limit().unwrap().name, "nesting depth");
    }
    assert_eq!(drain(out, feed).await.len(), 1);
}

#[tokio::test]
async fn a_batch_of_more_values_than_a_frame_may_hold_is_refused() {
    let (mut out, feed) = emitter();
    // One row of two billion nulls takes no bytes.
    let items = 2_000_000_000_i32;
    let nulls = ListArray::new(
        Arc::new(Field::new("item", DataType::Null, true)),
        OffsetBuffer::new(vec![0, items].into()),
        Arc::new(NullArray::new(usize::try_from(items).unwrap())),
        None,
    );
    let batch = RecordBatch::try_from_iter([("bomb", Arc::new(nulls) as ArrayRef)]).unwrap();
    let limit = out.batch(batch).await.unwrap_err().limit().unwrap();
    assert_eq!(
        (limit.name, limit.limit),
        ("batch values", MAX_BATCH_VALUES)
    );
    assert!(drain(out, feed).await.is_empty());
}

#[tokio::test]
async fn a_served_reads_batch_beyond_a_frame_is_sent_on_to_be_cut() {
    let (sink, feed) = partition_channel(NonZeroUsize::new(16).unwrap());
    let mut out: Emitter<u64> = Emitter::new(sink.cut(), 7, false);
    // Three rows of a buffer beyond a frame keep all of it alive: the cut copies what they name.
    let bytes = usize::try_from(MAX_BATCH_BYTES).unwrap() + 1;
    let whole = arrow_array::UInt8Array::from(vec![0_u8; bytes]);
    let slice: ArrayRef = Arc::new(whole.slice(0, 3));
    let batch = RecordBatch::try_from_iter([("pinned", slice)]).unwrap();
    out.batch(batch).await.unwrap();
    assert_eq!(drain(out, feed).await.len(), 1);
}

#[tokio::test]
async fn a_batch_keeping_more_alive_than_a_frame_may_hold_is_refused() {
    let (mut out, feed) = emitter();
    let bytes = usize::try_from(MAX_BATCH_BYTES).unwrap() + 1;
    // Three rows of a buffer keep all of it alive.
    let whole = arrow_array::UInt8Array::from(vec![0_u8; bytes]);
    let slice: ArrayRef = Arc::new(whole.slice(0, 3));
    let batch = RecordBatch::try_from_iter([("pinned", slice)]).unwrap();
    let limit = out.batch(batch).await.unwrap_err().limit().unwrap();
    assert_eq!((limit.name, limit.limit), ("batch bytes", MAX_BATCH_BYTES));
    let mut views = arrow_array::builder::BinaryViewBuilder::new();
    let block = views.append_block(vec![7_u8; 1 << 20].into());
    for _ in 0..65 {
        views.try_append_view(block, 0, 1 << 20).unwrap();
    }
    let batch =
        RecordBatch::try_from_iter([("views", Arc::new(views.finish()) as ArrayRef)]).unwrap();
    let limit = out.batch(batch).await.unwrap_err().limit().unwrap();
    assert_eq!((limit.name, limit.limit), ("view bytes", MAX_VIEW_BYTES));
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

#[tokio::test(start_paused = true)]
async fn stopped_resolves_once_the_engine_asks_the_read_to_stop_or_goes() {
    let within = std::time::Duration::from_secs(1);
    let (out, feed) = emitter();
    let waiting = tokio::time::timeout(within, out.stopped()).await;
    assert!(waiting.is_err(), "nothing asked the read to stop yet");
    feed.stop();
    tokio::time::timeout(within, out.stopped())
        .await
        .expect("asked to stop");
    let (out, feed) = emitter();
    drop(feed);
    tokio::time::timeout(within, out.stopped())
        .await
        .expect("the engine went");
}
