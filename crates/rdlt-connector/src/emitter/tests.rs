use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, ListArray, NullArray, RecordBatch, StructArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Fields};
use bytes::Bytes;
use proptest::prelude::*;
use rdlt_wire::limits::{BATCH_ROWS, BATCH_VALUES, FRAME_BYTES, JSON_PUSH_BYTES, SCHEMA_COLUMNS};
use serde::Serialize;
use serde_json::Value;
use serde_json::value::RawValue;

use super::Emitter;
use crate::cursor::Cursor;
use crate::error::ConnectorErrorKind;
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
async fn rows_become_one_push_of_a_json_line_each() {
    let (mut out, feed) = emitter();
    out.rows(&[Row { id: 1 }, Row { id: 2 }]).await.unwrap();
    out.rows::<Row>(&[]).await.unwrap();
    assert_eq!(
        drain(out, feed).await,
        vec![SourceEvent::Push(Push::Json(Bytes::from_static(
            b"{\"id\":1}\n{\"id\":2}\n"
        )))]
    );
}

#[tokio::test]
async fn rows_holding_raw_json_printed_over_lines_push_it_on_their_own_line() {
    #[derive(Serialize)]
    struct Document<'a> {
        id: u8,
        doc: &'a RawValue,
    }
    let printed = "{\n  \"a\": [1,\n2],\n  \"b\": \"x\\ny\"\n}";
    let raw = RawValue::from_string(printed.to_owned()).unwrap();
    let (mut out, feed) = emitter();
    let rows = [Document { id: 1, doc: &raw }, Document { id: 2, doc: &raw }];
    out.rows(&rows).await.unwrap();
    // Its line ends become spaces; the line end escaped in its string stays escaped.
    let row = |id: u8| format!(r#"{{"id":{id},"doc":{{   "a": [1, 2],   "b": "x\ny" }}}}"#);
    let lines = format!("{}\n{}\n", row(1), row(2));
    assert_eq!(
        drain(out, feed).await,
        vec![SourceEvent::Push(Push::Json(Bytes::from(lines)))]
    );
}

#[tokio::test]
async fn rows_holding_a_row_that_is_not_an_object_push_nothing_and_are_refused() {
    let refused: [&[&str]; 5] = [
        &[r#"[{"id":1},{"id":2}]"#],
        &["7"],
        &[r#""text""#],
        &["null"],
        &[r#"{"id":1}"#, "[1]"],
    ];
    for rows in refused {
        let rows: Vec<Box<RawValue>> = rows
            .iter()
            .map(|row| RawValue::from_string((*row).to_owned()).unwrap())
            .collect();
        let (mut out, feed) = emitter();
        let error = out.rows(&rows).await.unwrap_err();
        assert_eq!(
            (error.kind(), error.code()),
            (ConnectorErrorKind::Data, Some("json_not_object")),
            "{rows:?}"
        );
        assert!(drain(out, feed).await.is_empty(), "{rows:?}");
    }
    let object = RawValue::from_string(r#"{"id":1}"#.to_owned()).unwrap();
    let (mut out, feed) = emitter();
    out.rows(&[object]).await.unwrap();
    assert_eq!(drain(out, feed).await.len(), 1);
}

/// A JSON object a few levels deep, its strings holding line ends, quotes and backslashes.
fn object() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        "[a\n\r\t\"\\\\ \u{e9}\u{2028}]{0,6}".prop_map(Value::from),
    ];
    let value = leaf.prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::btree_map("[a-c]", inner, 0..4)
                .prop_map(|fields| Value::Object(fields.into_iter().collect())),
        ]
    });
    prop::collection::btree_map("[a-d]", value, 0..4)
        .prop_map(|fields| Value::Object(fields.into_iter().collect()))
}

/// The value on each line of the push `rows` makes, whose last line must end too.
fn pushed_lines<T: Serialize>(rows: &[T]) -> Vec<Value> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let (mut out, feed) = emitter();
    runtime.block_on(out.rows(rows)).unwrap();
    let events = runtime.block_on(drain(out, feed));
    let [SourceEvent::Push(Push::Json(push))] = events.as_slice() else {
        panic!("rows make one JSON push: {events:?}");
    };
    let lines = push
        .strip_suffix(b"\n")
        .expect("the last row ends its line");
    lines
        .split(|byte| *byte == b'\n')
        .map(|line| serde_json::from_slice(line).expect("a line is one JSON value"))
        .collect()
}

proptest! {
    #[test]
    fn rows_are_pushed_one_record_a_line(rows in prop::collection::vec(object(), 1..6)) {
        prop_assert_eq!(&pushed_lines(&rows), &rows);
        let printed: Vec<Box<RawValue>> = rows
            .iter()
            .map(|row| RawValue::from_string(serde_json::to_string_pretty(row).unwrap()).unwrap())
            .collect();
        prop_assert_eq!(&pushed_lines(&printed), &rows);
    }
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
    let too_big = Bytes::from(vec![b' '; usize::try_from(JSON_PUSH_BYTES).unwrap() + 1]);
    let error = out.json(too_big).await.unwrap_err();
    assert_eq!(error.limit().unwrap().name, "json push bytes");
    assert!(drain(out, feed).await.is_empty());
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
    let at_limit = Bytes::from(vec![b' '; usize::try_from(JSON_PUSH_BYTES).unwrap()]);
    out.json(at_limit).await.unwrap();
}

#[tokio::test]
async fn batches_over_the_row_or_column_limit_are_refused() {
    let (mut out, feed) = emitter();
    let rows = usize::try_from(BATCH_ROWS).unwrap() + 1;
    let tall = RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![0; rows])) as _)])
        .unwrap();
    assert_eq!(
        out.batch(tall).await.unwrap_err().limit().unwrap().name,
        "batch rows"
    );
    let columns = usize::try_from(SCHEMA_COLUMNS).unwrap() + 1;
    let wide = RecordBatch::try_from_iter((0..columns).map(|index| {
        (
            format!("c{index}"),
            Arc::new(Int64Array::from(vec![1])) as _,
        )
    }))
    .unwrap();
    assert_eq!(
        out.changes(wide).await.unwrap_err().limit().unwrap().name,
        "schema columns"
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
    assert_eq!((limit.name, limit.limit), ("batch values", BATCH_VALUES));
    assert!(drain(out, feed).await.is_empty());
}

#[tokio::test]
async fn a_served_reads_batch_beyond_a_frame_is_sent_on_to_be_cut() {
    let (sink, feed) = partition_channel(NonZeroUsize::new(16).unwrap());
    let mut out: Emitter<u64> = Emitter::new(sink.cut(), 7, false);
    // Three rows of a buffer beyond a frame keep all of it alive: the cut copies what they name.
    let bytes = usize::try_from(FRAME_BYTES).unwrap() + 1;
    let whole = arrow_array::UInt8Array::from(vec![0_u8; bytes]);
    let slice: ArrayRef = Arc::new(whole.slice(0, 3));
    let batch = RecordBatch::try_from_iter([("pinned", slice)]).unwrap();
    out.batch(batch).await.unwrap();
    assert_eq!(drain(out, feed).await.len(), 1);
}

#[tokio::test]
async fn a_batch_keeping_more_alive_than_a_frame_may_hold_is_refused() {
    let (mut out, feed) = emitter();
    let bytes = usize::try_from(FRAME_BYTES).unwrap() + 1;
    // Three rows of a buffer keep all of it alive.
    let whole = arrow_array::UInt8Array::from(vec![0_u8; bytes]);
    let slice: ArrayRef = Arc::new(whole.slice(0, 3));
    let batch = RecordBatch::try_from_iter([("pinned", slice)]).unwrap();
    let limit = out.batch(batch).await.unwrap_err().limit().unwrap();
    assert_eq!((limit.name, limit.limit), ("batch bytes", FRAME_BYTES));
    let mut views = arrow_array::builder::BinaryViewBuilder::new();
    let block = views.append_block(vec![7_u8; 1 << 20].into());
    for _ in 0..65 {
        views.try_append_view(block, 0, 1 << 20).unwrap();
    }
    let batch =
        RecordBatch::try_from_iter([("views", Arc::new(views.finish()) as ArrayRef)]).unwrap();
    let limit = out.batch(batch).await.unwrap_err().limit().unwrap();
    assert_eq!((limit.name, limit.limit), ("view bytes", FRAME_BYTES));
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
    let within = Duration::from_secs(1);
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

#[tokio::test]
async fn a_schema_whose_message_passes_the_schema_limit_is_refused_and_one_within_it_is_sent() {
    let batch = ids(vec![1, 2]);
    let message = u64::try_from(rdlt_wire::schema_message_bytes(&batch.schema())).unwrap();
    assert!(message > 100, "{message}");
    for (limit, sent) in [(message - 1, false), (message, true), (message + 1, true)] {
        let (sink, feed) = partition_channel(NonZeroUsize::new(16).unwrap());
        let limits = rdlt_wire::Limits {
            schema_bytes: limit,
            ..rdlt_wire::Limits::default()
        };
        let mut out = Emitter::<u64>::new(sink.within(limits), 7, false);
        let pushed = out.batch(batch.clone()).await;
        if sent {
            pushed.unwrap();
            assert_eq!(drain(out, feed).await.len(), 1);
        } else {
            let refused = pushed.unwrap_err().limit().unwrap();
            assert_eq!(
                (refused.name, refused.limit, refused.actual),
                ("schema bytes", limit, message)
            );
            assert!(drain(out, feed).await.is_empty());
        }
    }
}

#[tokio::test]
async fn a_schema_equal_to_the_last_ones_is_checked_once_and_one_differing_in_metadata_again() {
    let first = ids(vec![1]);
    let again = ids(vec![2]);
    assert!(!Arc::ptr_eq(&first.schema(), &again.schema()));
    let (mut out, _feed) = emitter();
    out.batch(first.clone()).await.unwrap();
    out.batch(again).await.unwrap();
    let kept = out.schema.clone().expect("a schema was checked");
    assert!(
        Arc::ptr_eq(&kept, &first.schema()),
        "an equal schema is not checked again"
    );
    let marked = first
        .schema()
        .as_ref()
        .clone()
        .with_metadata([("source".to_owned(), "a".to_owned())].into());
    let marked = first.clone().with_schema(Arc::new(marked)).unwrap();
    out.batch(marked.clone()).await.unwrap();
    let kept = out.schema.clone().expect("a schema was checked");
    assert!(
        Arc::ptr_eq(&kept, &marked.schema()),
        "a schema differing in its metadata is checked"
    );
}

#[cfg(feature = "wire")]
#[tokio::test]
async fn a_batch_too_wide_is_refused_as_schema_columns_in_process_and_on_the_wire() {
    let columns = usize::try_from(SCHEMA_COLUMNS).unwrap() + 1;
    let wide = RecordBatch::try_from_iter((0..columns).map(|index| {
        (
            format!("c{index}"),
            Arc::new(Int64Array::from(vec![1])) as _,
        )
    }))
    .unwrap();
    let (mut out, feed) = emitter();
    let refused = out.batch(wide.clone()).await.unwrap_err();
    assert_eq!(
        refused.limit().map(|limit| limit.name),
        Some("schema columns")
    );
    // A host's decoder refuses the same schema, arriving from a served source, by the same name.
    let message = rdlt_wire::Encoder::default()
        .schema(&wide.schema())
        .unwrap();
    let decoded = rdlt_wire::Decoder::new(rdlt_wire::Limits::default()).schema(&message);
    let Err(rdlt_wire::WireError::Refused(refusal)) = decoded else {
        panic!("the decoder refuses the schema by a limit");
    };
    assert_eq!(refusal.field, rdlt_wire::Limit::SchemaColumns);
    assert_eq!(
        crate::wire::frame_error(&rdlt_wire::WireError::Refused(refusal))
            .limit()
            .map(|limit| limit.name),
        Some("schema columns")
    );
    assert!(drain(out, feed).await.is_empty());
}

#[tokio::test]
async fn a_batch_is_checked_and_pushed_without_the_columns_its_stream_excludes() {
    let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let wide =
        RecordBatch::try_from_iter([("id", Arc::clone(&id)), ("secret", Arc::clone(&id))]).unwrap();
    let limits = rdlt_wire::Limits {
        schema_columns: 1,
        ..rdlt_wire::Limits::default()
    };
    let exclusion = crate::Exclusion::new(["secret"]);
    let (sink, feed) = partition_channel(NonZeroUsize::new(16).unwrap());
    let mut out = Emitter::<u64>::new(sink.within(limits).excluding(exclusion.clone()), 7, false);
    assert_eq!(out.excluded(), &exclusion);
    out.batch(wide.clone()).await.unwrap();
    out.changes(wide.clone()).await.unwrap();
    let narrow = wide.project(&[0]).unwrap();
    assert_eq!(
        drain(out, feed).await,
        [
            SourceEvent::Push(Push::Arrow(narrow.clone())),
            SourceEvent::Push(Push::Changes(narrow))
        ]
    );
    // Without the exclusion, its second column passes the limit.
    let (sink, _feed) = partition_channel(NonZeroUsize::new(16).unwrap());
    let mut out = Emitter::<u64>::new(sink.within(limits), 7, false);
    assert!(out.excluded().is_empty());
    let refused = out.batch(wide).await.unwrap_err().limit().unwrap();
    assert_eq!(refused.name, "schema columns");
}
