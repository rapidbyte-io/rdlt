use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::{Int64Array, RecordBatch};
use rdlt_connector::{
    CommitMeta, CommitSeq, ConnectContext, ConnectorErrorKind, Cursor, LoadId, OpenContext,
    Partition, PipelineId, ReadRequest, SchemaVersion, SegmentId, SegmentSet, SourceEvent,
    StreamName, TablePath, TableRef, destination_factory, partition_channel, source_factory,
};
use rdlt_connector_reference::{MemoryDestination, MemorySource, published};
use serde_json::json;

#[tokio::test]
async fn the_memory_source_refuses_a_zero_page_size_and_bad_stream_names() {
    let factory = source_factory::<MemorySource>();
    let zero = json!({ "streams": { "a": [] }, "page_size": 0 });
    assert_eq!(
        factory
            .connect(zero, ConnectContext::new())
            .await
            .err()
            .unwrap()
            .kind(),
        ConnectorErrorKind::Config
    );
    let control = json!({ "streams": { "a\nb": [] } });
    assert_eq!(
        factory
            .connect(control, ConnectContext::new())
            .await
            .err()
            .unwrap()
            .kind(),
        ConnectorErrorKind::Config
    );
}

#[tokio::test]
async fn a_cursor_past_the_end_reads_nothing() {
    let source = source_factory::<MemorySource>()
        .connect(
            json!({ "streams": { "a": [{"x": 1}] } }),
            ConnectContext::new(),
        )
        .await
        .unwrap();
    let (sink, mut feed) = partition_channel(NonZeroUsize::MIN);
    let cursor = Cursor::encode(1, &json!({ "next": 99 })).unwrap();
    let request = ReadRequest {
        stream: StreamName::new("a").unwrap(),
        partition: Partition::single(),
        cursor: Some(cursor),
    };
    source.read(request, sink).await.unwrap();
    let event: Option<SourceEvent> = feed.recv().await;
    assert_eq!(event, None);
}

fn open_context(pipeline: &str, load: u128) -> OpenContext {
    OpenContext {
        pipeline: PipelineId::parse(pipeline).expect("valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, load),
    }
}

#[tokio::test]
async fn opening_one_pipeline_keeps_another_pipelines_staging() {
    let destination = destination_factory::<MemoryDestination>()
        .connect(json!({ "store": "isolation" }), ConnectContext::new())
        .await
        .unwrap();
    let table = TableRef {
        path: TablePath::new(["t"]).unwrap(),
        name: "t".into(),
        version: SchemaVersion(1),
    };
    let mut first = destination.open(&open_context("first", 1)).await.unwrap();
    let mut writer = first.session.writer(&table).await.unwrap();
    let batch =
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1, 2])) as _)]).unwrap();
    writer.write(SegmentId(1), batch).await.unwrap();
    writer.flush().await.unwrap();
    destination.open(&open_context("second", 2)).await.unwrap();
    let meta = CommitMeta {
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
        commit_seq: CommitSeq::FIRST,
        epoch: first.epoch,
        segments: SegmentSet::from_iter([SegmentId(1)]),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
    };
    assert_eq!(first.session.commit(&meta).await.unwrap().rows, 2);
    assert_eq!(
        published("isolation", "t")
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );
}
