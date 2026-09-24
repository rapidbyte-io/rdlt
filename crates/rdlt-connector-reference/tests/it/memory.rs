use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::{Int64Array, RecordBatch};
use rdlt_connector::{
    CommitMeta, CommitSeq, ConnectContext, ConnectorErrorKind, Cursor, GenerationId, LoadId,
    OpenContext, OpenedSession, Partition, PipelineId, ReadRequest, SchemaVersion, SegmentId,
    SegmentSet, SourceEvent, StreamName, TablePath, TableRef, destination_factory,
    partition_channel, source_factory,
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
        generation: None,
        merge: None,
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

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id columns are Int64");
            column.values().to_vec()
        })
        .collect()
}

async fn stage(session: &mut OpenedSession, table: &TableRef, segment: u64, ids: Vec<i64>) {
    let mut writer = session
        .session
        .writer(table)
        .await
        .expect("the memory destination creates writers");
    let batch = RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(ids)) as _)])
        .expect("one column makes a batch");
    writer
        .write(SegmentId(segment), batch)
        .await
        .expect("staging succeeds");
    writer.flush().await.expect("flushing succeeds");
}

fn commit_meta(session: &OpenedSession, seq: CommitSeq, segments: &[u64]) -> CommitMeta {
    CommitMeta {
        load_id: LoadId::from_parts(UNIX_EPOCH, 7),
        commit_seq: seq,
        epoch: session.epoch,
        segments: segments.iter().copied().map(SegmentId).collect(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
    }
}

#[tokio::test]
async fn a_replace_generation_stays_hidden_until_its_finishing_commit_swaps_it_in() {
    let destination = destination_factory::<MemoryDestination>()
        .connect(json!({ "store": "replace" }), ConnectContext::new())
        .await
        .unwrap();
    assert!(destination.capabilities().write_modes.replace);
    let path = TablePath::new(["t"]).unwrap();
    let base = TableRef {
        path: path.clone(),
        name: "t".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    };
    let generation = TableRef {
        generation: Some(GenerationId(9)),
        ..base.clone()
    };
    let mut opened = destination.open(&open_context("replace", 1)).await.unwrap();
    stage(&mut opened, &base, 1, vec![1, 2]).await;
    let first = commit_meta(&opened, CommitSeq::FIRST, &[1]);
    opened.session.commit(&first).await.unwrap();
    stage(&mut opened, &generation, 2, vec![3]).await;
    let second = commit_meta(&opened, CommitSeq::FIRST.next(), &[2]);
    assert_eq!(opened.session.commit(&second).await.unwrap().rows, 1);
    assert_eq!(
        ids(&published("replace", "t")),
        [1, 2],
        "the generation is hidden"
    );
    stage(&mut opened, &generation, 3, vec![4]).await;
    let finish = CommitMeta {
        finish_generations: vec![(path, GenerationId(9))],
        ..commit_meta(&opened, CommitSeq::FIRST.next().next(), &[3])
    };
    opened.session.commit(&finish).await.unwrap();
    assert_eq!(ids(&published("replace", "t")), [3, 4]);
}
