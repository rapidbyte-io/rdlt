use std::num::NonZeroUsize;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use rdlt_connector::limits::MAX_BATCH_ROWS;
use rdlt_connector::{
    ConnectContext, ConnectorError, ConnectorErrorKind, Cursor, Partition, PartitionId, Push,
    ReadRequest, Source, SourceEvent, StreamName, partition_channel, source_factory,
};
use serde_json::json;

use super::{Generated, GeneratedStream, GeneratorSource, NextRow, mix};
use crate::limits::MAX_PARTITIONS;

#[test]
fn rows_draw_from_the_splitmix64_output_function() {
    // The first two outputs of SplitMix64 seeded with 0, as its reference publishes them.
    assert_eq!(mix(0), 0xE220_A839_7B1D_CDAF);
    assert_eq!(mix(0x9E37_79B9_7F4A_7C15), 0x6E78_9E6A_A1B9_65F4);
}

fn stream(json: serde_json::Value) -> GeneratedStream {
    serde_json::from_value(json).expect("a valid stream")
}

#[test]
fn a_stream_is_one_partition_read_a_hundred_rows_a_batch_by_default() {
    let stream = stream(serde_json::json!({ "name": "rows", "rows": 1 }));
    assert_eq!((stream.partitions, stream.batch_rows), (1, 100));
}

#[test]
fn a_partition_is_an_index_below_the_stream_s_partitions() {
    let generated = Generated(stream(
        serde_json::json!({ "name": "rows", "rows": 1, "partitions": 2 }),
    ));
    let index =
        |id: &str| generated.partition_index(&Partition::new(PartitionId::parse(id).unwrap()));
    assert_eq!(index("1").expect("a partition"), 1);
    assert!(index("2").is_err());
}

async fn connect(stream: serde_json::Value) -> Result<Box<dyn Source>, ConnectorError> {
    let config = json!({ "seed": 3, "streams": [stream] });
    source_factory::<GeneratorSource>()
        .connect(config, ConnectContext::new())
        .await
}

/// How a read of `partition` of `rows` from row `next` ended, the ids it pushed as the unsigned
/// numbers they stand for, and where its checkpoints resume.
async fn read(
    source: &dyn Source,
    partition: &str,
    next: u64,
) -> (Result<(), ConnectorError>, Vec<u64>, Vec<Option<u64>>) {
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(16).unwrap());
    let cursor = Cursor::encode(1, &NextRow { next: Some(next) }).unwrap();
    let request = ReadRequest::new(
        StreamName::new("rows").unwrap(),
        Partition::new(PartitionId::parse(partition).unwrap()),
        Some(cursor),
    );
    let collect = async {
        let (mut ids, mut resumes) = (Vec::new(), Vec::new());
        // A read that never ends is cut off here.
        while let Some(event) = feed.recv().await {
            match event {
                SourceEvent::Push(Push::Arrow(batch)) => {
                    let column = batch.column(0).as_primitive::<Int64Type>();
                    ids.extend(column.values().iter().map(|id| id.cast_unsigned()));
                }
                SourceEvent::Checkpoint { cursor, .. } => {
                    resumes.push(cursor.decode::<NextRow>(1).unwrap().next);
                }
                _ => {}
            }
            if ids.len() > 8 {
                feed.stop();
                break;
            }
        }
        (ids, resumes)
    };
    let (ended, (ids, resumes)) = tokio::join!(source.read(request, sink), collect);
    (ended, ids, resumes)
}

#[tokio::test]
async fn a_stream_of_more_partitions_or_rows_a_batch_than_a_source_holds_is_refused() {
    let at_limits = json!({
        "name": "rows", "rows": 1, "partitions": MAX_PARTITIONS, "batch_rows": MAX_BATCH_ROWS,
    });
    connect(at_limits).await.expect("a stream at its limits");
    for (partitions, batch_rows) in [(MAX_PARTITIONS + 1, 1), (1, MAX_BATCH_ROWS + 1)] {
        let stream = json!({
            "name": "rows", "rows": 1, "partitions": partitions, "batch_rows": batch_rows,
        });
        let refused = connect(stream).await.err().expect("past a limit");
        assert_eq!(refused.kind(), ConnectorErrorKind::Config);
        assert_eq!(refused.code(), Some("limit_exceeded"), "{refused}");
    }
}

#[tokio::test]
async fn a_cursor_at_a_row_of_another_partition_is_refused() {
    let stream = json!({ "name": "rows", "rows": 10, "partitions": 2, "batch_rows": 2 });
    let source = connect(stream).await.unwrap();
    let (ended, ids, _) = read(source.as_ref(), "1", 3).await;
    ended.expect("a row of the partition");
    assert_eq!(ids, [3, 5, 7, 9]);
    // Row 4 is partition 0's.
    let (ended, ids, _) = read(source.as_ref(), "1", 4).await;
    let refused = ended.expect_err("a row of partition 0");
    assert_eq!(refused.kind(), ConnectorErrorKind::Data);
    assert_eq!(refused.code(), Some("cursor_invalid"));
    assert!(ids.is_empty());
    // A cursor past the stream's rows reads nothing, whichever partition it names.
    let (ended, ids, _) = read(source.as_ref(), "1", 10).await;
    ended.expect("nothing is left");
    assert!(ids.is_empty());
}

#[tokio::test]
async fn a_read_ends_at_the_last_row_a_number_holds() {
    let stream = json!({ "name": "rows", "rows": u64::MAX, "partitions": 2, "batch_rows": 1 });
    let source = connect(stream).await.unwrap();
    let last = u64::MAX - 1;
    let (ended, ids, resumes) = read(source.as_ref(), "0", last).await;
    ended.expect("the read ends");
    assert_eq!(ids, [last]);
    // Where it resumes, no row is left.
    let resume = resumes.last().copied().flatten().expect("a checkpoint");
    let (ended, ids, _) = read(source.as_ref(), "0", resume).await;
    ended.expect("the resumed read ends");
    assert!(ids.is_empty());
}
