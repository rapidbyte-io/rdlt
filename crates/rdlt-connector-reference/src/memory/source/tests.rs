use std::num::NonZeroUsize;

use rdlt_connector::{
    ConnectContext, Cursor, Partition, PartitionId, Push, ReadRequest, SourceEvent, StreamName,
    partition_channel, source_factory,
};
use serde_json::{Value, json};

use super::{MemorySource, Offset};

#[tokio::test]
async fn a_page_as_large_as_a_number_holds_reads_the_rest_from_any_row() {
    let config = json!({
        "streams": { "rows": [{ "id": 0 }, { "id": 1 }, { "id": 2 }] },
        "page_size": usize::MAX,
    });
    let source = source_factory::<MemorySource>()
        .connect(config, ConnectContext::new())
        .await
        .unwrap();
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(16).unwrap());
    let request = ReadRequest::new(
        StreamName::new("rows").unwrap(),
        Partition::new(PartitionId::parse("all").unwrap()),
        Some(Cursor::encode(1, &Offset { next: 1 }).unwrap()),
    );
    let collect = async {
        let mut ids = Vec::new();
        while let Some(event) = feed.recv().await {
            if let SourceEvent::Push(Push::Json(bytes)) = event {
                let rows: Vec<Value> = serde_json::from_slice(&bytes).unwrap();
                ids.extend(rows.iter().map(|row| row["id"].as_u64().unwrap()));
            }
        }
        ids
    };
    let (ended, ids) = tokio::join!(source.read(request, sink), collect);
    ended.expect("the read ends");
    assert_eq!(ids, [1, 2]);
}

/// The offsets the checkpoints of a read of three rows, a page each, from the start carry,
/// following where `follow` says.
async fn checkpointed(follow: bool) -> Vec<usize> {
    let config =
        json!({ "streams": { "rows": [{ "id": 0 }, { "id": 1 }, { "id": 2 }] }, "page_size": 1 });
    let source = source_factory::<MemorySource>()
        .connect(config, ConnectContext::new())
        .await
        .unwrap();
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(16).unwrap());
    let request = ReadRequest::new(StreamName::new("rows").unwrap(), Partition::single(), None)
        .following(follow);
    let collect = async {
        let mut offsets = Vec::new();
        while let Some(event) = feed.recv().await {
            if let SourceEvent::Checkpoint { cursor, .. } = event {
                offsets.push(cursor.decode::<Offset>(1).unwrap().next);
            }
        }
        offsets
    };
    let (read, offsets) = tokio::join!(source.read(request, sink), collect);
    read.unwrap();
    offsets
}

#[tokio::test]
async fn a_read_that_does_not_follow_ends_its_partition_done_and_a_following_one_at_a_cursor() {
    // Done: rows follow the last checkpoint.
    assert_eq!(checkpointed(false).await, [1, 2]);
    assert_eq!(checkpointed(true).await, [1, 2, 3]);
}
