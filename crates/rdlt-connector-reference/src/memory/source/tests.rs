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
