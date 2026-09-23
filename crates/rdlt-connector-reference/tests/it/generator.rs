use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use rdlt_connector::{
    ConnectContext, Partition, PartitionId, Push, ReadRequest, Source, SourceEvent, StreamName,
    StreamState, partition_channel, source_factory,
};
use rdlt_connector_reference::GeneratorSource;
use serde_json::json;

async fn generator(rows: u64, partitions: u64) -> Box<dyn Source> {
    let config = json!({ "seed": 1, "streams": [{ "name": "events", "rows": rows, "partitions": partitions, "batch_rows": 4 }] });
    source_factory::<GeneratorSource>()
        .connect(config, ConnectContext::new())
        .await
        .expect("the generator connects")
}

async fn read_ids(source: &dyn Source, partition: Partition) -> Vec<(i64, i64)> {
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(16).expect("16 is non-zero"));
    let request = ReadRequest {
        stream: StreamName::new("events").expect("valid stream name"),
        partition,
        cursor: None,
    };
    let collect = async {
        let mut rows = Vec::new();
        while let Some(event) = feed.recv().await {
            if let SourceEvent::Push(Push::Arrow(batch)) = event {
                let ids = batch.column(0).as_primitive::<Int64Type>();
                let values = batch.column(1).as_primitive::<Int64Type>();
                rows.extend(
                    ids.values()
                        .iter()
                        .copied()
                        .zip(values.values().iter().copied()),
                );
            }
        }
        rows
    };
    let (read, rows) = tokio::join!(source.read(request, sink), collect);
    read.expect("the generator reads");
    rows
}

#[tokio::test]
async fn partitions_cover_every_row_exactly_once() {
    let source = generator(23, 4).await;
    let partitions = source
        .plan(&StreamName::new("events").unwrap(), &StreamState::default())
        .await
        .unwrap();
    assert_eq!(partitions.len(), 4);
    let mut ids = Vec::new();
    for partition in partitions {
        ids.extend(
            read_ids(source.as_ref(), partition)
                .await
                .into_iter()
                .map(|(id, _)| id),
        );
    }
    ids.sort_unstable();
    assert_eq!(ids, (0..23).collect::<Vec<_>>());
}

#[tokio::test]
async fn the_same_seed_generates_the_same_values() {
    let first = read_ids(
        generator(10, 1).await.as_ref(),
        Partition::new(PartitionId::parse("0").unwrap()),
    )
    .await;
    let second = read_ids(
        generator(10, 1).await.as_ref(),
        Partition::new(PartitionId::parse("0").unwrap()),
    )
    .await;
    assert_eq!(first, second);
    let distinct: BTreeSet<i64> = first.iter().map(|(_, value)| *value).collect();
    assert_eq!(distinct.len(), 10, "values are well distributed");
}

#[tokio::test]
async fn invalid_generator_configuration_is_refused() {
    let factory = source_factory::<GeneratorSource>();
    let zero = json!({ "seed": 1, "streams": [{ "name": "e", "rows": 1, "partitions": 0 }] });
    assert!(factory.connect(zero, ConnectContext::new()).await.is_err());
    let unknown = json!({ "seed": 1, "streams": [], "extra": true });
    assert_eq!(
        factory
            .connect(unknown, ConnectContext::new())
            .await
            .err()
            .unwrap()
            .code(),
        Some("config_invalid")
    );
}
