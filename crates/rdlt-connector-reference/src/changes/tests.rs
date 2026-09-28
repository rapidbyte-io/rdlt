use rdlt_connector::{
    ConnectContext, PartitionId, PartitionState, Source, StreamName, StreamState, source_factory,
};

use super::{CHANGES, Change, ChangedStream, ChangesSource, Position, change, expected};

fn stream() -> ChangedStream {
    ChangedStream {
        name: "orders".into(),
        keys: 6,
        snapshot_partitions: 2,
        changes: 40,
        batch_rows: 4,
        truncates: vec![30],
    }
}

async fn source() -> Box<dyn Source> {
    let config = serde_json::json!({
        "seed": 3,
        "streams": [{ "name": "orders", "keys": 6, "snapshot_partitions": 2, "changes": 40,
                      "batch_rows": 4, "truncates": [30] }],
    });
    source_factory::<ChangesSource>()
        .connect(config, ConnectContext::new())
        .await
        .expect("the source connects")
}

fn orders() -> StreamName {
    StreamName::new("orders").unwrap()
}

#[tokio::test]
async fn the_snapshot_is_planned_until_every_partition_is_done_then_the_changes() {
    let source = source().await;
    let fresh = source
        .plan(&orders(), &StreamState::default())
        .await
        .unwrap();
    assert_eq!(fresh.phase, None);
    let ids: Vec<&str> = fresh.partitions.iter().map(|p| p.id().as_str()).collect();
    assert_eq!(ids, ["snapshot-0", "snapshot-1"]);
    let done = rdlt_connector::Cursor::encode(
        1,
        &Position {
            next: 6,
            done: true,
        },
    )
    .unwrap();
    let mut state = StreamState::default();
    state.partitions.insert(
        PartitionId::parse("snapshot-0").unwrap(),
        PartitionState::Cursor(done),
    );
    let half = source.plan(&orders(), &state).await.unwrap();
    assert_eq!(half.phase, None, "one snapshot partition is still reading");
    state.partitions.insert(
        PartitionId::parse("snapshot-1").unwrap(),
        PartitionState::Done,
    );
    let changes = source.plan(&orders(), &state).await.unwrap();
    assert_eq!(changes.phase, Some(CHANGES));
    let id = PartitionId::parse("changes").unwrap();
    assert_eq!(changes.partitions.len(), 1);
    let start: Position = changes.starts[&id].decode(1).unwrap();
    assert_eq!(start.next, 1);
    state.phase = CHANGES;
    let again = source.plan(&orders(), &state).await.unwrap();
    assert_eq!(again.phase, None);
}

#[test]
fn a_truncate_clears_the_table_its_changes_leave() {
    let stream = stream();
    assert_eq!(change(3, &stream, 30), Change::Truncate);
    let table = expected(3, &stream);
    for (id, row) in &table {
        assert!(row.n > 30, "key {id} survived the truncate: {row:?}");
    }
    assert_eq!(
        expected(3, &stream),
        table,
        "the same seed gives the same table"
    );
}
