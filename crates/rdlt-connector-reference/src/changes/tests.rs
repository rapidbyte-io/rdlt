use std::num::NonZeroUsize;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, RecordBatch};
use rdlt_connector::{
    ConnectContext, Cursor, Partition, PartitionId, PartitionState, Push, ReadRequest, SEQ_COLUMN,
    Source, SourceEvent, StreamName, StreamState, partition_channel, source_factory,
};

use super::{CHANGES, Change, ChangedStream, ChangesSource, Position, change, expected, snapshot};

fn stream() -> ChangedStream {
    ChangedStream {
        name: "orders".into(),
        keys: 6,
        snapshot_partitions: 2,
        changes: 40,
        batch_rows: 4,
        truncates: vec![30],
        captured: 0,
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
    let done = Cursor::encode(
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

/// The change batches `source` pushes reading `partition` of `orders` from `cursor`.
async fn read(source: &dyn Source, partition: &str, cursor: Option<Cursor>) -> Vec<RecordBatch> {
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(16).unwrap());
    let request = ReadRequest {
        stream: orders(),
        partition: Partition::new(PartitionId::parse(partition).unwrap()),
        cursor,
    };
    let collect = async {
        let mut batches = Vec::new();
        while let Some(event) = feed.recv().await {
            if let SourceEvent::Push(Push::Changes(batch)) = event {
                batches.push(batch);
            }
        }
        batches
    };
    let (read, batches) = tokio::join!(source.read(request, sink), collect);
    read.unwrap();
    batches
}

/// Each row's key and position.
fn keyed(batches: &[RecordBatch]) -> Vec<(Option<i64>, u64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int64Type>();
        let seqs = batch
            .column_by_name(SEQ_COLUMN)
            .unwrap()
            .as_fixed_size_binary();
        for row in 0..batch.num_rows() {
            let id = ids.is_valid(row).then(|| ids.value(row));
            let position = u64::from_be_bytes(seqs.value(row)[8..].try_into().unwrap());
            rows.push((id, position));
        }
    }
    rows
}

#[tokio::test]
async fn a_snapshot_captured_after_some_changes_holds_them_and_the_changes_follow_it() {
    let config = serde_json::json!({
        "seed": 3,
        "streams": [{ "name": "orders", "keys": 6, "snapshot_partitions": 2, "changes": 40,
                      "batch_rows": 4, "truncates": [30], "captured": 10 }],
    });
    let source = source_factory::<ChangesSource>()
        .connect(config, ConnectContext::new())
        .await
        .unwrap();
    let mut stream_spec = stream();
    stream_spec.captured = 10;
    // The snapshot holds the table the first ten changes leave, each row at position 10.
    let mut snapshotted = keyed(&read(source.as_ref(), "snapshot-0", None).await);
    snapshotted.extend(keyed(&read(source.as_ref(), "snapshot-1", None).await));
    snapshotted.sort_unstable();
    let held: Vec<(Option<i64>, u64)> = snapshot(3, &stream_spec)
        .into_keys()
        .map(|id| (Some(id), 10))
        .collect();
    assert_eq!(snapshotted, held);
    // The changes start after it, where reading them from the beginning would repeat the ten.
    let mut state = StreamState::default();
    for id in ["snapshot-0", "snapshot-1"] {
        state
            .partitions
            .insert(PartitionId::parse(id).unwrap(), PartitionState::Done);
    }
    let plan = source.plan(&orders(), &state).await.unwrap();
    let start = plan.starts[&PartitionId::parse("changes").unwrap()].clone();
    let changes = keyed(&read(source.as_ref(), "changes", Some(start)).await);
    let positions: Vec<u64> = changes.iter().map(|(_, position)| *position).collect();
    assert_eq!(positions, (11..=40).collect::<Vec<_>>());
    let everything = keyed(&read(source.as_ref(), "changes", None).await);
    assert_eq!(everything.len(), 40);
}
