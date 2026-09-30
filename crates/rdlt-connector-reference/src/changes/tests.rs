use std::num::NonZeroUsize;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, RecordBatch};
use std::collections::BTreeSet;

use rdlt_connector::{
    ConnectContext, ConnectorError, ConnectorErrorKind, Cursor, Partition, PartitionId,
    PartitionState, Push, ReadRequest, SEQ_COLUMN, Source, SourceEvent, StreamName, StreamState,
    partition_channel, source_factory,
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
        replayable: true,
        changed_at: false,
        partial: true,
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
    // The snapshot's partitions end; the changes never do.
    assert!(
        fresh
            .partitions
            .iter()
            .all(|partition| !partition.is_unbounded())
    );
    assert!(changes.partitions.iter().all(Partition::is_unbounded));
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
    let request = ReadRequest::new(
        orders(),
        Partition::new(PartitionId::parse(partition).unwrap()),
        cursor,
    );
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

#[tokio::test]
async fn a_stream_without_snapshot_partitions_or_rows_per_batch_is_refused() {
    for (partitions, batch_rows) in [(0, 4), (2, 0)] {
        let config = serde_json::json!({
            "seed": 3,
            "streams": [{ "name": "orders", "keys": 6, "snapshot_partitions": partitions,
                          "changes": 1, "batch_rows": batch_rows }],
        });
        let refused = source_factory::<ChangesSource>()
            .connect(config, ConnectContext::new())
            .await
            .err()
            .expect("the stream is refused");
        assert_eq!(
            refused.kind(),
            ConnectorErrorKind::Config,
            "{partitions} {batch_rows}"
        );
    }
}

#[test]
fn changes_touch_the_snapshot_s_keys_and_half_as_many_again() {
    let stream = ChangedStream {
        keys: 100,
        changes: 4_000,
        truncates: Vec::new(),
        ..stream()
    };
    let ids: BTreeSet<i64> = (1..=stream.changes)
        .filter_map(|position| match change(3, &stream, position) {
            Change::Upsert { id, .. } | Change::Delete { id } => Some(id),
            Change::Truncate => None,
        })
        .collect();
    assert_eq!(ids.first(), Some(&0));
    assert_eq!(ids.last(), Some(&150));
}

/// Where each checkpoint `source` sends reading `partition` of `orders` from its start resumes.
async fn checkpoints(source: &dyn Source, partition: &str) -> Vec<Position> {
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(16).unwrap());
    let request = ReadRequest::new(
        orders(),
        Partition::new(PartitionId::parse(partition).unwrap()),
        None,
    );
    let collect = async {
        let mut positions = Vec::new();
        while let Some(event) = feed.recv().await {
            if let SourceEvent::Checkpoint { cursor, .. } = event {
                positions.push(cursor.decode::<Position>(1).unwrap());
            }
        }
        positions
    };
    let (read, positions) = tokio::join!(source.read(request, sink), collect);
    read.unwrap();
    positions
}

#[tokio::test]
async fn a_snapshot_partition_ends_at_its_last_row_and_an_empty_one_at_once() {
    // One key over two partitions: the second holds none.
    let config = serde_json::json!({
        "seed": 3,
        "streams": [{ "name": "orders", "keys": 1, "snapshot_partitions": 2, "changes": 0,
                      "batch_rows": 4 }],
    });
    let source = source_factory::<ChangesSource>()
        .connect(config, ConnectContext::new())
        .await
        .unwrap();
    let last = |next| Position { next, done: true };
    assert_eq!(checkpoints(source.as_ref(), "snapshot-0").await, [last(1)]);
    assert_eq!(checkpoints(source.as_ref(), "snapshot-1").await, [last(0)]);
}

#[tokio::test]
async fn changes_are_pushed_in_batches_of_the_rows_per_batch() {
    let source = source().await;
    let batches = read(source.as_ref(), "changes", None).await;
    let rows: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
    assert_eq!(rows, [4; 10]);
}

#[test]
fn a_stream_reads_in_one_snapshot_partition_and_batches_of_ten_by_default() {
    let stream: ChangedStream =
        serde_json::from_value(serde_json::json!({ "name": "orders", "keys": 1, "changes": 1 }))
            .unwrap();
    assert_eq!((stream.snapshot_partitions, stream.batch_rows), (1, 10));
}

#[test]
fn the_changes_a_seed_draws_stay_the_same() {
    // A seed's changes are part of the source's contract: a workload replays as it was.
    let drawn: Vec<(i64, bool)> = (1..=8)
        .map(|position| match change(3, &stream(), position) {
            Change::Upsert { id, value, .. } => (id, value.is_some()),
            other => panic!("position {position} drew {other:?}"),
        })
        .collect();
    let ids: Vec<i64> = drawn.iter().map(|(id, _)| *id).collect();
    let valued: Vec<bool> = drawn.iter().map(|(_, valued)| *valued).collect();
    assert_eq!(ids, [1, 0, 4, 8, 7, 7, 4, 8]);
    assert_eq!(valued, [true, true, true, false, true, true, false, true]);
}

#[tokio::test]
async fn a_snapshot_partition_the_stream_does_not_have_is_refused() {
    let source = source().await;
    let (sink, _feed) = partition_channel(NonZeroUsize::new(16).unwrap());
    let request = ReadRequest::new(
        orders(),
        Partition::new(PartitionId::parse("snapshot-2").unwrap()),
        None,
    );
    let refused = source
        .read(request, sink)
        .await
        .expect_err("no such partition");
    assert_eq!(refused.kind(), ConnectorErrorKind::Data);
}

#[tokio::test]
async fn a_source_that_forgets_refuses_the_changes_before_what_it_acknowledged() {
    let at = |next| Cursor::encode(1, &Position { next, done: false }).unwrap();
    // A stream that does not say it forgets can be read again.
    let catalog = source().await.discover().await.unwrap();
    assert!(catalog.get(&orders()).unwrap().is_replayable());
    for replayable in [true, false] {
        let config = serde_json::json!({
            "seed": 3, "slot": format!("forgets_{replayable}"),
            "streams": [{ "name": "orders", "keys": 6, "changes": 40,
                          "replayable": replayable }],
        });
        let source = source_factory::<ChangesSource>()
            .connect(config, ConnectContext::new())
            .await
            .unwrap();
        let catalog = source.discover().await.unwrap();
        assert_eq!(catalog.get(&orders()).unwrap().is_replayable(), replayable);
        let changes = PartitionId::parse("changes").unwrap();
        source
            .committed(&orders(), &[(changes.clone(), at(9))])
            .await
            .unwrap();
        // What it acknowledged it serves again only where it is replayable; from there on it
        // serves either way.
        let (sink, _feed) = partition_channel(NonZeroUsize::new(64).unwrap());
        let before = ReadRequest::new(orders(), Partition::new(changes.clone()), Some(at(8)));
        let early = source.read(before, sink).await;
        assert_eq!(
            early.as_ref().err().map(ConnectorError::kind),
            (!replayable).then_some(ConnectorErrorKind::Transient),
            "replayable: {replayable}"
        );
        assert!(!read(&*source, "changes", Some(at(9))).await.is_empty());
    }
}

#[tokio::test]
async fn a_timed_stream_names_its_change_time_and_each_row_carries_its_position_as_it() {
    let config = serde_json::json!({
        "seed": 3,
        "streams": [{ "name": "orders", "keys": 6, "snapshot_partitions": 1, "changes": 40,
                      "batch_rows": 4, "captured": 10, "changed_at": true }],
    });
    let source = source_factory::<ChangesSource>()
        .connect(config, ConnectContext::new())
        .await
        .unwrap();
    let catalog = source.discover().await.unwrap();
    let spec = catalog.get(&orders()).unwrap();
    assert_eq!(
        spec.change_time().map(ToString::to_string),
        Some("changed_at".into())
    );
    let declared = spec.schema().unwrap().field("changed_at").unwrap();
    assert!(matches!(
        declared.logical_type(),
        rdlt_connector::LogicalType::Timestamp(..)
    ));
    for partition in ["snapshot-0", "changes"] {
        for batch in read(source.as_ref(), partition, None).await {
            let at = batch.column_by_name("changed_at").unwrap();
            let at = at.as_primitive::<arrow_array::types::TimestampMicrosecondType>();
            let positions = keyed(std::slice::from_ref(&batch));
            let micros: Vec<u64> = at.values().iter().map(|at| at.unsigned_abs()).collect();
            let expected: Vec<u64> = positions.iter().map(|(_, position)| *position).collect();
            assert_eq!(micros, expected, "{partition}");
        }
    }
}

#[tokio::test]
async fn a_stream_of_whole_rows_leaves_no_value_unchanged_and_changes_the_same_keys() {
    let whole = |partial: bool| {
        serde_json::json!({
            "seed": 3,
            "streams": [{ "name": "orders", "keys": 6, "changes": 60, "batch_rows": 4,
                          "partial": partial }],
        })
    };
    let mut read_back = Vec::new();
    for partial in [true, false] {
        let source = source_factory::<ChangesSource>()
            .connect(whole(partial), ConnectContext::new())
            .await
            .unwrap();
        let batches = read(source.as_ref(), "changes", None).await;
        let flagged = batches
            .iter()
            .map(|batch| {
                let flags = batch
                    .column_by_name(rdlt_connector::UNCHANGED_COLUMN)
                    .unwrap();
                flags.len() - flags.null_count()
            })
            .sum::<usize>();
        assert_eq!(flagged > 0, partial);
        read_back.push(keyed(&batches));
    }
    assert_eq!(read_back[0], read_back[1]);
}
