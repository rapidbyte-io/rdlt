//! A change stream's table holds whatever sequences its source sent, the greatest too: no later
//! change follows it, and dropping the table is what clears it.

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, BinaryArray, Int8Array, Int64Array, RecordBatch};
use rdlt_connector::{
    ChangeColumns, ChangeOp, CommitMeta, CommitSeq, ConnectContext, Deletion, Destination,
    DestinationConnector, DroppedTable, Field, LoadId, LogicalType, MergeKey, OpenContext,
    PipelineId, PublishedReader, ReadBack, SchemaVersion, SegmentId, TableChange, TablePath,
    TableRef, TableSchema, readable_destination_factory,
};
use rdlt_connector_reference::{MemoryDestination, SqliteDestination};
use serde_json::json;

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["changes"]).expect("valid table path"),
        name: "changes".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: Some(MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
            changes: Some(ChangeColumns {
                op: "op".into(),
                unchanged: None,
                deletion: Deletion::Hard,
            }),
            history: None,
        }),
    }
}

/// Changes of `(id, sequence, op)`, a truncate's id none.
fn changes(rows: &[(Option<i64>, [u8; 16], ChangeOp)]) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(rows.iter().map(|row| row.0).collect::<Int64Array>()) as ArrayRef,
        ),
        (
            "seq",
            Arc::new(BinaryArray::from_iter_values(rows.iter().map(|row| row.1))) as _,
        ),
        (
            "op",
            Arc::new(Int8Array::from_iter_values(
                rows.iter().map(|row| row.2.code()),
            )) as _,
        ),
    ])
    .expect("a valid batch")
}

fn position(seq: u64) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&seq.to_be_bytes());
    bytes
}

/// Commits `batch`, and the drop of the table where `dropping`, in a session of its own.
async fn commit(destination: &dyn Destination, load: u128, batch: Option<RecordBatch>, drop: bool) {
    let context = OpenContext {
        pipeline: PipelineId::parse("poisoned").expect("valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, load),
    };
    let mut opened = destination.open(&context).await.expect("a session opens");
    if let Some(batch) = batch {
        let schema = TableSchema::new(vec![
            Field::new("id", LogicalType::Int64, false),
            Field::new("seq", LogicalType::Binary, false),
        ])
        .expect("the schema is valid");
        let create = TableChange::Create {
            table: table(),
            schema,
        };
        opened
            .session
            .apply_schema(&create)
            .await
            .expect("the table is created");
        let mut writer = opened.session.writer(&table()).await.expect("a writer");
        writer
            .write(SegmentId(1), batch)
            .await
            .expect("the write buffers");
        writer.flush().await.expect("the flush stages");
    }
    let dropped = DroppedTable {
        path: table().path,
        name: "changes".into(),
    };
    let meta = CommitMeta {
        load_id: context.load_id,
        commit_seq: CommitSeq::FIRST,
        epoch: opened.epoch,
        segments: [SegmentId(1)].into_iter().collect(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: if drop { vec![dropped] } else { Vec::new() },
        horizon: None,
    };
    opened
        .session
        .commit(&meta)
        .await
        .expect("the commit lands");
}

async fn ids(reader: &dyn PublishedReader) -> Vec<i64> {
    let mut ids: Vec<i64> = rdlt_connector::PublishedRows::gather(reader, &table())
        .await
        .expect("the table reads")
        .iter()
        .flat_map(|batch| {
            let ids = batch.column_by_name("id").expect("an id column");
            ids.as_primitive::<Int64Type>()
                .iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// Whether a table of `C` whose rows a truncate at the greatest sequence removed takes changes
/// again once it is dropped, and not before.
async fn a_dropped_table_takes_changes_again<C>(config: serde_json::Value)
where
    C: DestinationConnector + ReadBack,
{
    let (destination, reader) = readable_destination_factory::<C>()
        .connect_reading(config, ConnectContext::new())
        .await
        .expect("the destination connects");
    let (destination, reader) = (destination.as_ref(), reader.as_ref());
    let insert = |id: i64, seq: u64| (Some(id), position(seq), ChangeOp::Insert);
    commit(
        destination,
        1,
        Some(changes(&[insert(1, 10), insert(2, 11)])),
        false,
    )
    .await;
    assert_eq!(ids(reader).await, [1, 2]);
    // No sequence follows the greatest: the truncate removes every row and bounds every change.
    let greatest = (None, [u8::MAX; 16], ChangeOp::Truncate);
    commit(destination, 2, Some(changes(&[greatest])), false).await;
    commit(
        destination,
        3,
        Some(changes(&[insert(3, 12), insert(1, u64::MAX)])),
        false,
    )
    .await;
    assert_eq!(ids(reader).await, Vec::<i64>::new());
    // Dropped, as a reset of the stream with its tables drops it, the table forgets the bound.
    commit(destination, 4, None, true).await;
    commit(
        destination,
        5,
        Some(changes(&[insert(3, 12), insert(4, 13)])),
        false,
    )
    .await;
    assert_eq!(ids(reader).await, [3, 4]);
}

#[tokio::test]
async fn a_memory_table_dropped_forgets_a_sequence_no_change_could_follow() {
    a_dropped_table_takes_changes_again::<MemoryDestination>(json!({ "store": "poisoned" })).await;
}

#[tokio::test]
async fn a_sqlite_table_dropped_forgets_a_sequence_no_change_could_follow() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let config = json!({ "path": directory.path().join("poisoned.db") });
    a_dropped_table_takes_changes_again::<SqliteDestination>(config).await;
}
