//! A discard that lands late never removes a newer session's staging.

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use rdlt_connector::{
    CommitMeta, CommitSeq, ConnectContext, DestinationConnector, Field, LoadId, LogicalType,
    OpenContext, PipelineId, SchemaVersion, SegmentId, SegmentSet, Session, TableChange, TablePath,
    TableRef, TableSchema, TableWriter,
};
use rdlt_connector_reference::{
    FilesDestination, MemoryDestination, SqliteDestination, files, published, sqlite,
};
use serde_json::json;

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["late"]).expect("valid table path"),
        name: "late".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

/// The rows a newer session's commit reports when an older session's discard runs after it staged.
///
/// The discard can land that late when it waits for the database behind the newer session.
async fn late_discard<C: DestinationConnector>(config: serde_json::Value) -> u64 {
    let destination = C::connect(
        serde_json::from_value(config).expect("the configuration is valid"),
        &ConnectContext::new(),
    )
    .await
    .expect("the destination connects");
    let context = OpenContext {
        pipeline: PipelineId::parse("late").expect("valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
    };
    let mut older = destination
        .open(&context)
        .await
        .expect("the older session opens");
    let mut newer = destination
        .open(&context)
        .await
        .expect("the newer session opens");
    stage(&mut newer.session).await;
    older
        .session
        .discard_staged()
        .await
        .expect("the late discard runs");
    let meta = CommitMeta {
        load_id: context.load_id,
        commit_seq: CommitSeq::FIRST,
        epoch: newer.epoch,
        segments: [SegmentId(1)].into_iter().collect::<SegmentSet>(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
    };
    newer
        .session
        .commit(&meta)
        .await
        .expect("the newer session commits")
        .rows
}

/// Creates the table in `session` and stages ids 1, 2 and 3 in segment 1.
async fn stage(session: &mut impl Session) {
    let schema = TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)])
        .expect("the schema is valid");
    let create = TableChange::Create {
        table: table(),
        schema,
    };
    session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let mut writer = session.writer(&table()).await.expect("a writer opens");
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let batch = RecordBatch::try_from_iter([("id", ids)]).expect("the batch is valid");
    writer
        .write(SegmentId(1), batch)
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[tokio::test]
async fn a_late_discard_never_removes_a_newer_sessions_staging() {
    assert_eq!(
        late_discard::<MemoryDestination>(json!({ "store": "late_discard" })).await,
        3
    );
    assert_eq!(rows(&published("late_discard", "late")), 3, "memory");
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("late.db");
    assert_eq!(
        late_discard::<SqliteDestination>(json!({ "path": path })).await,
        3
    );
    assert_eq!(
        rows(&sqlite::published(&path, "late").unwrap()),
        3,
        "sqlite"
    );
    let root = directory.path().join("files");
    assert_eq!(
        late_discard::<FilesDestination>(json!({ "root": root })).await,
        3
    );
    assert_eq!(rows(&files::published(&root, "late").unwrap()), 3, "files");
}
