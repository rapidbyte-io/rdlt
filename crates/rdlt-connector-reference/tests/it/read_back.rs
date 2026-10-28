//! The memory and SQLite destinations reading back what they published, and nothing staged.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Int64Array, RecordBatch};
use rdlt_connector::{
    CommitKey, CommitMeta, ConnectContext, DestinationConnector, Field, LogicalType, OpenContext,
    OpenedSession, PipelineId, PublishedRows, ReadBack, SchemaVersion, SegmentId, TableChange,
    TablePath, TableRef, TableSchema, readable_destination_factory,
};
use rdlt_connector_reference::{MemoryDestination, SqliteDestination, tables};
use serde_json::json;

fn table(name: &str) -> TableRef {
    TableRef {
        path: TablePath::new([name]).expect("valid table path"),
        name: name.into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

/// Creates `table` and stages `ids` in `segment`.
async fn stage(opened: &mut OpenedSession, table: &TableRef, segment: u64, ids: Vec<i64>) {
    let schema = TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)])
        .expect("the schema is valid");
    let create = TableChange::Create {
        table: table.clone(),
        schema,
    };
    opened
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let mut writer = opened.session.writer(table).await.expect("a writer opens");
    let batch = RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(ids)) as _)])
        .expect("the batch is valid");
    writer
        .write(SegmentId(segment), batch)
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
}

/// The ids `C` reads back from table `published` after committing ids 1 and 2 to it, with ids 3
/// and 4 staged in another segment and never committed.
async fn read_back<C: DestinationConnector + ReadBack>(config: serde_json::Value) -> Vec<i64> {
    let (destination, reader) = readable_destination_factory::<C>()
        .connect_reading(config, ConnectContext::new())
        .await
        .expect("the destination connects");
    let context = OpenContext {
        pipeline: PipelineId::parse("reader").expect("valid pipeline id"),
    };
    let mut opened = destination.open(&context).await.expect("a session opens");
    let published = table("published");
    stage(&mut opened, &published, 1, vec![1, 2]).await;
    stage(&mut opened, &published, 2, vec![3, 4]).await;
    let meta = CommitMeta {
        key: CommitKey {
            epoch: opened.epoch,
            seq: 1,
        },
        after: opened.landed.as_ref().map(|landed| landed.key),
        segments: [SegmentId(1)].into_iter().collect(),
        abandoned: rdlt_connector::SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    };
    opened
        .session
        .commit(&meta)
        .await
        .expect("the commit lands");
    let batches = PublishedRows::gather(&*reader, &published)
        .await
        .expect("the table reads back");
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn the_memory_destination_reads_back_what_it_published() {
    let ids = read_back::<MemoryDestination>(json!({ "store": "read_back" })).await;
    assert_eq!(ids, [1, 2]);
}

#[tokio::test]
async fn the_sqlite_destination_reads_back_what_it_published() {
    let directory = crate::fixtures::tempdir().expect("a temporary directory");
    let path = directory.path().join("read_back.db");
    let ids = read_back::<SqliteDestination>(json!({ "path": path })).await;
    assert_eq!(ids, [1, 2]);
}

#[tokio::test]
async fn a_memory_store_lists_every_table_created_in_it() {
    let (destination, _reader) = readable_destination_factory::<MemoryDestination>()
        .connect_reading(json!({ "store": "listed" }), ConnectContext::new())
        .await
        .expect("the destination connects");
    let context = OpenContext {
        pipeline: PipelineId::parse("lister").expect("valid pipeline id"),
    };
    let mut opened = destination.open(&context).await.expect("a session opens");
    for name in ["second", "first"] {
        stage(&mut opened, &table(name), 1, vec![1]).await;
    }
    let mut listed = tables("listed");
    listed.sort();
    assert_eq!(listed, ["first", "second"]);
}

#[tokio::test]
async fn the_sqlite_destination_reads_a_large_table_back_in_bounded_batches() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("large.db");
    let (destination, reader) = readable_destination_factory::<SqliteDestination>()
        .connect_reading(json!({ "path": path }), ConnectContext::new())
        .await
        .expect("the destination connects");
    let context = OpenContext {
        pipeline: PipelineId::parse("reader").expect("valid pipeline id"),
    };
    let mut opened = destination.open(&context).await.expect("a session opens");
    let large = table("large");
    stage(&mut opened, &large, 1, (0..5000).collect()).await;
    let meta = CommitMeta {
        key: CommitKey {
            epoch: opened.epoch,
            seq: 1,
        },
        after: opened.landed.as_ref().map(|landed| landed.key),
        segments: [SegmentId(1)].into_iter().collect(),
        abandoned: rdlt_connector::SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    };
    opened
        .session
        .commit(&meta)
        .await
        .expect("the commit lands");
    let batches = PublishedRows::gather(&*reader, &large)
        .await
        .expect("the table reads back");
    let rows: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
    assert_eq!(rows.iter().sum::<usize>(), 5000);
    assert_eq!(rows, [1024, 1024, 1024, 1024, 904]);
}

/// The rows of each batch the SQLite destination reads back of a table whose one column, of
/// `kind`, holds `values`.
async fn sqlite_batches(kind: LogicalType, values: arrow_array::ArrayRef) -> Vec<usize> {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("wide.db");
    let (destination, reader) = readable_destination_factory::<SqliteDestination>()
        .connect_reading(json!({ "path": path }), ConnectContext::new())
        .await
        .expect("the destination connects");
    let context = OpenContext {
        pipeline: PipelineId::parse("reader").expect("valid pipeline id"),
    };
    let mut opened = destination.open(&context).await.expect("a session opens");
    let wide = table("wide");
    let schema =
        TableSchema::new(vec![Field::new("value", kind, false)]).expect("the schema is valid");
    let create = TableChange::Create {
        table: wide.clone(),
        schema,
    };
    opened
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let batch = RecordBatch::try_from_iter([("value", values)]).expect("the batch is valid");
    let mut writer = opened.session.writer(&wide).await.expect("a writer opens");
    writer
        .write(SegmentId(1), batch)
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
    drop(writer);
    let meta = CommitMeta {
        key: CommitKey {
            epoch: opened.epoch,
            seq: 1,
        },
        after: opened.landed.as_ref().map(|landed| landed.key),
        segments: [SegmentId(1)].into_iter().collect(),
        abandoned: rdlt_connector::SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    };
    opened
        .session
        .commit(&meta)
        .await
        .expect("the commit lands");
    let batches = PublishedRows::gather(&*reader, &wide)
        .await
        .expect("the table reads back");
    batches.iter().map(RecordBatch::num_rows).collect()
}

#[tokio::test]
async fn the_sqlite_destination_ends_a_batch_read_back_at_its_bytes_as_at_its_rows() {
    // Forty texts or blobs of a tenth of a megabyte: a megabyte is passed at every eleventh.
    let texts = vec!["x".repeat(100_000); 40];
    let blobs = vec![vec![7_u8; 100_000]; 40];
    let cases: [(LogicalType, arrow_array::ArrayRef); 2] = [
        (
            LogicalType::Utf8,
            Arc::new(arrow_array::StringArray::from(texts)),
        ),
        (
            LogicalType::Binary,
            Arc::new(arrow_array::BinaryArray::from_iter_values(blobs)),
        ),
    ];
    for (kind, values) in cases {
        let rows = sqlite_batches(kind.clone(), values).await;
        assert_eq!(rows, [11, 11, 11, 7], "{kind:?}");
    }
}

#[tokio::test]
async fn a_table_read_back_in_whole_batches_ends_without_an_empty_one() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("exact.db");
    let (destination, reader) = readable_destination_factory::<SqliteDestination>()
        .connect_reading(json!({ "path": path }), ConnectContext::new())
        .await
        .expect("the destination connects");
    let context = OpenContext {
        pipeline: PipelineId::parse("reader").expect("valid pipeline id"),
    };
    let mut opened = destination.open(&context).await.expect("a session opens");
    let (exact, empty) = (table("exact"), table("empty"));
    stage(&mut opened, &exact, 1, (0..2048).collect()).await;
    stage(&mut opened, &empty, 2, Vec::new()).await;
    let meta = CommitMeta {
        key: CommitKey {
            epoch: opened.epoch,
            seq: 1,
        },
        after: opened.landed.as_ref().map(|landed| landed.key),
        segments: [SegmentId(1)].into_iter().collect(),
        abandoned: rdlt_connector::SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    };
    opened
        .session
        .commit(&meta)
        .await
        .expect("the commit lands");
    let rows = |batches: Vec<RecordBatch>| -> Vec<usize> {
        batches.iter().map(RecordBatch::num_rows).collect()
    };
    let read = PublishedRows::gather(&*reader, &exact).await;
    assert_eq!(rows(read.expect("the table reads back")), [1024, 1024]);
    // A table without a row reads back as its columns, in a batch of none.
    let read = PublishedRows::gather(&*reader, &empty).await;
    assert_eq!(rows(read.expect("the table reads back")), [0]);
    let read = PublishedRows::gather(&*reader, &table("missing")).await;
    assert_eq!(
        rows(read.expect("the table reads back")),
        Vec::<usize>::new()
    );
}

/// What `C` reads back of the column `held` of a table, merged by `id` where `merged` says so,
/// that took one row holding `value` as `logical` and then a widen of `held` to `Json`, with no
/// commit since.
async fn widened_to_json<C: DestinationConnector + ReadBack>(
    config: serde_json::Value,
    merged: bool,
    (logical, value): (LogicalType, arrow_array::ArrayRef),
) -> Vec<Option<String>> {
    let (destination, reader) = readable_destination_factory::<C>()
        .connect_reading(config, ConnectContext::new())
        .await
        .expect("the destination connects");
    let context = OpenContext {
        pipeline: PipelineId::parse("widened").expect("valid pipeline id"),
    };
    let mut opened = destination.open(&context).await.expect("a session opens");
    let mut widened = table("widened");
    widened.merge = merged.then(|| rdlt_connector::MergeKey {
        columns: vec!["id".into()],
        seq: "seq".into(),
        root: None,
        changes: None,
        history: None,
    });
    let (schema, batch) = holding(&logical, value);
    let create = TableChange::Create {
        table: widened.clone(),
        schema,
    };
    opened.session.apply_schema(&create).await.expect("created");
    let mut writer = opened
        .session
        .writer(&widened)
        .await
        .expect("a writer opens");
    writer.write(SegmentId(1), batch).await.expect("buffers");
    writer.flush().await.expect("the flush stages");
    let meta = first_commit(&opened);
    opened
        .session
        .commit(&meta)
        .await
        .expect("the commit lands");
    let widen = TableChange::Widen {
        table: widened.clone(),
        column: "held".into(),
        from: logical,
        to: LogicalType::Json,
    };
    opened
        .session
        .apply_schema(&widen)
        .await
        .expect("the widen is taken");
    let batches = PublishedRows::gather(&*reader, &widened)
        .await
        .expect("the table reads back");
    texts(&batches, "held")
}

/// A table of `id`, `held` as `logical` and `seq`, and its row 1 holding `value`.
fn holding(logical: &LogicalType, value: arrow_array::ArrayRef) -> (TableSchema, RecordBatch) {
    let fields = vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("held", logical.clone(), true),
        Field::new("seq", LogicalType::Int64, false),
    ];
    let schema = TableSchema::new(fields).expect("the schema is valid");
    let ones = || Arc::new(Int64Array::from(vec![1])) as arrow_array::ArrayRef;
    let batch = RecordBatch::try_new(Arc::new(schema.to_arrow()), vec![ones(), value, ones()])
        .expect("the batch is valid");
    (schema, batch)
}

/// The first commit of `opened`, of segment 1.
fn first_commit(opened: &OpenedSession) -> CommitMeta {
    CommitMeta {
        key: CommitKey {
            epoch: opened.epoch,
            seq: 1,
        },
        after: opened.landed.as_ref().map(|landed| landed.key),
        segments: [SegmentId(1)].into_iter().collect(),
        abandoned: rdlt_connector::SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    }
}

/// The text the column `column` of `batches` holds, a row at a time.
fn texts(batches: &[RecordBatch], column: &str) -> Vec<Option<String>> {
    use arrow_array::Array as _;
    batches
        .iter()
        .flat_map(|batch| {
            let held = batch.column_by_name(column).expect("the column");
            let held = held.as_string_opt::<i32>().expect("JSON is text");
            (0..held.len())
                .map(|row| held.is_valid(row).then(|| held.value(row).to_owned()))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn a_table_read_back_after_a_widen_to_json_holds_each_stored_value_s_json() {
    use arrow_array::{ArrayRef, BooleanArray, StringArray};
    let text = || {
        (
            LogicalType::Utf8,
            Arc::new(StringArray::from(vec!["false"])) as ArrayRef,
        )
    };
    let truth = || {
        (
            LogicalType::Bool,
            Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
        )
    };
    let quoted = || vec![Some(r#""false""#.to_owned())];
    let bare = || vec![Some("true".to_owned())];
    for merged in [false, true] {
        for (case, (held, expected)) in [(text(), quoted()), (truth(), bare())]
            .into_iter()
            .enumerate()
        {
            let store = format!("widened-read-{merged}-{case}");
            let config = json!({ "store": store });
            let memory = widened_to_json::<MemoryDestination>(config, merged, held.clone()).await;
            assert_eq!(memory, expected, "memory, merged {merged}, case {case}");
            let root = crate::fixtures::tempdir().expect("a root");
            let config = json!({ "root": root.path(), "format": "arrow" });
            let files =
                widened_to_json::<rdlt_connector_reference::FilesDestination>(config, merged, held)
                    .await;
            assert_eq!(files, expected, "files, merged {merged}, case {case}");
        }
    }
}
