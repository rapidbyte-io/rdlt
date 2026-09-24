use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{BinaryArray, Int32Array, Int64Array, RecordBatch, StringArray};
use rdlt_connector::{
    CommitMeta, CommitSeq, ConnectContext, ConnectorErrorKind, Cursor, Field, GenerationId, LoadId,
    LogicalType, MergeKey, OpenContext, OpenedSession, Partition, PipelineId, ReadRequest,
    SchemaVersion, SegmentId, SegmentSet, SourceEvent, StreamName, TableChange, TablePath,
    TableRef, TableSchema, destination_factory, partition_channel, source_factory,
};
use rdlt_connector_reference::{MemoryDestination, MemorySource, published, schema};
use serde_json::json;

#[tokio::test]
async fn the_memory_source_refuses_a_zero_page_size_and_bad_stream_names() {
    let factory = source_factory::<MemorySource>();
    let zero = json!({ "streams": { "a": [] }, "page_size": 0 });
    assert_eq!(
        factory
            .connect(zero, ConnectContext::new())
            .await
            .err()
            .unwrap()
            .kind(),
        ConnectorErrorKind::Config
    );
    let control = json!({ "streams": { "a\nb": [] } });
    assert_eq!(
        factory
            .connect(control, ConnectContext::new())
            .await
            .err()
            .unwrap()
            .kind(),
        ConnectorErrorKind::Config
    );
}

#[tokio::test]
async fn a_cursor_past_the_end_reads_nothing() {
    let source = source_factory::<MemorySource>()
        .connect(
            json!({ "streams": { "a": [{"x": 1}] } }),
            ConnectContext::new(),
        )
        .await
        .unwrap();
    let (sink, mut feed) = partition_channel(NonZeroUsize::MIN);
    let cursor = Cursor::encode(1, &json!({ "next": 99 })).unwrap();
    let request = ReadRequest {
        stream: StreamName::new("a").unwrap(),
        partition: Partition::single(),
        cursor: Some(cursor),
    };
    source.read(request, sink).await.unwrap();
    let event: Option<SourceEvent> = feed.recv().await;
    assert_eq!(event, None);
}

fn open_context(pipeline: &str, load: u128) -> OpenContext {
    OpenContext {
        pipeline: PipelineId::parse(pipeline).expect("valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, load),
    }
}

#[tokio::test]
async fn opening_one_pipeline_keeps_another_pipelines_staging() {
    let destination = destination_factory::<MemoryDestination>()
        .connect(json!({ "store": "isolation" }), ConnectContext::new())
        .await
        .unwrap();
    let table = TableRef {
        path: TablePath::new(["t"]).unwrap(),
        name: "t".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    };
    let mut first = destination.open(&open_context("first", 1)).await.unwrap();
    let mut writer = first.session.writer(&table).await.unwrap();
    let batch =
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1, 2])) as _)]).unwrap();
    writer.write(SegmentId(1), batch).await.unwrap();
    writer.flush().await.unwrap();
    destination.open(&open_context("second", 2)).await.unwrap();
    let meta = CommitMeta {
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
        commit_seq: CommitSeq::FIRST,
        epoch: first.epoch,
        segments: SegmentSet::from_iter([SegmentId(1)]),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
    };
    assert_eq!(first.session.commit(&meta).await.unwrap().rows, 2);
    assert_eq!(
        published("isolation", "t")
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id columns are Int64");
            column.values().to_vec()
        })
        .collect()
}

async fn stage(session: &mut OpenedSession, table: &TableRef, segment: u64, ids: Vec<i64>) {
    let mut writer = session
        .session
        .writer(table)
        .await
        .expect("the memory destination creates writers");
    let batch = RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(ids)) as _)])
        .expect("one column makes a batch");
    writer
        .write(SegmentId(segment), batch)
        .await
        .expect("staging succeeds");
    writer.flush().await.expect("flushing succeeds");
}

fn commit_meta(session: &OpenedSession, seq: CommitSeq, segments: &[u64]) -> CommitMeta {
    CommitMeta {
        load_id: LoadId::from_parts(UNIX_EPOCH, 7),
        commit_seq: seq,
        epoch: session.epoch,
        segments: segments.iter().copied().map(SegmentId).collect(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
    }
}

#[tokio::test]
async fn a_replace_generation_stays_hidden_until_its_finishing_commit_swaps_it_in() {
    let destination = destination_factory::<MemoryDestination>()
        .connect(json!({ "store": "replace" }), ConnectContext::new())
        .await
        .unwrap();
    assert!(destination.capabilities().write_modes.replace);
    let path = TablePath::new(["t"]).unwrap();
    let base = TableRef {
        path: path.clone(),
        name: "t".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    };
    let generation = TableRef {
        generation: Some(GenerationId(9)),
        ..base.clone()
    };
    let mut opened = destination.open(&open_context("replace", 1)).await.unwrap();
    stage(&mut opened, &base, 1, vec![1, 2]).await;
    let first = commit_meta(&opened, CommitSeq::FIRST, &[1]);
    opened.session.commit(&first).await.unwrap();
    stage(&mut opened, &generation, 2, vec![3]).await;
    let second = commit_meta(&opened, CommitSeq::FIRST.next(), &[2]);
    assert_eq!(opened.session.commit(&second).await.unwrap().rows, 1);
    assert_eq!(
        ids(&published("replace", "t")),
        [1, 2],
        "the generation is hidden"
    );
    stage(&mut opened, &generation, 3, vec![4]).await;
    let finish = CommitMeta {
        finish_generations: vec![(path, GenerationId(9))],
        ..commit_meta(&opened, CommitSeq::FIRST.next().next(), &[3])
    };
    opened.session.commit(&finish).await.unwrap();
    assert_eq!(ids(&published("replace", "t")), [3, 4]);
}

fn table_ref(name: &str) -> TableRef {
    TableRef {
        path: TablePath::new([name]).expect("valid table path"),
        name: name.into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

#[tokio::test]
async fn schema_changes_follow_the_table_and_applying_them_again_changes_nothing() {
    let destination = destination_factory::<MemoryDestination>()
        .connect(json!({ "store": "schemas" }), ConnectContext::new())
        .await
        .unwrap();
    let mut opened = destination.open(&open_context("schemas", 1)).await.unwrap();
    let table = table_ref("t");
    let add = TableChange::AddColumn {
        table: table.clone(),
        field: Field::new("extra", LogicalType::Utf8, true),
    };
    let error = opened.session.apply_schema(&add).await.unwrap_err();
    assert_eq!(
        error.kind(),
        ConnectorErrorKind::Data,
        "the table does not exist yet"
    );
    let create = |fields| TableChange::Create {
        table: table.clone(),
        schema: TableSchema::new(fields).unwrap(),
    };
    let id = Field::new("id", LogicalType::Int32, false);
    let name = Field::new("name", LogicalType::Utf8, false);
    for change in [create(vec![id.clone()]), create(vec![id, name])] {
        opened.session.apply_schema(&change).await.unwrap();
    }
    let widen = TableChange::Widen {
        table: table.clone(),
        column: "id".into(),
        from: LogicalType::Int32,
        to: LogicalType::Int64,
    };
    for change in [&add, &add, &widen, &widen] {
        opened.session.apply_schema(change).await.unwrap();
    }
    let expected = TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("name", LogicalType::Utf8, true),
        Field::new("extra", LogicalType::Utf8, true),
    ])
    .unwrap();
    assert_eq!(schema("schemas", "t"), Some(expected));
    let missing = TableChange::Widen {
        table,
        column: "missing".into(),
        from: LogicalType::Int32,
        to: LogicalType::Int64,
    };
    let error = opened.session.apply_schema(&missing).await.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
}

#[tokio::test]
async fn a_change_declaring_a_column_at_another_type_fails_and_changes_nothing() {
    let destination = destination_factory::<MemoryDestination>()
        .connect(json!({ "store": "conflicts" }), ConnectContext::new())
        .await
        .unwrap();
    let mut opened = destination
        .open(&open_context("conflicts", 1))
        .await
        .unwrap();
    let table = table_ref("t");
    let create = |fields| TableChange::Create {
        table: table.clone(),
        schema: TableSchema::new(fields).unwrap(),
    };
    let expected = TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("name", LogicalType::Utf8, true),
    ])
    .unwrap();
    let setup = create(expected.fields().iter().cloned().collect());
    opened.session.apply_schema(&setup).await.unwrap();
    let conflicts = [
        create(vec![Field::new("id", LogicalType::Utf8, false)]),
        TableChange::AddColumn {
            table: table.clone(),
            field: Field::new("name", LogicalType::Int64, true),
        },
    ];
    for change in &conflicts {
        let error = opened.session.apply_schema(change).await.unwrap_err();
        assert_eq!(
            (error.kind(), error.code()),
            (ConnectorErrorKind::Data, Some("schema_conflict")),
            "{change:?}"
        );
    }
    assert_eq!(
        schema("conflicts", "t"),
        Some(expected),
        "a conflict changes nothing"
    );
}

#[tokio::test]
async fn widening_a_column_a_crashed_attempt_widened_otherwise_joins_the_two() {
    let destination = destination_factory::<MemoryDestination>()
        .connect(json!({ "store": "joins" }), ConnectContext::new())
        .await
        .unwrap();
    let mut opened = destination.open(&open_context("joins", 1)).await.unwrap();
    let table = table_ref("t");
    let widen = |to| TableChange::Widen {
        table: table.clone(),
        column: "n".into(),
        from: LogicalType::Int32,
        to,
    };
    let changes = [
        TableChange::Create {
            table: table.clone(),
            schema: TableSchema::new(vec![Field::new("n", LogicalType::Int32, true)]).unwrap(),
        },
        widen(LogicalType::Int64),
        widen(LogicalType::Float64),
    ];
    for change in &changes {
        opened.session.apply_schema(change).await.unwrap();
    }
    let joined = LogicalType::Int64.join(&LogicalType::Float64);
    assert_eq!(
        schema("joins", "t"),
        Some(TableSchema::new(vec![Field::new("n", joined, true)]).unwrap())
    );
}

fn seq(value: u8) -> Vec<u8> {
    let mut seq = vec![0; 16];
    seq[15] = value;
    seq
}

fn merge_table() -> TableRef {
    TableRef {
        merge: Some(MergeKey {
            columns: vec!["id".into()],
            seq: "_rdlt_seq".into(),
        }),
        ..table_ref("m")
    }
}

/// Stages `batch` as `segment` of the merge table and commits it as `commit_seq`.
async fn merge_commit(
    opened: &mut OpenedSession,
    segment: u64,
    batch: RecordBatch,
    commit_seq: CommitSeq,
) {
    let mut writer = opened
        .session
        .writer(&merge_table())
        .await
        .expect("the memory destination creates writers");
    writer
        .write(SegmentId(segment), batch)
        .await
        .expect("staging succeeds");
    writer.flush().await.expect("flushing succeeds");
    let meta = commit_meta(opened, commit_seq, &[segment]);
    opened
        .session
        .commit(&meta)
        .await
        .expect("the commit lands");
}

/// The merge table's `(id, note)` rows, sorted.
fn merged_rows() -> Vec<(i64, Option<String>)> {
    let mut rows: Vec<(i64, Option<String>)> = published("merge", "m")
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column_by_name("id")
                .expect("merged rows have ids")
                .as_primitive::<Int64Type>()
                .clone();
            let notes = batch
                .column_by_name("note")
                .expect("merged rows have notes")
                .as_string::<i32>()
                .clone();
            (0..batch.num_rows())
                .map(|row| {
                    (
                        ids.value(row),
                        notes.iter().nth(row).flatten().map(str::to_owned),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect();
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn a_merge_matches_rows_published_before_the_table_changed() {
    let destination = destination_factory::<MemoryDestination>()
        .connect(json!({ "store": "merge" }), ConnectContext::new())
        .await
        .unwrap();
    let table = merge_table();
    let mut opened = destination.open(&open_context("merge", 1)).await.unwrap();
    let create = TableChange::Create {
        table: table.clone(),
        schema: TableSchema::new(vec![
            Field::new("id", LogicalType::Int32, false),
            Field::new("_rdlt_seq", LogicalType::Binary, false),
        ])
        .unwrap(),
    };
    opened.session.apply_schema(&create).await.unwrap();
    let first = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![1, 2])) as _),
        (
            "_rdlt_seq",
            Arc::new(BinaryArray::from_iter_values([seq(1), seq(2)])) as _,
        ),
    ])
    .unwrap();
    merge_commit(&mut opened, 1, first, CommitSeq::FIRST).await;
    for change in [
        TableChange::Widen {
            table: table.clone(),
            column: "id".into(),
            from: LogicalType::Int32,
            to: LogicalType::Int64,
        },
        TableChange::AddColumn {
            table,
            field: Field::new("note", LogicalType::Utf8, true),
        },
    ] {
        opened.session.apply_schema(&change).await.unwrap();
    }
    let second = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![2])) as _),
        (
            "_rdlt_seq",
            Arc::new(BinaryArray::from_iter_values([seq(1)])) as _,
        ),
        ("note", Arc::new(StringArray::from(vec!["new"])) as _),
    ])
    .unwrap();
    merge_commit(&mut opened, 2, second, CommitSeq::FIRST.next()).await;
    assert_eq!(merged_rows(), [(1, None), (2, Some("new".to_owned()))]);
}
