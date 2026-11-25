use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::builder::{Int64Builder, ListBuilder};
use arrow_array::{
    ArrayRef, Int64Array, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field as ArrowField};
use rdlt_connector::{
    ConnectContext, ConnectorErrorKind, Partition, PartitionId, PublishedRows, ReadRequest,
    SegmentId, StreamName, TableChange, TableSchema, partition_channel, source_factory,
};
use rdlt_connector_reference::{FilesDestination, FilesSource, files};
use serde_json::json;

use crate::fixtures::{connect, files_under, ids, meta, open, stage, table};

#[tokio::test]
async fn a_new_session_removes_what_older_sessions_staged_and_never_published() {
    let root = crate::fixtures::tempdir().unwrap();
    let destination = connect(root.path(), "jsonl").await;
    let (schema, batch) = ids(&[1, 2]);
    let mut first = open(destination.as_ref()).await;
    stage(&mut first, &table("rows"), &schema, batch.clone(), 1).await;
    stage(&mut first, &table("rows"), &schema, batch, 2).await;
    let committed = meta(&first, 1, &[1]);
    first.session.commit(&committed).await.unwrap();
    let staging = root.path().join("_rdlt").join("pipelines");
    let data = || -> Vec<_> {
        files_under(&staging)
            .into_iter()
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .collect()
    };
    assert_eq!(data().len(), 2, "both staged files are on disk");
    let _second = open(destination.as_ref()).await;
    let data: Vec<_> = files_under(&staging)
        .into_iter()
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "jsonl")
        })
        .collect();
    assert_eq!(data.len(), 1, "only the published file is left: {data:?}");
    let published = files::published(root.path(), "rows").unwrap();
    assert_eq!(
        published.iter().map(RecordBatch::num_rows).sum::<usize>(),
        2
    );
}

#[tokio::test]
async fn only_the_most_recent_manifests_are_kept() {
    let root = crate::fixtures::tempdir().unwrap();
    let destination = connect(root.path(), "arrow").await;
    let mut opened = open(destination.as_ref()).await;
    for seq in 1..=12 {
        opened
            .session
            .commit(&meta(&opened, seq, &[]))
            .await
            .unwrap();
    }
    let manifests = files_under(&root.path().join("_rdlt").join("pipelines"));
    assert_eq!(manifests.len(), 9, "{manifests:?}");
}

#[tokio::test]
async fn nested_values_and_times_read_back_as_they_were_written() {
    for format in ["jsonl", "arrow"] {
        let root = crate::fixtures::tempdir().unwrap();
        let destination = connect(root.path(), format).await;
        let mut opened = open(destination.as_ref()).await;
        let (schema, batch) = nested(format);
        stage(&mut opened, &table("rows"), &schema, batch.clone(), 1).await;
        opened
            .session
            .commit(&meta(&opened, 1, &[1]))
            .await
            .unwrap();
        let published = files::published(root.path(), "rows").unwrap();
        assert_eq!(published, [batch], "{format}");
    }
}

/// A batch of a struct, a list and, for Arrow files, a timestamp column, with its schema.
fn nested(format: &str) -> (TableSchema, RecordBatch) {
    let inner = Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef;
    let point = StructArray::from(vec![(
        Arc::new(ArrowField::new("tag", DataType::Utf8, true)),
        inner,
    )]);
    let mut items = ListBuilder::new(Int64Builder::new());
    items.append_value([Some(1), Some(2)]);
    items.append_null();
    let mut columns: Vec<(&str, ArrayRef)> = vec![
        ("point", Arc::new(point)),
        ("items", Arc::new(items.finish())),
    ];
    if format == "arrow" {
        let at = TimestampMicrosecondArray::from(vec![Some(1), None]).with_timezone("UTC");
        columns.push(("at", Arc::new(at)));
    }
    let batch = RecordBatch::try_from_iter_with_nullable(
        columns.into_iter().map(|(name, array)| (name, array, true)),
    )
    .expect("the batch is valid");
    let schema = TableSchema::from_arrow(&batch.schema()).expect("the schema is logical");
    (schema, batch)
}

#[tokio::test]
async fn a_root_that_cannot_be_written_is_a_configuration_error() {
    let root = crate::fixtures::tempdir().unwrap();
    let file = root.path().join("file");
    std::fs::write(&file, b"").unwrap();
    let destination = connect(&file.join("below"), "jsonl").await;
    let error = destination.check().await.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Config);
}

#[tokio::test]
async fn a_source_root_that_cannot_be_listed_is_a_configuration_error() {
    let root = crate::fixtures::tempdir().unwrap();
    let missing = json!({ "root": root.path().join("missing") });
    let error = source_factory::<FilesSource>()
        .connect(missing, ConnectContext::new())
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ConnectorErrorKind::Config);
}

#[tokio::test]
async fn a_partition_the_source_does_not_list_is_never_opened() {
    let root = crate::fixtures::tempdir().unwrap();
    std::fs::write(root.path().join("users.jsonl"), "{\"id\": 1}\n").unwrap();
    let outside = root.path().join("outside.jsonl");
    std::fs::write(&outside, "{\"id\": 2}\n").unwrap();
    std::fs::create_dir(root.path().join("inner")).unwrap();
    std::fs::write(root.path().join("inner").join("a.jsonl"), "{\"id\": 3}\n").unwrap();
    let source = source_factory::<FilesSource>()
        .connect(json!({ "root": root.path() }), ConnectContext::new())
        .await
        .unwrap();
    let (sink, _feed) = partition_channel(NonZeroUsize::MIN);
    let request = ReadRequest::new(
        StreamName::new("inner").unwrap(),
        Partition::new(PartitionId::parse("../outside.jsonl").unwrap()),
        None,
    );
    let error = source.read(request, sink).await.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
}

#[tokio::test]
async fn a_files_writer_s_flush_counts_the_bytes_of_the_files_it_staged() {
    let root = crate::fixtures::tempdir().expect("a temporary directory");
    let destination = connect(root.path(), "jsonl").await;
    let mut opened = open(destination.as_ref()).await;
    let (schema, batch) = ids(&[1, 2, 3]);
    let create = TableChange::Create {
        table: table("rows"),
        schema,
    };
    opened
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let mut writer = opened
        .session
        .writer(&table("rows"))
        .await
        .expect("a writer");
    for segment in [1, 2] {
        writer
            .write(SegmentId(segment), batch.clone())
            .await
            .expect("the write buffers");
    }
    let stats = writer.flush().await.expect("the flush stages");
    let written: u64 = files_under(root.path())
        .iter()
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "jsonl")
        })
        .map(|path| std::fs::metadata(path).expect("the file exists").len())
        .sum();
    assert_eq!((stats.rows, stats.bytes), (6, written));
    assert!(written > 0);
}

#[test]
fn a_files_source_pushes_1024_rows_a_batch_by_default() {
    let config: rdlt_connector_reference::FilesSourceConfig =
        serde_json::from_value(json!({ "root": "anywhere" })).expect("a valid configuration");
    assert_eq!(config.batch_rows.get(), 1024);
}

#[tokio::test]
async fn the_files_destination_reads_back_what_it_published() {
    let root = crate::fixtures::tempdir().expect("a temporary directory");
    let (destination, reader) = rdlt_connector::readable_destination_factory::<FilesDestination>()
        .connect_reading(json!({ "root": root.path() }), ConnectContext::new())
        .await
        .expect("the destination connects");
    let mut opened = open(destination.as_ref()).await;
    let (schema, batch) = ids(&[4, 5, 6]);
    stage(&mut opened, &table("rows"), &schema, batch, 1).await;
    let meta = meta(&opened, 1, &[1]);
    opened
        .session
        .commit(&meta)
        .await
        .expect("the commit lands");
    let rows: usize = PublishedRows::gather(&*reader, &table("rows"))
        .await
        .expect("the table reads back")
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 3);
}

#[tokio::test]
async fn the_files_source_reads_every_row_of_both_formats() {
    let root = crate::fixtures::tempdir().expect("a temporary directory");
    std::fs::write(
        root.path().join("lines.jsonl"),
        "{\"id\": 1}\n{\"id\": 2}\n",
    )
    .expect("the file is written");
    let schema = Arc::new(arrow_schema::Schema::new(vec![ArrowField::new(
        "id",
        DataType::Int64,
        false,
    )]));
    let file = std::fs::File::create(root.path().join("ipc.arrow")).expect("the file is created");
    let mut writer =
        arrow_ipc::writer::FileWriter::try_new(file, &schema).expect("the writer starts");
    for ids in [vec![1, 2], vec![3]] {
        let column = Arc::new(Int64Array::from(ids)) as ArrayRef;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column]).expect("a batch");
        writer.write(&batch).expect("the batch is written");
    }
    writer.finish().expect("the file is finished");
    let source = source_factory::<FilesSource>()
        .connect(json!({ "root": root.path() }), ConnectContext::new())
        .await
        .expect("the source connects");
    for (stream, expected) in [("lines", 2), ("ipc", 3)] {
        let (sink, mut feed) = partition_channel(NonZeroUsize::new(64).expect("not zero"));
        let partitions = source
            .plan(
                &StreamName::new(stream).expect("a valid name"),
                &rdlt_connector::StreamState::default(),
            )
            .await
            .expect("the stream plans")
            .partitions;
        let request = ReadRequest::new(
            StreamName::new(stream).expect("a valid name"),
            partitions[0].clone(),
            None,
        );
        let reading = source.read(request, sink);
        let counting = async {
            let mut rows = 0;
            while let Some(event) = feed.recv().await {
                rows += match event {
                    rdlt_connector::SourceEvent::Push(rdlt_connector::Push::Arrow(batch)) => {
                        batch.num_rows()
                    }
                    rdlt_connector::SourceEvent::Push(rdlt_connector::Push::Json(lines)) => lines
                        .split(|byte| *byte == b'\n')
                        .filter(|line| !line.is_empty())
                        .count(),
                    _ => 0,
                };
            }
            rows
        };
        let (read, rows) = tokio::join!(reading, counting);
        read.expect("the read ends");
        assert_eq!(rows, expected, "{stream}");
    }
}

/// A session of the pipeline `name` of `destination`.
async fn opened_as(
    destination: &dyn rdlt_connector::Destination,
    name: &str,
) -> rdlt_connector::OpenedSession {
    let context = rdlt_connector::OpenContext {
        pipeline: rdlt_connector::PipelineId::parse(name).expect("a valid pipeline id"),
    };
    destination.open(&context).await.expect("the session opens")
}

#[tokio::test]
async fn a_writer_or_a_change_of_a_table_never_created_claims_nothing() {
    use rdlt_connector::{Field, Fields, GenerationId, LogicalType};
    let root = crate::fixtures::tempdir().unwrap();
    let (destination, reader) = crate::fixtures::connect_with(root.path(), json!({})).await;
    let mut opened = open(destination.as_ref()).await;
    let ghost = table("ghost");
    let unowned = (ConnectorErrorKind::Config, Some("table_unowned"));
    let generation = rdlt_connector::TableRef {
        generation: Some(GenerationId(4)),
        ..ghost.clone()
    };
    for written in [
        ghost.clone(),
        crate::fixtures::merge_table("ghost"),
        generation,
    ] {
        let Err(error) = opened.session.writer(&written).await else {
            panic!("a table writer opened for {written:?}");
        };
        assert_eq!((error.kind(), error.code()), unowned, "{written:?}");
    }
    let added = TableChange::AddColumn {
        table: ghost.clone(),
        field: Field::new("more", LogicalType::Utf8, true),
    };
    let widened = TableChange::Widen {
        table: ghost.clone(),
        column: "id".into(),
        from: LogicalType::Int32,
        to: LogicalType::Int64,
    };
    for change in [added, widened] {
        let error = opened.session.apply_schema(&change).await.unwrap_err();
        assert_eq!((error.kind(), error.code()), unowned, "{change:?}");
    }
    // A creation refused, of a type out of canonical form, claims nothing either.
    let unsorted = Fields::new(vec![
        Field::new("b", LogicalType::Int64, true),
        Field::new("a", LogicalType::Int64, true),
    ])
    .unwrap();
    let column = Field::new("meta", LogicalType::Struct(unsorted), true);
    let refused = TableChange::Create {
        table: ghost.clone(),
        schema: TableSchema::new(vec![column]).unwrap(),
    };
    let error = opened.session.apply_schema(&refused).await.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
    let catalogs = root.path().join("_rdlt").join("tables");
    assert!(crate::fixtures::dirs_under(&catalogs).is_empty());
    // Any pipeline may create the table, and load it.
    let mut other = opened_as(destination.as_ref(), "other").await;
    let (schema, batch) = ids(&[7]);
    stage(&mut other, &ghost, &schema, batch, 1).await;
    other.session.commit(&meta(&other, 1, &[1])).await.unwrap();
    let published = crate::fixtures::published_ids(reader.as_ref(), &ghost).await;
    assert_eq!(published, [7]);
}

#[tokio::test]
async fn a_table_another_pipeline_created_is_theirs_and_keeps_through_a_creation_it_refuses() {
    use rdlt_connector::{Field, LogicalType};
    let root = crate::fixtures::tempdir().unwrap();
    let (destination, reader) = crate::fixtures::connect_with(root.path(), json!({})).await;
    let mut opened = open(destination.as_ref()).await;
    let theirs = table("theirs");
    let mut other = opened_as(destination.as_ref(), "other").await;
    let (schema, batch) = ids(&[7]);
    stage(&mut other, &theirs, &schema, batch, 1).await;
    other.session.commit(&meta(&other, 1, &[1])).await.unwrap();
    // A creation its table refuses leaves the table as it was, and its owner's.
    let text = Field::new("id", LogicalType::Utf8, true);
    let conflicting = TableChange::Create {
        table: theirs.clone(),
        schema: TableSchema::new(vec![text]).unwrap(),
    };
    let error = other.session.apply_schema(&conflicting).await.unwrap_err();
    assert_eq!(error.code(), Some("schema_conflict"));
    other.session.writer(&theirs).await.unwrap();
    let published = crate::fixtures::published_ids(reader.as_ref(), &theirs).await;
    assert_eq!(published, [7]);
    // Another pipeline's table is refused as theirs, and one no pipeline owns, to a session a
    // newer one fenced, as fenced.
    let Err(refused) = opened.session.writer(&theirs).await else {
        panic!("a table writer opened for another pipeline's table");
    };
    let owned = (ConnectorErrorKind::Config, Some("table_owned"));
    assert_eq!((refused.kind(), refused.code()), owned);
    let _newer = open(destination.as_ref()).await;
    let Err(stale) = opened.session.writer(&table("nobody")).await else {
        panic!("a table writer opened for a fenced session");
    };
    assert_eq!(stale.kind(), ConnectorErrorKind::Fenced);
}
