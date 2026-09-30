use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::builder::{Int64Builder, ListBuilder};
use arrow_array::{
    ArrayRef, Int64Array, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field as ArrowField};
use rdlt_connector::{
    CommitMeta, CommitSeq, ConnectContext, ConnectorErrorKind, Destination, Field, LoadId,
    LogicalType, OpenContext, OpenedSession, Partition, PartitionId, PipelineId, ReadRequest,
    SchemaVersion, SegmentId, SegmentSet, StreamName, TableChange, TablePath, TableRef,
    TableSchema, destination_factory, partition_channel, source_factory,
};
use rdlt_connector_reference::{FilesDestination, FilesSource, files};
use serde_json::json;

async fn connect(root: &Path, format: &str) -> Box<dyn Destination> {
    destination_factory::<FilesDestination>()
        .connect(
            json!({ "root": root, "format": format }),
            ConnectContext::new(),
        )
        .await
        .expect("the files destination connects")
}

async fn open(destination: &dyn Destination, load: u128) -> OpenedSession {
    let context = OpenContext {
        pipeline: PipelineId::parse("files").expect("valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, load),
    };
    destination.open(&context).await.expect("the root opens")
}

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["rows"]).expect("valid table path"),
        name: "rows".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

fn meta(opened: &OpenedSession, load: u128, seq: CommitSeq, segments: &[u64]) -> CommitMeta {
    CommitMeta {
        load_id: LoadId::from_parts(UNIX_EPOCH, load),
        commit_seq: seq,
        epoch: opened.epoch,
        segments: segments
            .iter()
            .copied()
            .map(SegmentId)
            .collect::<SegmentSet>(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
    }
}

/// Creates the table with `schema` and stages `batch` as segment `segment`.
async fn stage(opened: &mut OpenedSession, schema: &TableSchema, batch: RecordBatch, segment: u64) {
    let create = TableChange::Create {
        table: table(),
        schema: schema.clone(),
    };
    opened
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let mut writer = opened
        .session
        .writer(&table())
        .await
        .expect("a writer opens");
    writer
        .write(SegmentId(segment), batch)
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
}

fn ids(values: &[i64]) -> (TableSchema, RecordBatch) {
    let schema = TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)])
        .expect("the schema is valid");
    let batch = RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
    )])
    .expect("the batch is valid");
    (schema, batch)
}

/// Every file under `dir`.
fn files_under(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return found;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            found.extend(files_under(&path));
        } else {
            found.push(path);
        }
    }
    found
}

#[tokio::test]
async fn a_new_session_removes_what_older_sessions_staged_and_never_published() {
    let root = tempfile::tempdir().unwrap();
    let destination = connect(root.path(), "jsonl").await;
    let (schema, batch) = ids(&[1, 2]);
    let mut first = open(destination.as_ref(), 1).await;
    stage(&mut first, &schema, batch.clone(), 1).await;
    stage(&mut first, &schema, batch, 2).await;
    let committed = meta(&first, 1, CommitSeq::FIRST, &[1]);
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
    let _second = open(destination.as_ref(), 2).await;
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
    let root = tempfile::tempdir().unwrap();
    let destination = connect(root.path(), "arrow").await;
    let mut opened = open(destination.as_ref(), 1).await;
    let mut seq = CommitSeq::FIRST;
    for _ in 0..12 {
        opened
            .session
            .commit(&meta(&opened, 1, seq, &[]))
            .await
            .unwrap();
        seq = seq.next();
    }
    let manifests = files_under(&root.path().join("_rdlt").join("pipelines"));
    assert_eq!(manifests.len(), 9, "{manifests:?}");
}

#[tokio::test]
async fn nested_values_and_times_read_back_as_they_were_written() {
    for format in ["jsonl", "arrow"] {
        let root = tempfile::tempdir().unwrap();
        let destination = connect(root.path(), format).await;
        let mut opened = open(destination.as_ref(), 1).await;
        let (schema, batch) = nested(format);
        stage(&mut opened, &schema, batch.clone(), 1).await;
        opened
            .session
            .commit(&meta(&opened, 1, CommitSeq::FIRST, &[1]))
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
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("file");
    std::fs::write(&file, b"").unwrap();
    let destination = connect(&file.join("below"), "jsonl").await;
    let error = destination.check().await.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Config);
}

#[tokio::test]
async fn a_source_root_that_cannot_be_listed_is_a_configuration_error() {
    let root = tempfile::tempdir().unwrap();
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
    let root = tempfile::tempdir().unwrap();
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
    let root = tempfile::tempdir().expect("a temporary directory");
    let destination = connect(root.path(), "jsonl").await;
    let mut opened = open(destination.as_ref(), 1).await;
    let (schema, batch) = ids(&[1, 2, 3]);
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
    let root = tempfile::tempdir().expect("a temporary directory");
    let (destination, reader) = rdlt_connector::readable_destination_factory::<FilesDestination>()
        .connect_reading(json!({ "root": root.path() }), ConnectContext::new())
        .await
        .expect("the destination connects");
    let mut opened = open(destination.as_ref(), 1).await;
    let (schema, batch) = ids(&[4, 5, 6]);
    stage(&mut opened, &schema, batch, 1).await;
    let meta = meta(&opened, 1, CommitSeq::FIRST, &[1]);
    opened
        .session
        .commit(&meta)
        .await
        .expect("the commit lands");
    let rows: usize = reader
        .published(&table())
        .await
        .expect("the table reads back")
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 3);
}

#[tokio::test]
async fn the_files_source_reads_every_row_of_both_formats() {
    let root = tempfile::tempdir().expect("a temporary directory");
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
