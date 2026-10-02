//! The files source reads only regular files under its root, within its limits, and pushes JSON
//! lines as they are written.

use std::num::NonZeroUsize;
use std::os::unix::fs::symlink;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rdlt_connector::{
    ConnectContext, ConnectorError, ConnectorErrorKind, Cursor, Push, ReadRequest, Source,
    SourceEvent, StreamName, StreamState, partition_channel, source_factory,
};
use rdlt_connector_reference::FilesSource;
use serde_json::{Value, json};

/// How long any read of these tests may take: a read that waits on a pipe or reads without end
/// fails here rather than hanging.
const BOUND: Duration = Duration::from_secs(30);

async fn connect_with(root: &Path, settings: Value) -> Result<Box<dyn Source>, ConnectorError> {
    let mut config = json!({ "root": root });
    for (key, value) in settings.as_object().into_iter().flatten() {
        config[key] = value.clone();
    }
    source_factory::<FilesSource>()
        .connect(config, ConnectContext::new())
        .await
}

async fn connect(root: &Path) -> Box<dyn Source> {
    connect_with(root, json!({}))
        .await
        .expect("the source connects")
}

/// The names of the streams `source` lists, in order.
async fn streams(source: &dyn Source) -> Vec<String> {
    let catalog = source.discover().await.expect("the source lists");
    catalog
        .iter()
        .map(|stream| stream.name().to_string())
        .collect()
}

/// Reads the first partition of `stream` from `cursor`: every event it sent, and how it ended.
async fn read(
    source: &dyn Source,
    stream: &str,
    cursor: Option<Cursor>,
) -> (Vec<SourceEvent>, Result<(), ConnectorError>) {
    let stream = StreamName::new(stream).expect("a valid name");
    let partitions = source
        .plan(&stream, &StreamState::default())
        .await
        .expect("the stream plans")
        .partitions;
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(64).expect("not zero"));
    let request = ReadRequest::new(stream, partitions[0].clone(), cursor);
    let reading = source.read(request, sink);
    let collecting = async {
        let mut events = Vec::new();
        while let Some(event) = feed.recv().await {
            events.push(event);
        }
        events
    };
    let (ended, events) = tokio::time::timeout(BOUND, async { tokio::join!(reading, collecting) })
        .await
        .expect("the read ends");
    (events, ended)
}

/// The JSON lines `events` pushed, one string per push.
fn pushed(events: &[SourceEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event {
            SourceEvent::Push(Push::Json(json)) => {
                Some(String::from_utf8(json.to_vec()).expect("UTF-8"))
            }
            _ => None,
        })
        .collect()
}

fn cursors(events: &[SourceEvent]) -> Vec<Cursor> {
    events
        .iter()
        .filter_map(|event| match event {
            SourceEvent::Checkpoint { cursor, .. } => Some(cursor.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_link_in_the_source_root_is_never_read() {
    let base = crate::fixtures::tempdir().unwrap();
    let (root, outside) = (base.path().join("root"), base.path().join("outside"));
    std::fs::create_dir_all(outside.join("tenant")).unwrap();
    std::fs::create_dir_all(root.join("inner")).unwrap();
    std::fs::write(outside.join("token.json"), "{\"token\":\"s3cr3t\"}\n").unwrap();
    std::fs::write(outside.join("tenant").join("p.jsonl"), "{\"ssn\":1}\n").unwrap();
    std::fs::write(root.join("good.jsonl"), "{\"id\":1}\n").unwrap();
    std::fs::write(root.join("inner").join("a.jsonl"), "{\"id\":2}\n").unwrap();
    symlink(outside.join("token.json"), root.join("leak.jsonl")).unwrap();
    symlink(outside.join("tenant"), root.join("dirleak")).unwrap();
    symlink(
        outside.join("token.json"),
        root.join("inner").join("leak.jsonl"),
    )
    .unwrap();
    let source = connect(&root).await;
    assert_eq!(streams(source.as_ref()).await, ["good", "inner"]);
    let (events, ended) = read(source.as_ref(), "inner", None).await;
    ended.expect("the directory's own file reads");
    assert_eq!(pushed(&events), ["{\"id\":2}\n"]);
    // A file a link replaced after the source listed it is refused when read.
    std::fs::remove_file(root.join("good.jsonl")).unwrap();
    symlink(outside.join("token.json"), root.join("good.jsonl")).unwrap();
    let (events, ended) = read(source.as_ref(), "good", None).await;
    assert!(pushed(&events).is_empty());
    assert_eq!(ended.unwrap_err().kind(), ConnectorErrorKind::Data);
    // So is a directory a link replaced.
    std::fs::rename(root.join("inner"), root.join("was")).unwrap();
    symlink(outside.join("tenant"), root.join("inner")).unwrap();
    let (events, ended) = read(source.as_ref(), "inner", None).await;
    assert!(pushed(&events).is_empty());
    let error = ended.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Config);
    assert_eq!(error.code(), Some("not_a_directory"));
}

#[tokio::test]
async fn a_source_root_or_a_stream_s_directory_others_may_write_is_refused() {
    use std::os::unix::fs::PermissionsExt as _;
    let root = crate::fixtures::tempdir().unwrap();
    std::fs::create_dir(root.path().join("inner")).unwrap();
    std::fs::write(root.path().join("inner").join("a.jsonl"), "{}\n").unwrap();
    let mode = |path: &Path, mode| {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    };
    // Whoever may write a directory can plant names in it, hard links to other files among
    // them: only a directory that is its user's alone is a source's.
    let inner = root.path().join("inner");
    let shared = [
        (root.path(), 0o777),
        (root.path(), 0o1777),
        (root.path(), 0o770),
        (inner.as_path(), 0o777),
        (inner.as_path(), 0o720),
    ];
    for (path, bits) in shared {
        mode(path, bits);
        let Err(error) = connect_with(root.path(), json!({})).await else {
            panic!("{} with mode {bits:o} was read", path.display());
        };
        assert_eq!(error.kind(), ConnectorErrorKind::Config, "{bits:o}");
        assert_eq!(error.code(), Some("not_private"), "{bits:o}");
        mode(path, 0o700);
    }
    for bits in [0o700, 0o755, 0o500] {
        mode(root.path(), bits);
        let connected = connect_with(root.path(), json!({})).await;
        mode(root.path(), 0o700);
        assert!(connected.is_ok(), "{bits:o}");
    }
}

#[tokio::test]
async fn a_file_in_a_private_root_is_read_whoever_else_names_it() {
    // A hard link is a name like any other: in a root only its operator may write, every name
    // is the operator's own, and a file with several is read, as snapshots and backups make them.
    let base = crate::fixtures::tempdir().unwrap();
    let (root, elsewhere) = (base.path().join("root"), base.path().join("elsewhere"));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&elsewhere).unwrap();
    std::fs::write(elsewhere.join("orders.jsonl"), "{\"id\":1}\n").unwrap();
    std::fs::hard_link(elsewhere.join("orders.jsonl"), root.join("orders.jsonl")).unwrap();
    let source = connect(&root).await;
    let (events, ended) = read(source.as_ref(), "orders", None).await;
    ended.expect("the file reads");
    assert_eq!(pushed(&events), ["{\"id\":1}\n"]);
}

fn mkfifo(path: &Path) {
    let made = std::process::Command::new("mkfifo")
        .arg(path)
        .status()
        .expect("mkfifo runs");
    assert!(made.success());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pipe_in_the_source_root_is_never_opened_to_wait_on() {
    let root = crate::fixtures::tempdir().unwrap();
    std::fs::write(root.path().join("good.jsonl"), "{\"id\":1}\n").unwrap();
    mkfifo(&root.path().join("orders.jsonl"));
    let source = connect(root.path()).await;
    assert_eq!(streams(source.as_ref()).await, ["good"]);
    // A file a pipe replaced after the source listed it is refused, not waited on.
    std::fs::remove_file(root.path().join("good.jsonl")).unwrap();
    mkfifo(&root.path().join("good.jsonl"));
    let (events, ended) = read(source.as_ref(), "good", None).await;
    assert!(pushed(&events).is_empty());
    assert_eq!(ended.unwrap_err().kind(), ConnectorErrorKind::Data);
}

#[tokio::test]
async fn a_file_the_source_cannot_name_is_skipped() {
    let root = crate::fixtures::tempdir().unwrap();
    std::fs::create_dir(root.path().join("inner")).unwrap();
    std::fs::write(root.path().join("orders.jsonl"), "{\"id\":1}\n").unwrap();
    std::fs::write(root.path().join("ev\u{1b}[31mil\n.jsonl"), "").unwrap();
    std::fs::write(root.path().join("inner").join("a\tb.jsonl"), "").unwrap();
    std::fs::write(root.path().join("inner").join("a.jsonl"), "{\"id\":2}\n").unwrap();
    let source = connect(root.path()).await;
    assert_eq!(streams(source.as_ref()).await, ["inner", "orders"]);
    let stream = StreamName::new("inner").unwrap();
    let plan = source.plan(&stream, &StreamState::default()).await.unwrap();
    assert_eq!(plan.partitions.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_line_beyond_the_limit_is_refused_before_it_is_held() {
    let root = crate::fixtures::tempdir().unwrap();
    // One line of 64 GiB that never ends, as a sparse file holds it.
    std::fs::File::create(root.path().join("events.jsonl"))
        .unwrap()
        .set_len(1 << 36)
        .unwrap();
    let source = connect_with(root.path(), json!({ "max_file_bytes": 1_u64 << 40 }))
        .await
        .unwrap();
    let (events, ended) = read(source.as_ref(), "events", None).await;
    assert!(pushed(&events).is_empty());
    let error = ended.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
    assert_eq!(error.code(), Some("limit_exceeded"));
    // The limit is the configuration's: a line at it reads, a line one byte longer does not.
    let line = format!("{{\"v\":\"{}\"}}\n", "x".repeat(88));
    assert_eq!(line.len(), 97);
    std::fs::write(root.path().join("events.jsonl"), &line).unwrap();
    for (limit, reads) in [(96, true), (95, false)] {
        let source = connect_with(root.path(), json!({ "max_line_bytes": limit }))
            .await
            .unwrap();
        let (events, ended) = read(source.as_ref(), "events", None).await;
        assert_eq!(ended.is_ok(), reads, "{limit}");
        assert_eq!(pushed(&events).len(), usize::from(reads), "{limit}");
        if let Err(error) = ended {
            let limit_exceeded = error.limit().expect("a limit");
            assert_eq!((limit_exceeded.limit, limit_exceeded.actual), (95, 96));
        }
    }
}

#[tokio::test]
async fn a_file_beyond_the_limit_is_refused_unread() {
    let root = crate::fixtures::tempdir().unwrap();
    std::fs::write(root.path().join("events.jsonl"), "{\"id\":1}\n{\"id\":2}\n").unwrap();
    for (limit, reads) in [(18, true), (17, false)] {
        let source = connect_with(root.path(), json!({ "max_file_bytes": limit }))
            .await
            .unwrap();
        let (events, ended) = read(source.as_ref(), "events", None).await;
        assert_eq!(ended.is_ok(), reads, "{limit}");
        assert_eq!(pushed(&events).is_empty(), !reads, "{limit}");
        if let Err(error) = ended {
            assert_eq!(error.code(), Some("limit_exceeded"));
        }
    }
}

#[tokio::test]
async fn integers_beyond_64_bits_are_pushed_as_they_are_written() {
    let root = crate::fixtures::tempdir().unwrap();
    let lines = "{\"id\":9007199254740993}\n{\"id\":9223372036854775807}\n\
                 {\"id\":18446744073709551615}\n{\"id\":18446744073709551614}\n{\"id\":1.5}\n";
    std::fs::write(root.path().join("ids.jsonl"), lines).unwrap();
    let source = connect(root.path()).await;
    let (events, ended) = read(source.as_ref(), "ids", None).await;
    ended.expect("the file reads");
    assert_eq!(pushed(&events).concat(), lines);
    assert!(
        events
            .iter()
            .all(|event| !matches!(event, SourceEvent::Push(Push::Arrow(_)))),
        "a JSON lines file was typed by the source"
    );
}

#[tokio::test]
async fn a_json_lines_read_pushes_bounded_records_and_resumes_after_them() {
    let root = crate::fixtures::tempdir().unwrap();
    let lines = "{\"id\":1}\n\n{\"id\":2}\r\n   \n{\"id\":3}\n{\"id\":4}\n{\"id\":5}";
    std::fs::write(root.path().join("ids.ndjson"), lines).unwrap();
    let source = connect_with(root.path(), json!({ "batch_rows": 2 }))
        .await
        .unwrap();
    let (events, ended) = read(source.as_ref(), "ids", None).await;
    ended.expect("the file reads");
    let all = [
        "{\"id\":1}\n{\"id\":2}\r\n",
        "{\"id\":3}\n{\"id\":4}\n",
        "{\"id\":5}\n",
    ];
    assert_eq!(pushed(&events), all);
    let cursors = cursors(&events);
    assert_eq!(cursors.len(), 3, "a checkpoint follows each push");
    for (after, cursor) in cursors.into_iter().enumerate() {
        let (events, ended) = read(source.as_ref(), "ids", Some(cursor)).await;
        ended.expect("the file reads again");
        assert_eq!(pushed(&events), all[after + 1..], "after push {after}");
    }
}

#[tokio::test]
async fn a_json_lines_push_holds_at_most_the_bytes_one_push_may() {
    let root = crate::fixtures::tempdir().unwrap();
    // Three lines of 32 MiB, their endings counted: two fill one push of 64 MiB exactly, the
    // third starts the next.
    let line = format!("{{\"v\":\"{}\"}}\n", "x".repeat(32 * 1024 * 1024 - 9));
    assert_eq!(line.len(), 32 * 1024 * 1024);
    std::fs::write(root.path().join("wide.jsonl"), line.repeat(3)).unwrap();
    let source = connect(root.path()).await;
    let (events, ended) = read(source.as_ref(), "wide", None).await;
    ended.expect("the file reads");
    let sizes: Vec<usize> = pushed(&events).iter().map(String::len).collect();
    assert_eq!(sizes, [2 * line.len(), line.len()]);
}

/// Writes an Arrow file of `batches` batches of three ids each at `path`.
fn arrow_file(path: &Path, batches: usize) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let file = std::fs::File::create(path).expect("the fixture is made");
    let mut writer =
        arrow_ipc::writer::FileWriter::try_new(file, &schema).expect("the fixture is made");
    for _ in 0..batches {
        let column = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![column]).expect("the fixture is made");
        writer.write(&batch).expect("the fixture is made");
    }
    writer.finish().expect("the fixture is made");
}

/// Overwrites one field of the first record-batch block in the footer of the Arrow file at
/// `path`: its offset, metadata length or body length.
pub(crate) fn poison(path: &Path, field: &str, value: i64) {
    let mut bytes = std::fs::read(path).expect("the fixture is made");
    let end = bytes.len() - 10;
    let length = i32::from_le_bytes(bytes[end..end + 4].try_into().expect("the fixture is made"));
    let start = end - usize::try_from(length).expect("the fixture is made");
    let image = {
        let footer = arrow_ipc::root_as_footer(&bytes[start..end]).expect("the fixture is made");
        let block = footer.recordBatches().expect("the fixture is made").get(0);
        let mut image = Vec::new();
        image.extend_from_slice(&block.offset().to_le_bytes());
        image.extend_from_slice(&block.metaDataLength().to_le_bytes());
        image.extend_from_slice(&[0; 4]);
        image.extend_from_slice(&block.bodyLength().to_le_bytes());
        image
    };
    let at = start
        + bytes[start..end]
            .windows(24)
            .position(|window| window == image)
            .expect("the block is in the footer");
    match field {
        "offset" => bytes[at..at + 8].copy_from_slice(&value.to_le_bytes()),
        "metadata" => {
            let value = i32::try_from(value).expect("the fixture is made");
            bytes[at + 8..at + 12].copy_from_slice(&value.to_le_bytes());
        }
        _ => bytes[at + 16..at + 24].copy_from_slice(&value.to_le_bytes()),
    }
    std::fs::write(path, bytes).expect("the fixture is made");
}

/// Every way a block's footer entry can point outside its file.
pub(crate) fn poisons() -> Vec<(&'static str, i64)> {
    vec![
        ("body", -1),
        ("body", 1 << 46),
        ("body", 1 << 62),
        ("body", i64::MAX),
        ("body", 1 << 20),
        ("offset", -8),
        ("offset", 1 << 46),
        ("offset", i64::MAX),
        ("metadata", -1),
        ("metadata", i64::from(i32::MAX)),
        ("metadata", 0),
        ("metadata", 4),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn an_arrow_file_whose_block_lies_outside_it_is_refused() {
    for (field, value) in poisons() {
        let root = crate::fixtures::tempdir().unwrap();
        let path = root.path().join("orders.arrow");
        arrow_file(&path, 2);
        poison(&path, field, value);
        let source = connect(root.path()).await;
        let (events, ended) = read(source.as_ref(), "orders", None).await;
        assert!(events.is_empty(), "{field} {value}");
        let error = ended.expect_err("the file is refused");
        assert_eq!(error.kind(), ConnectorErrorKind::Data, "{field} {value}");
    }
}

#[tokio::test]
async fn an_arrow_file_cut_short_or_with_a_footer_outside_it_is_refused() {
    let whole = crate::fixtures::tempdir().unwrap();
    arrow_file(&whole.path().join("orders.arrow"), 2);
    let bytes = std::fs::read(whole.path().join("orders.arrow")).unwrap();
    let end = bytes.len() - 10;
    let mut cases: Vec<Vec<u8>> = (0..bytes.len())
        .step_by(7)
        .map(|cut| bytes[..cut].to_vec())
        .collect();
    for length in [-1_i32, 0, 1, i32::MAX, i32::try_from(bytes.len()).unwrap()] {
        let mut damaged = bytes.clone();
        damaged[end..end + 4].copy_from_slice(&length.to_le_bytes());
        cases.push(damaged);
    }
    for (case, damaged) in cases.into_iter().enumerate() {
        let root = crate::fixtures::tempdir().unwrap();
        std::fs::write(root.path().join("orders.arrow"), damaged).unwrap();
        let source = connect(root.path()).await;
        let (events, ended) = read(source.as_ref(), "orders", None).await;
        assert!(events.is_empty(), "case {case}");
        let error = ended.expect_err("the file is refused");
        assert_eq!(error.kind(), ConnectorErrorKind::Data, "case {case}");
    }
}

#[tokio::test]
async fn an_arrow_read_resumes_after_the_batches_read() {
    let root = crate::fixtures::tempdir().unwrap();
    arrow_file(&root.path().join("orders.arrow"), 3);
    let source = connect(root.path()).await;
    let (events, ended) = read(source.as_ref(), "orders", None).await;
    ended.expect("the file reads");
    let batches = |events: &[SourceEvent]| {
        events
            .iter()
            .filter(|event| matches!(event, SourceEvent::Push(Push::Arrow(_))))
            .count()
    };
    assert_eq!(batches(&events), 3);
    for (after, cursor) in cursors(&events).into_iter().enumerate() {
        let (events, ended) = read(source.as_ref(), "orders", Some(cursor)).await;
        ended.expect("the file reads again");
        assert_eq!(batches(&events), 2 - after);
    }
}

#[tokio::test]
async fn a_line_limit_beyond_what_one_push_holds_is_a_configuration_error() {
    let root = crate::fixtures::tempdir().unwrap();
    let push = rdlt_connector::limits::MAX_JSON_PUSH_BYTES;
    for (limit, connects) in [(push - 2, true), (push - 1, false), (push, false)] {
        let connected = connect_with(root.path(), json!({ "max_line_bytes": limit })).await;
        assert_eq!(connected.is_ok(), connects, "{limit}");
        if let Err(error) = connected {
            assert_eq!(error.kind(), ConnectorErrorKind::Config);
        }
    }
    for zero in ["max_line_bytes", "max_file_bytes", "batch_rows"] {
        let connected = connect_with(root.path(), json!({ zero: 0 })).await;
        assert!(connected.is_err(), "{zero}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_that_grows_beyond_the_limit_while_it_is_read_fails_the_read() {
    use std::io::Write as _;
    let root = crate::fixtures::tempdir().unwrap();
    let path = root.path().join("events.jsonl");
    std::fs::write(&path, "{\"id\":1}\n{\"id\":2}\n").unwrap();
    let settings = json!({ "max_file_bytes": 64, "batch_rows": 1 });
    let source = connect_with(root.path(), settings).await.unwrap();
    let stream = StreamName::new("events").unwrap();
    let plan = source.plan(&stream, &StreamState::default()).await.unwrap();
    let (sink, mut feed) = partition_channel(NonZeroUsize::MIN);
    let request = ReadRequest::new(stream, plan.partitions[0].clone(), None);
    let reading = source.read(request, sink);
    let collecting = async {
        let mut events = Vec::new();
        while let Some(event) = feed.recv().await {
            // Once the read is under way, the file grows far beyond its limit.
            if events.is_empty() {
                let mut file = std::fs::File::options().append(true).open(&path).unwrap();
                for id in 0..100_000 {
                    writeln!(file, "{{\"id\":{id}}}").unwrap();
                }
            }
            events.push(event);
        }
        events
    };
    let (ended, events) = tokio::time::timeout(BOUND, async { tokio::join!(reading, collecting) })
        .await
        .expect("the read ends");
    let error = ended.expect_err("the file is beyond its limit");
    assert_eq!(error.code(), Some("limit_exceeded"));
    let limit = error.limit().expect("a limit");
    assert_eq!(
        (limit.name, limit.limit, limit.actual),
        ("file bytes", 64, 65)
    );
    // What was pushed is whole records from within the limit: no line cut where the limit fell.
    let pushes = pushed(&events);
    let bytes: usize = pushes.iter().map(String::len).sum();
    assert!(bytes <= 64, "{bytes} bytes pushed");
    assert!(pushes.iter().all(|push| push.ends_with("}\n")));
}

#[tokio::test]
async fn an_arrow_file_beyond_the_file_limit_is_refused_unread() {
    let root = crate::fixtures::tempdir().unwrap();
    arrow_file(&root.path().join("orders.arrow"), 2);
    let size = std::fs::metadata(root.path().join("orders.arrow"))
        .unwrap()
        .len();
    for (limit, reads) in [(size, true), (size - 1, false)] {
        let source = connect_with(root.path(), json!({ "max_file_bytes": limit }))
            .await
            .unwrap();
        let (events, ended) = read(source.as_ref(), "orders", None).await;
        assert_eq!(ended.is_ok(), reads, "{limit}");
        assert_eq!(events.is_empty(), !reads, "{limit}");
        if let Err(error) = ended {
            assert_eq!(error.limit().map(|limit| limit.name), Some("file bytes"));
        }
    }
}

#[tokio::test]
async fn a_cursor_beyond_what_its_file_now_holds_is_refused() {
    let root = crate::fixtures::tempdir().unwrap();
    let lines = root.path().join("ids.jsonl");
    std::fs::write(&lines, "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n").unwrap();
    arrow_file(&root.path().join("orders.arrow"), 3);
    let source = connect_with(root.path(), json!({ "batch_rows": 1 }))
        .await
        .unwrap();
    for (stream, shorter) in [("ids", "{\"id\":9}\n{\"id\":8}\n"), ("orders", "")] {
        let (events, ended) = read(source.as_ref(), stream, None).await;
        ended.expect("the file reads");
        let cursors = cursors(&events);
        assert_eq!(cursors.len(), 3, "{stream}");
        // A cursor at the file's end reads nothing more, and is no error.
        let (events, ended) = read(source.as_ref(), stream, cursors.last().cloned()).await;
        ended.expect("the file is read to its end");
        assert!(events.is_empty(), "{stream}");
        // The file is cut, or replaced by a shorter one: the cursor stands beyond it.
        if stream == "ids" {
            std::fs::write(&lines, shorter).unwrap();
        } else {
            arrow_file(&root.path().join("orders.arrow"), 2);
        }
        let (events, ended) = read(source.as_ref(), stream, cursors.last().cloned()).await;
        assert!(events.is_empty(), "{stream}");
        let error = ended.expect_err("the cursor is beyond the file");
        assert_eq!(error.kind(), ConnectorErrorKind::Data, "{stream}");
        assert_eq!(error.code(), Some("cursor_beyond_file"), "{stream}");
        // A cursor the shorter file still holds reads on from there.
        let (events, ended) = read(source.as_ref(), stream, Some(cursors[1].clone())).await;
        ended.expect("the cursor fits");
        assert!(events.is_empty(), "{stream}");
        let (events, ended) = read(source.as_ref(), stream, Some(cursors[0].clone())).await;
        ended.expect("the cursor fits");
        assert_eq!(events.len(), 2, "{stream}: a push and its checkpoint");
    }
}

#[tokio::test]
async fn two_entries_that_name_the_same_stream_are_refused_by_both_names() {
    for other in ["orders.ndjson", "orders.arrow", "orders"] {
        let root = crate::fixtures::tempdir().unwrap();
        std::fs::write(root.path().join("orders.jsonl"), "{\"id\":1}\n").unwrap();
        let other_path = root.path().join(other);
        if other == "orders" {
            std::fs::create_dir(&other_path).unwrap();
            std::fs::write(other_path.join("0.jsonl"), "{\"id\":2}\n").unwrap();
        } else if other.ends_with("arrow") {
            arrow_file(&other_path, 1);
        } else {
            std::fs::write(&other_path, "{\"id\":2}\n").unwrap();
        }
        let Err(error) = connect_with(root.path(), json!({})).await else {
            panic!("{other}: two entries name the stream orders");
        };
        assert_eq!(error.kind(), ConnectorErrorKind::Config, "{other}");
        assert_eq!(error.code(), Some("duplicate_stream"), "{other}");
        let message = error.to_string();
        assert!(message.contains("orders.jsonl"), "{other}: {message}");
        assert!(
            message.contains(&format!("{other:?}")),
            "{other}: {message}"
        );
    }
}

#[tokio::test]
async fn a_directory_whose_name_is_no_stream_s_is_skipped_unopened() {
    use std::os::unix::fs::PermissionsExt as _;
    // A directory that names no stream is none of the source's: it is not entered, so one the
    // source would refuse to enter, as a file system's own directory may be, fails nothing.
    let root = crate::fixtures::tempdir().unwrap();
    std::fs::write(root.path().join("orders.jsonl"), "{\"id\":1}\n").unwrap();
    for name in ["no\tstream", "no\nstream"] {
        assert!(StreamName::new(name).is_err(), "{name}");
        let other = root.path().join(name);
        std::fs::create_dir(&other).unwrap();
        std::fs::write(other.join("a.jsonl"), "{}\n").unwrap();
        std::fs::set_permissions(&other, std::fs::Permissions::from_mode(0o777)).unwrap();
    }
    let source = connect(root.path()).await;
    let (events, ended) = read(source.as_ref(), "orders", None).await;
    ended.expect("the stream reads");
    assert_eq!(pushed(&events), ["{\"id\":1}\n"]);
    assert_eq!(streams(source.as_ref()).await, ["orders"]);
}
