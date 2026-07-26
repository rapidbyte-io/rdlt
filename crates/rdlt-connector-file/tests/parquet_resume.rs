//! Resume integrity for parquet inputs.
//!
//! A resumed read must never trust a recorded row-group offset that the file's
//! current content does not justify. Size alone cannot see it: a file that GREW
//! while its consumed prefix was rewritten passes every size and identity check,
//! and the read then continues at an offset that now names different data.
//!
//! The mirror obligation is just as load-bearing: a file that grew by genuine
//! appending must still resume. An integrity check that refuses honest appends
//! turns an incremental pipeline into a full re-read.

use std::path::Path;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use rdlt_connector::{Cursor, PushPayload, ReadRequest, Source, SourceError, records_channel};
use rdlt_connector_file::FileSource;

/// Write a parquet file whose row groups are exactly `groups`. `ArrowWriter::flush`
/// closes the current group and early-returns when nothing is in progress, so a
/// flush between batches yields one group per slice and no trailing empty group.
fn write_groups(path: &Path, groups: &[&[i64]], compression: parquet::basic::Compression) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(compression)
        .build();
    let file = std::fs::File::create(path).expect("create");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("writer");
    for group in groups {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(group.to_vec()))],
        )
        .expect("batch");
        writer.write(&batch).expect("write");
        writer.flush().expect("flush");
    }
    writer.close().expect("close");
}

fn plain(path: &Path, groups: &[&[i64]]) {
    write_groups(path, groups, parquet::basic::Compression::UNCOMPRESSED);
}

/// Same shape as `plain`, under a different column name.
fn write_named(path: &Path, column: &str, groups: &[&[i64]]) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        column,
        DataType::Int64,
        false,
    )]));
    let file = std::fs::File::create(path).expect("create");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("writer");
    for group in groups {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(group.to_vec()))],
        )
        .expect("batch");
        writer.write(&batch).expect("write");
        writer.flush().expect("flush");
    }
    writer.close().expect("close");
}

fn source_for(path: &Path) -> FileSource {
    FileSource::from_yaml(&format!(
        "streams:\n  - name: events\n    format: parquet\n    path: \"{}\"\n",
        path.display()
    ))
    .expect("valid config")
}

/// Read the stream to completion; returns (rows delivered, final checkpoint).
async fn read_all(
    source: &FileSource,
    since: Option<Cursor>,
) -> Result<(usize, Option<Cursor>), SourceError> {
    let (out, mut input) = records_channel(16 << 20);
    let spec = source.streams().await?[0].clone();
    let read = {
        let request = ReadRequest::new(spec, since, out);
        async move { source.read(request).await }
    };
    tokio::pin!(read);

    let mut rows = 0usize;
    let mut last_cursor = None;
    let mut read_result: Option<Result<(), SourceError>> = None;
    loop {
        tokio::select! {
            push = input.recv() => match push {
                Some(push) => match push.payload {
                    PushPayload::Arrow(batch) => rows += batch.num_rows(),
                    PushPayload::Checkpoint(cursor) => last_cursor = Some(cursor),
                    other => panic!("unexpected push {other:?}"),
                },
                None => break,
            },
            result = &mut read, if read_result.is_none() => read_result = Some(result),
        }
    }
    match read_result {
        Some(Err(e)) => Err(e),
        _ => Ok((rows, last_cursor)),
    }
}

#[tokio::test]
async fn a_genuine_append_resumes_and_delivers_only_the_new_groups() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.parquet");

    plain(&path, &[&[1, 2, 3]]);
    let source = source_for(&path);
    let (first, cursor) = read_all(&source, None).await.expect("first run");
    assert_eq!(first, 3);

    plain(&path, &[&[1, 2, 3], &[4, 5, 6]]);
    let (second, _) = read_all(&source, cursor)
        .await
        .expect("an append must resume");
    assert_eq!(second, 3, "only the appended group is delivered");
}

#[tokio::test]
async fn a_rewritten_prefix_is_refused_even_though_the_file_grew() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.parquet");

    plain(&path, &[&[1, 2, 3]]);
    let source = source_for(&path);
    let (_, cursor) = read_all(&source, None).await.expect("first run");

    // Grew by one group AND the consumed group's VALUES changed — with the SAME
    // cardinality, so every physical quantity in the footer (page offsets, chunk
    // sizes, dictionary size, row counts) is bit-identical to what run 1 saw.
    // A layout-derived check accepts this; only a content-derived one refuses it.
    // `[9,9,9]` would collapse the dictionary from three entries to one and pass
    // for a reason that has nothing to do with the property under test.
    plain(&path, &[&[9, 8, 7], &[4, 5, 6]]);
    let error = read_all(&source, cursor)
        .await
        .expect_err("a rewritten prefix must be refused");
    assert!(
        matches!(error, SourceError::Fatal { .. }),
        "a stale resume offset is fatal, not retried: {error:?}"
    );
}

/// Uniformly-shaped row groups make every page offset a function of POSITION,
/// so a regeneration that merely shifts the groups leaves the description of the
/// consumed prefix identical. Accepting it re-delivers rows already delivered and
/// never delivers the prepended ones — silent duplication AND silent loss.
#[tokio::test]
async fn a_prefix_shifted_by_a_prepended_group_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.parquet");

    plain(&path, &[&[1, 2, 3], &[4, 5, 6], &[7, 8, 9]]);
    let source = source_for(&path);
    let (first, cursor) = read_all(&source, None).await.expect("first run");
    assert_eq!(first, 9);

    // A backfill, a re-sort, or a late-arriving batch prepended.
    plain(&path, &[&[10, 11, 12], &[1, 2, 3], &[4, 5, 6], &[7, 8, 9]]);
    let error = read_all(&source, cursor)
        .await
        .expect_err("a shifted prefix is not an append");
    assert!(matches!(error, SourceError::Fatal { .. }), "{error:?}");
}

/// A prefix whose SCHEMA changed is a changed prefix. A renamed column, or a
/// width-preserving type change, moves no byte of the data and must still be
/// refused — otherwise the prefix's rows are stranded under a stale name while
/// the appended rows arrive under the new one.
#[tokio::test]
async fn a_prefix_whose_schema_changed_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.parquet");

    plain(&path, &[&[1, 2, 3]]);
    let source = source_for(&path);
    let (_, cursor) = read_all(&source, None).await.expect("first run");

    write_named(&path, "user_id", &[&[1, 2, 3], &[4, 5, 6]]);
    let error = read_all(&source, cursor)
        .await
        .expect_err("a renamed column is a changed prefix");
    assert!(matches!(error, SourceError::Fatal { .. }), "{error:?}");
}

/// The regression pin for the design this feature's Phase 0 rejected.
///
/// A cursor written before integrity values existed carries none, so run 2
/// resumes unverified — and must record a value describing the WHOLE consumed
/// prefix, not just the part it read this time. Recording the narrower value
/// makes run 3 compare a description of `start..done` against one of `0..done`
/// and refuse a legitimate append — permanently, for every pre-existing cursor.
#[tokio::test]
async fn a_cursor_without_an_integrity_value_recovers_rather_than_poisoning_itself() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.parquet");

    plain(&path, &[&[1, 2, 3]]);
    let source = source_for(&path);
    let (_, cursor) = read_all(&source, None).await.expect("first run");

    // What a cursor written by an older build looks like.
    let mut doc = cursor.expect("cursor").as_value().clone();
    for entry in doc["files"].as_object_mut().expect("files").values_mut() {
        entry
            .as_object_mut()
            .expect("entry")
            .remove("row_groups_hash");
    }
    let legacy = Cursor::new(doc);

    plain(&path, &[&[1, 2, 3], &[4, 5, 6]]);
    let (second, cursor2) = read_all(&source, Some(legacy))
        .await
        .expect("an unverified resume proceeds");
    assert_eq!(second, 3);

    plain(&path, &[&[1, 2, 3], &[4, 5, 6], &[7, 8, 9]]);
    let (third, _) = read_all(&source, cursor2)
        .await
        .expect("the run after an unverified resume must still accept an append");
    assert_eq!(third, 3, "the recovered cursor verifies and resumes");
}

/// The recorded narrowing, pinned deliberately rather than discovered: the
/// description covers physical layout, so a file rewritten wholesale by a
/// different writer — even one preserving the logical prefix — is refused.
/// A whole-file re-encode is not an append.
#[tokio::test]
async fn a_re_encoded_prefix_is_refused_even_when_the_rows_are_unchanged() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.parquet");

    plain(&path, &[&[1, 2, 3]]);
    let source = source_for(&path);
    let (_, cursor) = read_all(&source, None).await.expect("first run");

    // Same logical rows, different physical encoding, plus an appended group.
    write_groups(
        &path,
        &[&[1, 2, 3], &[4, 5, 6]],
        parquet::basic::Compression::SNAPPY,
    );
    let error = read_all(&source, cursor)
        .await
        .expect_err("a re-encoded prefix is not an append");
    assert!(
        matches!(error, SourceError::Fatal { .. }),
        "a re-encode is refused fatally, not retried: {error:?}"
    );
}
