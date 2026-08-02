//! Parquet reading: row-group batches pushed as already-structured
//! Arrow (the passthrough path — no shredding); the cursor unit is
//! ROW GROUPS, and the resume integrity is a CONTENT digest.
//!
//! The digest must come from content, not layout: row-group sizes and
//! page offsets are position-determined, so a group rewritten with
//! different values — or a regeneration that shifts uniformly-shaped
//! groups — leaves every footer quantity identical. The schema folds
//! in because a renamed column, or a width-preserving type change,
//! alters what the prefix MEANS without moving a byte of it.

use std::io::{Read as _, Seek as _, SeekFrom};
use std::ops::ControlFlow;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::FileReader as _;
use parquet::file::reader::SerializedFileReader;
use rdlt_connector_sdk::source::Feed;
use rdlt_connector_sdk::spi::SourceError;

use crate::source::cursor::{
    FileCursor, FileMeta, FileProgress, FileTask, ResumeCheck, TAIL_WINDOW,
};
use crate::source::list::local_listing;

/// Local listing where sizes are ROW-GROUP counts (the parquet unit).
pub(crate) fn local_listing_with_row_groups(pattern: &str) -> Result<Vec<FileMeta>, SourceError> {
    local_listing(pattern)?
        .into_iter()
        .map(|meta| {
            let file = std::fs::File::open(&meta.path)
                .map_err(|e| SourceError::fatal(format!("opening `{}`: {e}", meta.path)))?;
            let reader = SerializedFileReader::new(file)
                .map_err(|e| SourceError::fatal(format!("reading parquet `{}`: {e}", meta.path)))?;
            Ok(FileMeta {
                size_units: reader.metadata().num_row_groups() as u64,
                ..meta
            })
        })
        .collect()
}

/// One past the last byte of row group `last`'s last column chunk.
///
/// A chunk begins at its dictionary page when it has one, else its
/// first data page; `compressed_size` spans all its pages. CHECKED
/// arithmetic throughout — the footer is untrusted input, which is
/// also why `byte_range()` is not used: it asserts on a negative
/// offset instead of returning an error.
fn end_of_prefix(path: &str, metadata: &ParquetMetaData, last: u64) -> Result<u64, SourceError> {
    let bad = |what: &str| {
        SourceError::fatal(format!(
            "parquet `{path}`: row group {last} has {what}; refusing to verify a resume offset \
             against a footer that does not describe itself"
        ))
    };
    let group = metadata.row_group(last as usize);
    let mut end: u64 = 0;
    for chunk in group.columns() {
        let start = chunk
            .dictionary_page_offset()
            .unwrap_or_else(|| chunk.data_page_offset());
        let start = u64::try_from(start).map_err(|_| bad("a negative page offset"))?;
        let size =
            u64::try_from(chunk.compressed_size()).map_err(|_| bad("a negative chunk size"))?;
        let chunk_end = start
            .checked_add(size)
            .ok_or_else(|| bad("an overflowing extent"))?;
        end = end.max(chunk_end);
    }
    Ok(end)
}

/// The prefix integrity value: the schema (paths + physical + logical
/// types, NUL-separated), the group count, and the last window of the
/// prefix's own BYTES — one bounded read, the same discipline the
/// record formats' tail window uses.
fn prefix_digest(
    path: &str,
    metadata: &ParquetMetaData,
    groups: u64,
) -> Result<String, SourceError> {
    let mut hasher = blake3::Hasher::new();
    for field in metadata.file_metadata().schema_descr().columns() {
        hasher.update(field.path().string().as_bytes());
        hasher.update(&[0]);
        hasher.update(format!("{:?}", field.physical_type()).as_bytes());
        hasher.update(&[0]);
        hasher.update(format!("{:?}", field.logical_type_ref()).as_bytes());
        hasher.update(&[0]);
    }
    hasher.update(&groups.to_le_bytes());

    let end = end_of_prefix(path, metadata, groups - 1)?;
    let window = end.min(TAIL_WINDOW);
    let mut file = std::fs::File::open(path)
        .map_err(|e| SourceError::fatal(format!("opening `{path}`: {e}")))?;
    file.seek(SeekFrom::Start(end - window))
        .map_err(|e| SourceError::fatal(format!("seeking `{path}`: {e}")))?;
    let mut buffer = vec![0u8; window as usize];
    file.read_exact(&mut buffer).map_err(|e| {
        SourceError::fatal(format!(
            "parquet `{path}`: the {window} bytes ending the consumed prefix could not be read \
             ({e}); refusing to trust a resume offset the file no longer covers"
        ))
    })?;
    hasher.update(&buffer);
    Ok(hasher.finalize().to_hex().to_string())
}

/// Read from `task.start` (a row-group index): one fresh reader per
/// group (resume is group-scoped, so each must read independently;
/// the footer re-parse is microseconds), one checkpoint per group.
pub(crate) async fn read_task(
    task: &FileTask,
    cursor: &mut FileCursor,
    feed: &mut Feed,
) -> Result<bool, SourceError> {
    // A check of the wrong KIND is refused, not ignored — the jsonl
    // reader's rule, symmetric here: a byte-window hash recorded by a
    // record reader says nothing about row groups, and skipping it
    // would silently disarm verification (030 review). Before any IO —
    // the refusal needs nothing from the file.
    if let Some(ResumeCheck::TailBytes { .. }) = task.resume_check.as_ref() {
        return Err(SourceError::fatal(format!(
            "file `{}` recorded a byte-window integrity value but is being read as row \
             groups; refusing to resume without a check this reader can evaluate — clear \
             it from the pipeline state",
            task.path
        )));
    }

    let read_path = task.read_path.as_deref().unwrap_or(&task.path);
    let open = || {
        std::fs::File::open(read_path)
            .map_err(|e| SourceError::fatal(format!("opening `{}`: {e}", task.path)))
    };
    let builder = ParquetRecordBatchReaderBuilder::try_new(open()?)
        .map_err(|e| SourceError::fatal(format!("reading parquet `{}`: {e}", task.path)))?;
    let metadata = builder.metadata().clone();
    let total_groups = metadata.num_row_groups() as u64;

    // The cursor is an operator-editable document: a position past the
    // end refuses typed, never wraps. EQUAL to the count means nothing
    // new arrived — the loop reads nothing, and that is not an error.
    if task.start > total_groups {
        return Err(SourceError::fatal(format!(
            "file `{}` records progress through row group {} but now holds only \
             {total_groups} — refusing to resume past the end; clear it from the pipeline \
             state or restore the file",
            task.path, task.start
        )));
    }

    if let Some(ResumeCheck::RowGroupPrefix { hash }) = task.resume_check.as_ref()
        && prefix_digest(read_path, &metadata, task.start)? != *hash
    {
        return Err(SourceError::fatal(format!(
            "file `{}` was rewritten before the resume offset (the content of the {} row \
             groups preceding it changed since the last run); refusing to read from a stale \
             offset — clear it from the pipeline state or restore the file",
            task.path, task.start
        )));
    }

    for group in task.start..total_groups {
        let reader = ParquetRecordBatchReaderBuilder::try_new(open()?)
            .map_err(|e| SourceError::fatal(format!("reading parquet `{}`: {e}", task.path)))?
            .with_row_groups(vec![group as usize])
            .build()
            .map_err(|e| SourceError::fatal(format!("reading parquet `{}`: {e}", task.path)))?;
        for batch in reader {
            let batch = batch.map_err(|e| {
                SourceError::fatal(format!(
                    "corrupt parquet `{}` (row group {group}): {e}",
                    task.path
                ))
            })?;
            if feed.arrow(batch).await == ControlFlow::Break(()) {
                return Ok(false);
            }
        }
        cursor.record(
            &task.path,
            FileProgress {
                done_units: group + 1,
                size_units: total_groups,
                ended_at_record_boundary: true, // groups are whole records by construction
                mtime_ms: task.mtime_ms,
                etag: task.etag.clone(),
                tail_hash: None, // group units describe their own prefix instead
                row_groups_hash: Some(prefix_digest(read_path, &metadata, group + 1)?),
            },
        );
        if feed.checkpoint(cursor.encode()).await == ControlFlow::Break(()) {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn write_parquet(path: &std::path::Path, values: &[i64]) {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int64Array::from(values.to_vec()))],
        )
        .expect("batch");
        let file = std::fs::File::create(path).expect("create");
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    /// The digest is CONTENT-derived: identical layout with different
    /// values digests differently, and the same file digests stably.
    #[test]
    fn the_prefix_digest_reads_content_not_layout() {
        let dir = tempfile::tempdir().expect("dir");
        let a = dir.path().join("a.parquet");
        let b = dir.path().join("b.parquet");
        write_parquet(&a, &[1, 2, 3]);
        write_parquet(&b, &[4, 5, 6]);
        let digest_of = |p: &std::path::Path| {
            let file = std::fs::File::open(p).expect("open");
            let reader = SerializedFileReader::new(file).expect("reader");
            prefix_digest(p.to_str().expect("utf8"), reader.metadata(), 1).expect("digest")
        };
        assert_eq!(digest_of(&a), digest_of(&a), "stable");
        assert_ne!(
            digest_of(&a),
            digest_of(&b),
            "same shape, different values, different digest"
        );
    }

    /// The listing counts ROW GROUPS, not bytes.
    #[test]
    fn the_listing_counts_row_groups() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("g.parquet");
        write_parquet(&path, &[1, 2, 3, 4]);
        let pattern = path.to_str().expect("utf8");
        let listing = local_listing_with_row_groups(pattern).expect("lists");
        assert_eq!(listing[0].size_units, 1, "one batch, one group");
    }
}

#[cfg(test)]
mod wrong_kind_tests {
    use rdlt_connector_sdk::source::Feed;
    use rdlt_connector_sdk::spi::records_channel;

    use crate::source::cursor::{FileCursor, FileTask, ResumeCheck};

    /// A byte-window check on a row-group reader is refused, never
    /// ignored — the symmetric twin of the jsonl reader's rule (030
    /// review: it used to fall through and silently skip groups).
    #[tokio::test]
    async fn a_tail_bytes_check_is_refused_not_ignored() {
        let (out, _keep) = records_channel(1 << 20);
        let mut feed = Feed::new(out);
        let mut cursor = FileCursor::default();
        let task = FileTask {
            path: "data/a.parquet".into(),
            read_path: None,
            start: 1,
            size_units: 3,
            mtime_ms: None,
            etag: None,
            resume_check: Some(ResumeCheck::TailBytes {
                window: 10,
                hash: "beef".into(),
            }),
        };
        let err = super::read_task(&task, &mut cursor, &mut feed)
            .await
            .expect_err("refused")
            .to_string();
        assert!(
            err.contains("recorded a byte-window integrity value but is being read as row groups"),
            "{err}"
        );
    }
}
