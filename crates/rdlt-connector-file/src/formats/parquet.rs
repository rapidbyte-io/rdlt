//! Parquet reading: row-group batches pushed as already-structured Arrow data
//! (the passthrough path — no shredding); cursor unit = row groups.

use std::io::{Read, Seek, SeekFrom};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use rdlt_connector::{RecordsOut, SourceError};

use crate::source::cursor::{
    FileCursor, FileMeta, FileProgress, FileTask, ResumeCheck, TAIL_WINDOW,
};

/// Like `resolve_files`, but sizes are ROW GROUP counts (the parquet seek unit).
pub(crate) fn resolve_with_row_groups(pattern: &str) -> Result<Vec<FileMeta>, SourceError> {
    let files = crate::source::resolve_files(pattern)?;
    files
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

/// Where the consumed prefix physically ENDS: one past the last byte of the last
/// column chunk of row group `last`.
///
/// A chunk begins at its dictionary page when it has one, otherwise at its first
/// data page, and `compressed_size` spans all of its pages. Computed with
/// checked arithmetic and refused when the footer is not self-consistent — a
/// footer is untrusted input, which is also why `byte_range()` is not used: it
/// asserts on a negative offset rather than returning an error.
fn end_of_prefix(path: &str, metadata: &ParquetMetaData, last: u64) -> Result<u64, SourceError> {
    let bad = |what: &str| {
        SourceError::fatal(format!(
            "parquet `{path}`: row group {last} has {what}; refusing to verify a resume \
             offset against a footer that does not describe itself"
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

/// The integrity value for the consumed prefix: the file's SCHEMA plus a window
/// of the prefix's own BYTES, ending where the prefix ends.
///
/// It must be derived from CONTENT, not from layout. Row-group sizes and page
/// offsets are position-determined, not value-determined: a group rewritten with
/// entirely different values, and a whole-file regeneration that merely shifts
/// uniformly-shaped groups, both leave every footer quantity identical. Hashing
/// bytes is the same discipline the record formats use for their tail window,
/// and it costs one bounded read — free on the object-store path, where the
/// object has already been fetched to a local file.
///
/// The schema is folded in because a renamed column, or a width-preserving type
/// change, alters what the prefix MEANS without moving a byte of it.
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
            "parquet `{path}`: the {window} bytes ending the consumed prefix could not be \
             read ({e}); refusing to trust a resume offset the file no longer covers"
        ))
    })?;
    hasher.update(&buffer);
    Ok(hasher.finalize().to_hex().to_string())
}

/// Read one file from `task.start` (a row-group index), pushing one Arrow batch
/// stream per remaining row group; checkpoint per row group.
pub(crate) async fn read_task(
    task: &FileTask,
    cursor: &mut FileCursor,
    out: &mut RecordsOut,
) -> Result<bool, SourceError> {
    let read_path = task.read_path.as_deref().unwrap_or(&task.path);
    let file = std::fs::File::open(read_path)
        .map_err(|e| SourceError::fatal(format!("opening `{}`: {e}", task.path)))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| SourceError::fatal(format!("reading parquet `{}`: {e}", task.path)))?;
    let metadata = builder.metadata().clone();
    let total_groups = metadata.num_row_groups() as u64;

    // A recorded position past the end is a typed refusal, never an arithmetic
    // operation: the cursor is an operator-editable document. `start` EQUAL to
    // the count is not an error — it means nothing new has arrived — and the
    // loop below correctly reads nothing.
    if task.start > total_groups {
        return Err(SourceError::fatal(format!(
            "file `{}` records progress through row group {} but now holds only {total_groups} — \
             refusing to resume past the end; clear it from the pipeline state or \
             restore the file",
            task.path, task.start
        )));
    }

    if let Some(ResumeCheck::RowGroupPrefix { hash, .. }) = task.resume_check.as_ref()
        && prefix_digest(read_path, &metadata, task.start)? != *hash
    {
        return Err(SourceError::fatal(format!(
            "file `{}` was rewritten before the resume offset (the content of the {} row \
             groups preceding it changed since the last run); refusing to read from a \
             stale offset — clear it from the pipeline state or restore the file",
            task.path, task.start
        )));
    }

    // Each row group gets a fresh reader (its own file handle + footer
    // parse): resume is row-group-scoped, so every group must be readable
    // independently of its predecessors, and the footer re-parse is
    // microseconds against the group's read cost.
    for group in task.start..total_groups {
        let file = std::fs::File::open(read_path)
            .map_err(|e| SourceError::fatal(format!("opening `{}`: {e}", task.path)))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
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
            if out.arrow(batch).await.is_err() {
                return Ok(false); // closed channel = cancellation
            }
        }
        cursor.record(
            &task.path,
            FileProgress {
                done_units: group + 1,
                size_units: total_groups,
                ended_at_record_boundary: true, // row groups are whole records by construction
                mtime_ms: task.mtime_ms,
                etag: task.etag.clone(),
                tail_hash: None, // row-group units describe their own prefix instead
                row_groups_hash: Some(prefix_digest(read_path, &metadata, group + 1)?),
            },
        );
        if out.checkpoint(cursor.encode()).await.is_err() {
            return Ok(false);
        }
    }
    Ok(true)
}
