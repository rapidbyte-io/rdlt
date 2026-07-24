//! Parquet reading: row-group batches pushed as already-structured Arrow data
//! (the passthrough path — no shredding); cursor unit = row groups.

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use rdlt_connector::{RecordsOut, SourceError};

use crate::source::cursor::{FileCursor, FileMeta, FileProgress, FileTask};

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
                size: reader.metadata().num_row_groups() as u64,
                ..meta
            })
        })
        .collect()
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
    let total_groups = builder.metadata().num_row_groups() as u64;

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
                done: group + 1,
                size: total_groups,
                eol: true, // row groups are whole records by construction
                mtime_ms: task.mtime_ms,
                etag: task.etag.clone(),
                tail_hash: None, // row-group units: etag/mtime identity governs
            },
        );
        if out.checkpoint(cursor.encode()).await.is_err() {
            return Ok(false);
        }
    }
    Ok(true)
}
