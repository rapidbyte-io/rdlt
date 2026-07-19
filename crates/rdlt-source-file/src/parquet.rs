//! Parquet reading: row-group batches pushed as already-structured Arrow data
//! (passthrough path, clause S7); cursor unit = row groups.

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use rdlt_connector::{RecordsOut, SourceError};

use crate::cursor::{FileCursor, FileTask};

/// Like `resolve_files`, but sizes are ROW GROUP counts (the parquet seek unit).
pub(crate) fn resolve_with_row_groups(pattern: &str) -> Result<Vec<(String, u64)>, SourceError> {
    let files = crate::resolve_files(pattern)?;
    files
        .into_iter()
        .map(|(path, _bytes)| {
            let file = std::fs::File::open(&path)
                .map_err(|e| SourceError::fatal(format!("opening `{path}`: {e}")))?;
            let reader = SerializedFileReader::new(file)
                .map_err(|e| SourceError::fatal(format!("reading parquet `{path}`: {e}")))?;
            Ok((path, reader.metadata().num_row_groups() as u64))
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
    let file = std::fs::File::open(&task.path)
        .map_err(|e| SourceError::fatal(format!("opening `{}`: {e}", task.path)))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| SourceError::fatal(format!("reading parquet `{}`: {e}", task.path)))?;
    let total_groups = builder.metadata().num_row_groups() as u64;

    for group in task.start..total_groups {
        let file = std::fs::File::open(&task.path)
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
                return Ok(false); // cancellation (clause S4)
            }
        }
        cursor.record(&task.path, group + 1, total_groups);
        if out.checkpoint(cursor.encode()).await.is_err() {
            return Ok(false);
        }
    }
    Ok(true)
}
