//! Test/inspection helpers: total PUBLISHED rows of a table (parquet footers /
//! jsonl line counts), partitions included, staged parts excluded. The async
//! path counts over the shared `Location::keys_of_table` ownership listing; the
//! synchronous path (local only) walks the table directory directly. Both count
//! bytes through the one `count_data_units` rule.

use std::path::Path;

use bytes::Bytes;
use parquet::file::reader::{FileReader, SerializedFileReader};
use rdlt_connector::DestinationError;

use super::{FileDest, fatal};

/// Rows in one data file, decided by extension: parquet footer row count, jsonl
/// newline count, anything else zero (not a data file this destination writes).
fn count_data_units(name: &str, bytes: &[u8]) -> Result<u64, DestinationError> {
    if name.ends_with(".parquet") {
        let reader = SerializedFileReader::new(Bytes::copy_from_slice(bytes)).map_err(fatal)?;
        Ok(reader.metadata().file_metadata().num_rows() as u64)
    } else if name.ends_with(".jsonl") {
        Ok(bytes.iter().filter(|b| **b == b'\n').count() as u64)
    } else {
        Ok(0)
    }
}

impl FileDest {
    /// Test/inspection helper: total published rows of a table, partitions
    /// included, staged excluded. Synchronous — local stores only.
    pub fn count_rows(&self, table: &str) -> Result<u64, DestinationError> {
        let Some(root) = self.location.local_root() else {
            return Err(fatal(
                "count_rows is synchronous; use count_rows_async for object stores",
            ));
        };
        count_rows_local(&root.join(table))
    }

    /// Async row count (object stores; works for local too), over the shared
    /// per-table ownership listing.
    pub async fn count_rows_async(&self, table: &str) -> Result<u64, DestinationError> {
        let mut total = 0u64;
        for tail in self.location.keys_of_table(table).await? {
            if tail.ends_with(".parquet") || tail.ends_with(".jsonl") {
                let bytes = self.location.read_table_file(table, &tail).await?;
                total += count_data_units(&tail, &bytes)?;
            }
        }
        Ok(total)
    }
}

/// Recursive local row count (partition subdirectories included).
fn count_rows_local(dir: &Path) -> Result<u64, DestinationError> {
    let mut total = 0u64;
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(fatal(e)),
    };
    for entry in entries {
        let path = entry.map_err(fatal)?.path();
        if path.is_dir() {
            total += count_rows_local(&path)?;
        } else if let Some(name) = path.file_name().and_then(|n| n.to_str())
            && (name.ends_with(".parquet") || name.ends_with(".jsonl"))
        {
            let bytes = std::fs::read(&path).map_err(fatal)?;
            total += count_data_units(name, &bytes)?;
        }
    }
    Ok(total)
}
