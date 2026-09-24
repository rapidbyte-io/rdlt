//! The table catalog: each table's columns, kept under the root so every session and reader sees
//! them.

use std::fs;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use rdlt_connector::{ConnectorError, Result, TableSchema};

use super::io;

fn catalog(root: &Path) -> PathBuf {
    root.join("_rdlt").join("tables")
}

/// The columns of the table `name`, once it exists.
pub(super) fn read(root: &Path, name: &str) -> Result<Option<TableSchema>> {
    let path = catalog(root).join(format!("{name}.json"));
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            ConnectorError::internal(format!("table catalog {}: {error}", path.display()))
        }),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io::failed("reading", &path)(error)),
    }
}

/// Replaces the columns of the table `name` with `schema`, atomically.
pub(super) fn write(root: &Path, name: &str, schema: &TableSchema) -> Result<()> {
    static WRITES: AtomicU64 = AtomicU64::new(0);
    let dir = catalog(root);
    fs::create_dir_all(&dir).map_err(io::failed("creating a directory", &dir))?;
    let temporary = dir.join(format!(
        ".{name}-{}-{}.tmp",
        std::process::id(),
        WRITES.fetch_add(1, Ordering::Relaxed)
    ));
    let json = serde_json::to_vec_pretty(schema).expect("schemas serialize to JSON");
    let written = (|| {
        let mut file = fs::File::create_new(&temporary)?;
        file.write_all(&json)?;
        file.sync_all()
    })();
    written.map_err(io::failed("writing", &temporary))?;
    let path = dir.join(format!("{name}.json"));
    fs::rename(&temporary, &path).map_err(io::failed("replacing", &path))?;
    io::sync_dir(&dir)
}
