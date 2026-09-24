//! The table catalog: each table's columns, kept under the root so every session and reader sees
//! them.
//!
//! Each change creates the table's next catalog version exclusively, so two sessions changing one
//! table at once never lose a change: a session that loses works its change out again.

#[cfg(test)]
mod tests;

use std::fs;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use rdlt_connector::{ConnectorError, Result, TableSchema};

use super::io;

/// The directory holding the catalog versions of the table `name`.
fn catalog(root: &Path, name: &str) -> PathBuf {
    root.join("_rdlt").join("tables").join(name)
}

/// The columns of the table `name`, once it exists.
pub(super) fn read(root: &Path, name: &str) -> Result<Option<TableSchema>> {
    Ok(latest(root, name)?.map(|(_, schema)| schema))
}

/// Applies `change` to the columns of the table `name`: it gets the current columns and returns
/// the new ones, or `None` to change nothing.
///
/// When another change lands first, `change` runs again on the columns that change left.
pub(super) fn update(
    root: &Path,
    name: &str,
    mut change: impl FnMut(Option<&TableSchema>) -> Result<Option<TableSchema>>,
) -> Result<()> {
    loop {
        let current = latest(root, name)?;
        let version = current.as_ref().map_or(0, |(version, _)| *version);
        let Some(next) = change(current.as_ref().map(|(_, schema)| schema))? else {
            return Ok(());
        };
        if create(root, name, version + 1, &next)? {
            return Ok(());
        }
    }
}

/// The newest catalog version of the table `name` and its columns.
fn latest(root: &Path, name: &str) -> Result<Option<(u64, TableSchema)>> {
    let dir = catalog(root, name);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io::failed("listing", &dir)(error)),
    };
    let mut newest = None;
    for entry in entries {
        let entry = entry.map_err(io::failed("listing", &dir))?;
        let version = entry
            .file_name()
            .to_str()
            .and_then(|file| file.strip_suffix(".json"))
            .and_then(|stem| stem.parse::<u64>().ok());
        newest = newest.max(version);
    }
    let Some(version) = newest else {
        return Ok(None);
    };
    let path = dir.join(format!("{version:020}.json"));
    let bytes = fs::read(&path).map_err(io::failed("reading", &path))?;
    let schema = serde_json::from_slice(&bytes).map_err(|error| {
        ConnectorError::internal(format!("table catalog {}: {error}", path.display()))
    })?;
    Ok(Some((version, schema)))
}

/// Creates catalog `version` of the table `name` with `schema`, unless it exists; returns whether
/// it did.
fn create(root: &Path, name: &str, version: u64, schema: &TableSchema) -> Result<bool> {
    static WRITES: AtomicU64 = AtomicU64::new(0);
    let dir = catalog(root, name);
    fs::create_dir_all(&dir).map_err(io::failed("creating a directory", &dir))?;
    let temporary = dir.join(format!(
        ".{version}-{}-{}.tmp",
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
    let path = dir.join(format!("{version:020}.json"));
    let linked = fs::hard_link(&temporary, &path);
    drop(fs::remove_file(&temporary));
    match linked {
        Ok(()) => io::sync_dir(&dir).map(|()| true),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(io::failed("publishing", &path)(error)),
    }
}
