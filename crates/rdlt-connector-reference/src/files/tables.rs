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

use rdlt_connector::{ConnectorError, PipelineId, Result, TableSchema};

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

/// Claims the table `name` for `pipeline` where no pipeline owns it yet; another pipeline's table
/// is refused as `table_owned`.
///
/// The owner is the first pipeline to create the table's owner file, which no later claim
/// replaces.
pub(super) fn claim(root: &Path, name: &str, pipeline: &PipelineId) -> Result<()> {
    let dir = catalog(root, name);
    let path = dir.join(OWNER);
    // An owner file that cannot be read is created where it is missing; the second read reports
    // whatever else kept it from being read.
    let owner = if let Ok(owner) = fs::read_to_string(&path) {
        owner
    } else {
        io::create_dirs(&dir)?;
        publish(&dir, OWNER, pipeline.as_str().as_bytes())?;
        fs::read_to_string(&path).map_err(io::failed("reading", &path))?
    };
    if owner == pipeline.as_str() {
        Ok(())
    } else {
        Err(ConnectorError::table_owned(name, &owner))
    }
}

/// The file in a table's catalog naming the pipeline that owns it.
const OWNER: &str = "owner";

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
    let dir = catalog(root, name);
    io::create_dirs(&dir)?;
    let json = serde_json::to_vec_pretty(schema).expect("schemas serialize to JSON");
    publish(&dir, &format!("{version:020}.json"), &json)
}

/// Creates the file `file` in `dir` holding `bytes`, durably, unless it exists; returns whether
/// it did.
fn publish(dir: &Path, file: &str, bytes: &[u8]) -> Result<bool> {
    static WRITES: AtomicU64 = AtomicU64::new(0);
    let temporary = dir.join(format!(
        ".{file}-{}-{}.tmp",
        std::process::id(),
        WRITES.fetch_add(1, Ordering::Relaxed)
    ));
    let written = (|| {
        let mut created = fs::File::create_new(&temporary)?;
        created.write_all(bytes)?;
        created.sync_all()
    })();
    written.map_err(io::failed("writing", &temporary))?;
    let path = dir.join(file);
    let linked = fs::hard_link(&temporary, &path);
    drop(fs::remove_file(&temporary));
    match linked {
        Ok(()) => io::sync_dir(dir).map(|()| true),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(io::failed("publishing", &path)(error)),
    }
}
