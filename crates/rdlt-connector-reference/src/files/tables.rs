//! The table catalog: each table's columns, kept under the root so every session and reader sees
//! them.
//!
//! Each change creates the table's next catalog version exclusively, so two sessions changing one
//! table at once never lose a change: a session that loses works its change out again.

#[cfg(test)]
mod tests;

use std::fs::File;
use std::io::{self as stdio, ErrorKind, Write as _};
use std::time::{Duration, Instant};

use rdlt_connector::{ConnectorError, ConnectorErrorKind, PipelineId, Result, TableSchema};

use super::{io, versions};
use crate::limits::{CATALOG_BYTES, OWNER_BYTES, TABLE_NAME_BYTES, TEMPORARY_AGE};
use crate::rooted::{self, Dir, Limit};

/// The code of an error for a table's lock another holder kept for the whole wait.
pub(super) const LOCK_TIMEOUT: &str = "lock_timeout";

/// The directories of the catalog, in the destination's private directory.
const TABLES: &str = "tables";
const LOCKS: &str = "locks";
const TRASH: &str = "trash";

/// The file in a table's catalog naming the pipeline that owns it.
const OWNER: &str = "owner";

const CATALOG_LIMIT: Limit = Limit {
    name: "catalog bytes",
    bytes: CATALOG_BYTES,
};

const OWNER_LIMIT: Limit = Limit {
    name: "owner bytes",
    bytes: OWNER_BYTES,
};

/// Checks that `name` is a table identifier of this destination, the only names that are ever
/// part of a path: ASCII letters, digits and underscores, at most 128 bytes.
pub(super) fn named(name: &str) -> Result<()> {
    let word = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';
    let fits = !name.is_empty() && name.len() <= usize::from(TABLE_NAME_BYTES);
    if fits && name.bytes().all(word) {
        Ok(())
    } else {
        Err(
            ConnectorError::data(format!("{name:?} is no table identifier"))
                .with_code(io::INVALID_NAME),
        )
    }
}

/// The catalog of the table `name` under the private directory `rdlt`, if the table has one.
fn catalog(rdlt: &Dir, name: &str) -> Result<Option<Dir>> {
    named(name)?;
    match rdlt.walk([TABLES, name]) {
        Ok(dir) => Ok(Some(dir)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io::failed("opening", &rdlt.at(TABLES).join(name))(error)),
    }
}

/// The catalog of the table `name` under `rdlt`, created where missing.
fn catalog_created(rdlt: &Dir, name: &str) -> Result<Dir> {
    named(name)?;
    rdlt.walk_created([TABLES, name])
        .map_err(io::failed("creating", &rdlt.at(TABLES).join(name)))
}

/// The columns of the table `name`, once it exists.
pub(super) fn read(rdlt: &Dir, name: &str) -> Result<Option<TableSchema>> {
    Ok(latest(rdlt, name)?.map(|(_, schema)| schema))
}

/// Applies `change` to the columns of the table `name`: it gets the current columns and returns
/// the new ones, or `None` to change nothing.
///
/// When another change lands first, `change` runs again on the columns that change left, a
/// bounded number of times.
pub(super) fn update(
    rdlt: &Dir,
    name: &str,
    mut change: impl FnMut(Option<&TableSchema>) -> Result<Option<TableSchema>>,
) -> Result<()> {
    io::retried(&format!("changing table {name}"), || {
        let current = latest(rdlt, name)?;
        let version = current.as_ref().map_or(0, |(version, _)| *version);
        let Some(next) = change(current.as_ref().map(|(_, schema)| schema))? else {
            return Ok(Some(()));
        };
        let Some(version) = version.checked_add(1) else {
            return Err(ConnectorError::data(format!(
                "table {name}: its catalog holds the last version there is"
            )));
        };
        Ok(create(rdlt, name, version, &next)?.then_some(()))
    })
}

/// The pipeline the owner file of the catalog `dir` names, once one does.
fn owner_of(dir: &Dir) -> Result<Option<String>> {
    let failed = |error| io::failed("reading", &dir.at(OWNER))(error);
    match dir.read(OWNER, OWNER_LIMIT) {
        Ok(bytes) => String::from_utf8(bytes).map(Some).map_err(|error| {
            failed(stdio::Error::new(
                ErrorKind::InvalidData,
                error.utf8_error(),
            ))
        }),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(failed(error)),
    }
}

/// Claims the table `name` for `pipeline` where no pipeline owns it yet; another pipeline's table
/// is refused as `table_owned`.
///
/// The owner is the first pipeline to create the table's owner file, which no later claim
/// replaces.
pub(super) fn claim(rdlt: &Dir, name: &str, pipeline: &PipelineId) -> Result<()> {
    let dir = catalog_created(rdlt, name)?;
    let owner = if let Some(owner) = owner_of(&dir)? {
        owner
    } else {
        let published = publish(&dir, OWNER, pipeline.as_str().as_bytes());
        published.map_err(io::failed("writing", &dir.at(OWNER)))?;
        // Whichever claim created the file, the file names the owner.
        owner_of(&dir)?.ok_or_else(|| {
            let gone = stdio::Error::from(ErrorKind::NotFound);
            io::failed("reading", &dir.at(OWNER))(gone)
        })?
    };
    if owner == pipeline.as_str() {
        Ok(())
    } else {
        Err(ConnectorError::table_owned(name, &owner))
    }
}

/// The pipeline that owns the table `name`, once one does.
pub(super) fn owner(rdlt: &Dir, name: &str) -> Result<Option<String>> {
    match catalog(rdlt, name)? {
        Some(dir) => owner_of(&dir),
        None => Ok(None),
    }
}

/// Runs `work` holding the lock of the table `name`'s catalog, which claims and releases take, so
/// a release never removes a catalog another pipeline claimed after it looked at the owner.
///
/// The lock file stays in place, outside the catalog, so every process locks the same file. A
/// lock another holder keeps for all of `wait` is a transient error coded `lock_timeout`.
pub(super) fn locked<T>(
    rdlt: &Dir,
    name: &str,
    wait: Duration,
    work: impl FnOnce() -> Result<T>,
) -> Result<T> {
    named(name)?;
    let locks = rdlt
        .dir_created(LOCKS)
        .map_err(io::failed("creating", &rdlt.at(LOCKS)))?;
    let path = locks.at(name);
    let file = lock_file(&locks, name).map_err(io::failed("opening", &path))?;
    acquire(&file, wait).map_err(|error| match error {
        Some(error) => io::failed("locking", &path)(error),
        None => ConnectorError::new(
            ConnectorErrorKind::Transient,
            format!("locking {}: another holder kept the lock", path.display()),
        )
        .with_code(LOCK_TIMEOUT),
    })?;
    let done = work();
    drop(file);
    done
}

/// Opens the lock file `name` in `locks`, creating it where missing: a regular file of this
/// user's alone, never a link and never another user's.
fn lock_file(locks: &Dir, name: &str) -> stdio::Result<File> {
    let mut lost = None;
    // A file created between the open that missed it and the create that lost to it is opened.
    for _ in 0..2 {
        match locks.file(name) {
            Ok(file) => {
                rooted::private(&file)?;
                return Ok(file);
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        match locks.create(name) {
            Ok(file) => {
                locks.sync()?;
                return Ok(file);
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => lost = Some(error),
            Err(error) => return Err(error),
        }
    }
    Err(lost.unwrap_or_else(|| ErrorKind::AlreadyExists.into()))
}

/// Takes the exclusive lock on `file`, trying for at most `wait`; `None` is a lock that stayed
/// held.
fn acquire(file: &File, wait: Duration) -> Result<(), Option<stdio::Error>> {
    let started = Instant::now();
    let mut pause = Duration::from_millis(1);
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(()),
            Err(std::fs::TryLockError::WouldBlock) => {}
            Err(std::fs::TryLockError::Error(error)) => return Err(Some(error)),
        }
        let left = wait.saturating_sub(started.elapsed());
        if left.is_zero() {
            return Err(None);
        }
        std::thread::sleep(pause.min(left));
        pause = (pause * 2).min(Duration::from_millis(50));
    }
}

/// Removes the catalog of the table `name`, which `pipeline` dropped, as [`release_held`] does,
/// holding the catalog's lock, once `dropped` says under the lock that the table is still
/// dropped.
///
/// A session another has overtaken finds the table no longer dropped, and removes nothing: the
/// newer session may have created the table again.
pub(super) fn release(
    rdlt: &Dir,
    name: &str,
    pipeline: &PipelineId,
    wait: Duration,
    dropped: impl FnOnce() -> Result<bool>,
) -> Result<()> {
    locked(rdlt, name, wait, || {
        if dropped()? {
            release_held(rdlt, name, pipeline)
        } else {
            Ok(())
        }
    })
}

/// Removes the catalog of the table `name`, which `pipeline` dropped: its columns and its owner,
/// so any pipeline may create a table of that name again; the caller holds the catalog's lock.
///
/// A catalog another pipeline owns by now is left alone. The catalog is first renamed out of
/// place, so a reader never meets it half removed.
pub(super) fn release_held(rdlt: &Dir, name: &str, pipeline: &PipelineId) -> Result<()> {
    if owner(rdlt, name)?.is_some_and(|owner| owner != pipeline.as_str()) {
        return Ok(());
    }
    let tables = match rdlt.dir(TABLES) {
        Ok(tables) => tables,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io::failed("opening", &rdlt.at(TABLES))(error)),
    };
    let trash = rdlt
        .dir_created(TRASH)
        .map_err(io::failed("creating", &rdlt.at(TRASH)))?;
    let moved = rooted::unique("").map_err(io::failed("naming in", trash.path()))?;
    match tables.rename(name, &trash, &moved) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io::failed("moving", &tables.at(name))(error)),
    }
    tables
        .sync()
        .map_err(io::failed("syncing", tables.path()))?;
    trash
        .remove_tree(&moved)
        .map_err(io::failed("removing", &trash.at(&moved)))
}

/// Removes the catalogs releases that died left renamed out of place under `rdlt`.
pub(super) fn empty_trash(rdlt: &Dir) -> Result<()> {
    let trash = match rdlt.dir(TRASH) {
        Ok(trash) => trash,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io::failed("opening", &rdlt.at(TRASH))(error)),
    };
    let failed = io::failed("emptying", trash.path());
    for (name, _) in trash.entries().map_err(&failed)? {
        trash.remove_tree(&name).map_err(&failed)?;
    }
    Ok(())
}

/// The newest catalog version of the table `name` and its columns.
fn latest(rdlt: &Dir, name: &str) -> Result<Option<(u64, TableSchema)>> {
    let Some(dir) = catalog(rdlt, name)? else {
        return Ok(None);
    };
    let read = versions::newest(&dir, CATALOG_LIMIT);
    let Some((version, bytes)) = read.map_err(io::failed("reading the catalog", dir.path()))?
    else {
        return Ok(None);
    };
    let schema = serde_json::from_slice(&bytes).map_err(|error| {
        let path = dir.at(versions::name(version));
        ConnectorError::internal(format!("table catalog {}: {error}", path.display()))
    })?;
    Ok(Some((version, schema)))
}

/// Creates catalog `version` of the table `name` with `schema`, unless it exists; returns whether
/// it did, removing the versions older than those kept.
fn create(rdlt: &Dir, name: &str, version: u64, schema: &TableSchema) -> Result<bool> {
    let dir = catalog_created(rdlt, name)?;
    let json = serde_json::to_vec_pretty(schema).expect("schemas serialize to JSON");
    let failed = io::failed("writing the catalog", dir.path());
    // A catalog version no reader accepts is never written.
    CATALOG_LIMIT
        .admit(u64::try_from(json.len()).unwrap_or(u64::MAX))
        .map_err(|refusal| failed(refusal.into()))?;
    dir.sweep(TEMPORARY_AGE).map_err(&failed)?;
    if !versions::create(&dir, version, &json).map_err(&failed)? {
        return Ok(false);
    }
    let listed = versions::listed(&dir).map_err(&failed)?;
    versions::prune(&dir, &listed, version);
    Ok(true)
}

/// Creates the file `file` in `dir` holding `bytes`, durably, unless it exists; returns whether
/// it did.
fn publish(dir: &Dir, file: &str, bytes: &[u8]) -> stdio::Result<bool> {
    let mut temporary = dir.temporary()?;
    temporary.file().write_all(bytes)?;
    temporary.publish(file)
}
