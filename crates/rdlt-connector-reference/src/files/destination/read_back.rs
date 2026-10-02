//! Reading back what the files destination published: every pipeline's latest manifest lists
//! the files of a table.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::prelude::*;

use super::super::manifest::{self, Manifest};
use super::super::{io, tables};
#[cfg(feature = "certify")]
use super::{FilesDestination, held_or_opened};
use super::{PIPELINES, existing};
#[cfg(feature = "certify")]
use crate::blocking::blocking;
use crate::rooted::{Dir, Kind};

#[cfg(feature = "certify")]
impl ReadBack for FilesDestination {
    async fn published(&self, table: &TableRef, rows: PublishedRows) -> Result<()> {
        let (root, name) = (self.root.to_path_buf(), table.name.clone());
        let held = Arc::clone(&self.rdlt);
        blocking(move || {
            let mut send = |batch| rows.blocking_send(batch);
            // A destination that never opened reads what is there, and creates nothing.
            if held.lock().is_none() {
                return published_each(&root, &name, &mut send);
            }
            published_in(&*held_or_opened(&root, &held)?, &name, &mut send)
        })
        .await
    }
}

/// Every published batch of `table` under `root`, over every pipeline's latest manifest.
pub fn published(root: impl Into<PathBuf>, table: &str) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    published_each(&root.into(), table, &mut |batch| {
        batches.push(batch);
        Ok(())
    })?;
    Ok(batches)
}

/// Gives `each` every batch [`published`] answers of `table` under `root`.
fn published_each(
    root: &Path,
    table: &str,
    each: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    tables::named(table)?;
    match existing(root)? {
        Some(rdlt) => published_in(&rdlt, table, each),
        None => Ok(()),
    }
}

/// Gives `each` every published batch of `table` under the private directory `rdlt`, a
/// pipeline's at a time: what one pipeline publishes of the table is read whole, since it is
/// read again where a commit removed a file meanwhile.
fn published_in(
    rdlt: &Dir,
    table: &str,
    each: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    tables::named(table)?;
    let schema = Arc::new(
        tables::read(rdlt, table)?
            .map_or_else(arrow_schema::Schema::empty, |schema| schema.to_arrow()),
    );
    let pipelines = match rdlt.dir(PIPELINES) {
        Ok(pipelines) => pipelines,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io::failed("opening", &rdlt.at(PIPELINES))(error)),
    };
    let listing = io::failed("listing", pipelines.path());
    for (name, kind) in pipelines.entries().map_err(&listing)? {
        if kind != Kind::Dir {
            continue;
        }
        let dir = pipelines.dir(&name).map_err(&listing)?;
        for batch in published_by(&dir, table, &schema, manifest::latest)? {
            each(batch)?;
        }
    }
    Ok(())
}

/// `batches` of `table`, each of the columns its rows hold, as a reader is given them: every
/// column of `schema`, one a batch never had as nulls its rows share.
fn whole(
    table: &str,
    schema: &arrow_schema::SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>> {
    let lacks = |batch: &RecordBatch| batch.num_columns() != schema.fields().len();
    if !batches.iter().any(lacks) {
        return Ok(batches);
    }
    let doing = format!("reading table {table}");
    crate::merge::read_back(schema, &batches).map_err(|error| crate::merge::failed(&doing, &error))
}

/// Every batch the pipeline whose directory `dir` is publishes of `table`, as the manifest
/// `latest` reads lists them.
///
/// A commit removes the files it supersedes once its manifest is durable, so a file listed by
/// the manifest just read may be gone: when a newer manifest exists by then, the table is read
/// again from it, a bounded number of times. A file missing under the manifest that is still
/// the latest is lost.
pub(in crate::files) fn published_by(
    dir: &Dir,
    table: &str,
    schema: &arrow_schema::SchemaRef,
    mut latest: impl FnMut(&Dir) -> Result<Option<Manifest>>,
) -> Result<Vec<RecordBatch>> {
    io::retried(&format!("reading table {table}"), || {
        let Some(manifest) = latest(dir)? else {
            return Ok(Some(Vec::new()));
        };
        let files = manifest.tables.get(table);
        let mut batches = Vec::new();
        for file in files.into_iter().flat_map(|files| &files.files) {
            match manifest::read_held(dir, &file.path, schema) {
                Ok(read) => batches.extend(whole(table, schema, read)?),
                Err(error) if error.code() == Some(io::FILE_MISSING) => {
                    let newer = latest(dir)?.is_some_and(|now| now.version != manifest.version);
                    return if newer { Ok(None) } else { Err(error) };
                }
                Err(error) => return Err(error),
            }
        }
        Ok(Some(batches))
    })
}
